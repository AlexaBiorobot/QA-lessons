#!/usr/bin/env python3
import os
import json
import time

import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# —————————————————————————————
# Константы
LESSONS_SS        = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
LATAM_SHEET       = "lessons LATAM"
BRAZIL_SHEET      = "lessons Brazil"

RATING_LATAM_SS   = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
RATING_BRAZIL_SS  = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"
RATING_SHEET      = "Rating"

QA_LATAM_SS       = RATING_LATAM_SS
QA_BRAZIL_SS      = RATING_BRAZIL_SS
QA_SHEET          = "QA - Lesson evaluation"

REPL_SS           = "1LF2NrAm8J3c43wOoumtsyfQsX1z0_lUQVdByGSPe27U"
REPL_SHEET        = "Replacement"
# —————————————————————————————

@st.cache_data
def get_client():
    """Авторизация для gspread: сначала из env, иначе из st.secrets."""
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT")
    if not sa_json:
        sa_json = st.secrets["GCP_SERVICE_ACCOUNT"]
    creds_info = json.loads(sa_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    return gspread.authorize(creds)

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    """Retry при любых исключениях (включая 5xx) с экспоненциальным бэкофом."""
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_attempts:
                raise
            time.sleep(backoff)
            backoff *= 2

def load_sheet_values(ss_id, sheet_name):
    client = get_client()
    sh     = api_retry(client.open_by_key, ss_id)
    ws     = api_retry(sh.worksheet, sheet_name)
    rows   = api_retry(ws.get_all_values)
    # выравниваем длину строк
    maxc   = max(len(r) for r in rows)
    header = rows[0] + [""]*(maxc-len(rows[0]))
    data   = [r+[""]*(maxc-len(r)) for r in rows[1:]]
    return pd.DataFrame(data, columns=header)

@st.cache_data
def build_df():
    # 1) Уроки
    def load_lessons(ss, name, region):
        df = load_sheet_values(ss, name)
        # R(17),Q(16),B(1),J(9),N(13),G(6),H(7),Y(24)
        df = df.iloc[:, [17,16,1,9,13,6,7,24]]
        df.columns = [
            "Tutor name","Tutor ID","Date of the lesson","Group",
            "Course ID","Module","Lesson","Lesson Link"
        ]
        df["Region"] = region
        df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
        return df

    df_lat = load_lessons(LESSONS_SS, LATAM_SHEET,  "LATAM")
    df_brz = load_lessons(LESSONS_SS, BRAZIL_SHEET, "Brazil")
    df = pd.concat([df_lat, df_brz], ignore_index=True)

    # 2) Рейтинг
    def load_rating(ss):
        df_r = load_sheet_values(ss, RATING_SHEET)
        cols = [
            "Tutor ID","Rating",
            "Num of QA scores","Num of QA scores (last 90 days)",
            "Average QA score","Average QA score (last 2 scores within last 90 days)",
            "Average QA marker","Average QA marker (last 2 markers within last 90 days)"
        ]
        return df_r[cols]

    r_lat = load_rating(RATING_LATAM_SS)
    r_brz = load_rating(RATING_BRAZIL_SS)

    # Разделяем по региону, мёрджим нужный рейтинг
    df = df.merge(r_lat, on="Tutor ID", how="left", indicator=False).where(df["Region"]=="LATAM", df)
    df = df.merge(r_brz, on="Tutor ID", how="left", indicator=False).where(df["Region"]=="Brazil", df)

    # 3) QA-оценки
    def load_qa(ss):
        df_q = load_sheet_values(ss, QA_SHEET)
        df_q["Date"] = pd.to_datetime(df_q["B"], errors="coerce")
        return df_q[["A","E","Date","C","D"]].rename(columns={
            "A":"Tutor ID","E":"Group","C":"QA score","D":"QA marker"
        })

    q_lat = load_qa(QA_LATAM_SS)
    q_brz = load_qa(QA_BRAZIL_SS)
    df = df.merge(q_lat, on=["Tutor ID","Group","Date of the lesson"], how="left").where(df["Region"]=="LATAM", df)
    df = df.merge(q_brz, on=["Tutor ID","Group","Date of the lesson"], how="left").where(df["Region"]=="Brazil", df)

    # 4) Replacement
    df_rp = load_sheet_values(REPL_SS, REPL_SHEET)
    df_rp["Date"]  = pd.to_datetime(df_rp["D"], errors="coerce")
    df_rp["Group"] = df_rp["F"]
    df_rp = df_rp[["Date","Group"]].assign(**{"Replacement or not":"Replacement/Postponement"})
    df = df.merge(df_rp, left_on=["Date of the lesson","Group"],
                  right_on=["Date","Group"], how="left")
    df["Replacement or not"] = df["Replacement or not"].fillna("")

    return df

# === UI ===
st.set_page_config(layout="wide")
df = build_df()

# Sidebar-фильтры
st.sidebar.header("Filters")
filters = {}
for col in df.columns:
    if df[col].dtype == object or pd.api.types.is_categorical_dtype(df[col]):
        opts = df[col].dropna().unique().tolist()
        sel  = st.sidebar.multiselect(col, opts, default=opts)
        filters[col] = sel

mask = pd.Series(True, index=df.index)
for c, sel in filters.items():
    mask &= df[c].isin(sel)

dff = df[mask]

st.title("QA & Rating Dashboard")
st.dataframe(dff, use_container_width=True)

# Кнопка скачивания
csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_dashboard.csv", "text/csv")
