#!/usr/bin/env python3
import os
import json
import time
import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError

# —————————————————————————————
# Константы (ID таблиц и имена листов)
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

@st.cache_data(show_spinner=False)
def get_client():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT") or st.secrets["GCP_SERVICE_ACCOUNT"]
    creds_dict = json.loads(sa_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for attempt in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            code = getattr(e.response, "status_code", None)
            if code and 500 <= code < 600 and attempt < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

def load_sheet(ss_id, sheet_name):
    client = get_client()
    sh = api_retry(client.open_by_key, ss_id)
    ws = api_retry(sh.worksheet, sheet_name)
    data = api_retry(ws.get_all_values)
    # выравниваем строки до одинаковой длины
    maxc = max(len(r) for r in data)
    header = data[0] + [""]*(maxc-len(data[0]))
    rows   = [r + [""]*(maxc-len(r)) for r in data[1:]]
    return pd.DataFrame(rows, columns=header)

@st.cache_data(show_spinner=False)
def build_df():
    # 1) Уроки
    def load_lessons(ss_id, sheet, region):
        df = load_sheet(ss_id, sheet)
        # R=17, Q=16, B=1, J=9, N=13, G=6, H=7, Y=24
        df = df.iloc[:, [17,16,1,9,13,6,7,24]]
        df.columns = [
            "Tutor name","Tutor ID","Date of the lesson","Group",
            "Course ID","Module","Lesson","Lesson Link"
        ]
        df["Region"] = region
        df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
        # кликабельные ссылки
        df["Lesson Link"] = df["Lesson Link"].apply(lambda u: f'<a href="{u}" target="_blank">Link</a>')
        return df

    df_lat = load_lessons(LESSONS_SS, LATAM_SHEET,  "LATAM")
    df_brz = load_lessons(LESSONS_SS, BRAZIL_SHEET, "Brazil")
    lessons = pd.concat([df_lat, df_brz], ignore_index=True)

    # 2) Рейтинг + F:K
    def load_rating(ss_id):
        df = load_sheet(ss_id, RATING_SHEET)
        cols = ["Tutor ID","Rating",
                "Num of QA scores","Num of QA scores (last 90 days)",
                "Average QA score","Average QA score (last 2 scores within last 90 days)",
                "Average QA marker","Average QA marker (last 2 markers within last 90 days)"]
        return df[cols]

    r_lat = load_rating(RATING_LATAM_SS)
    r_brz = load_rating(RATING_BRAZIL_SS)

    # присоединяем
    merged = lessons.merge(
        r_lat, on="Tutor ID", how="left", suffixes=(None,None)
    ).where(lessons["Region"]=="LATAM", lessons)
    merged = merged.merge(
        r_brz, on="Tutor ID", how="left"
    ).where(merged["Region"]=="Brazil", merged)

    # 3) QA-оценки
    def load_qa(ss_id):
        df = load_sheet(ss_id, QA_SHEET)
        df["Date"] = pd.to_datetime(df["B"], errors="coerce")
        return df[["A","E","Date","C","D"]].rename(columns={
            "A":"Tutor ID","E":"Group","C":"QA score","D":"QA marker"
        })

    q_lat = load_qa(QA_LATAM_SS)
    q_brz = load_qa(QA_BRAZIL_SS)
    merged = merged.merge(q_lat, on=["Tutor ID","Group","Date of the lesson"], how="left") \
                   .where(merged["Region"]=="LATAM", merged)
    merged = merged.merge(q_brz, on=["Tutor ID","Group","Date of the lesson"], how="left") \
                   .where(merged["Region"]=="Brazil", merged)

    # 4) Replacement
    rp = load_sheet(REPL_SS, REPL_SHEET)
    rp["Date"]  = pd.to_datetime(rp["D"], errors="coerce")
    rp["Group"] = rp["F"]
    rp = rp[["Date","Group"]].assign(**{"Replacement or not":"Replacement/Postponement"})
    merged = merged.merge(
        rp, left_on=["Date of the lesson","Group"],
        right_on=["Date","Group"], how="left"
    )
    merged["Replacement or not"] = merged["Replacement or not"].fillna("")

    return merged

# === UI ===
st.set_page_config(page_title="QA & Rating", layout="wide")
df = build_df()

# фильтры
st.sidebar.header("Filters")
selections = {}
for col in df.columns:
    if df[col].dtype == "object":
        selections[col] = st.sidebar.multiselect(col, df[col].dropna().unique())

mask = pd.Series(True, index=df.index)
for col, sel in selections.items():
    if sel:
        mask &= df[col].isin(sel)

dff = df[mask]

st.title("QA & Rating Dashboard")
st.write(f"Showing {len(dff)}/{len(df)} rows")
st.dataframe(dff, unsafe_allow_html=True, use_container_width=True)

# кнопка скачивания
csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_rating.csv", "text/csv")
