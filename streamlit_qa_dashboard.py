#!/usr/bin/env python3
import os
import json
import time
import streamlit as st
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError

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
    import streamlit as _st
    scope   = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT") or _st.secrets["GCP_SERVICE_ACCOUNT"]
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(sa_json), scope)
    return gspread.authorize(creds)

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for i in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except Exception:
            if i == max_attempts:
                raise
            time.sleep(backoff)
            backoff *= 2

def load_sheet_values(ss_id, sheet_name):
    client = get_client()
    sh     = api_retry(client.open_by_key, ss_id)
    ws     = api_retry(sh.worksheet, sheet_name)
    rows   = api_retry(ws.get_all_values)
    maxc   = max(len(r) for r in rows)
    header = rows[0] + [""]*(maxc-len(rows[0]))
    data   = [r+[""]*(maxc-len(r)) for r in rows[1:]]
    return pd.DataFrame(data, columns=header)

@st.cache_data
def build_df():
    # 1) уроки
    def load_lessons(ss_id, sheet_name, region):
        raw = load_sheet_values(ss_id, sheet_name)
        # выбираем R(17),Q(16),B(1),J(9),N(13),G(6),H(7),Y(24)
        df = raw.iloc[:, [17,16,1,9,13,6,7,24]].copy()
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

    # 2) рейтинг
    def load_rating(ss_id):
        r = load_sheet_values(ss_id, RATING_SHEET)
        cols = [
            "Tutor ID","Rating",
            "Num of QA scores",
            "Num of QA scores (last 90 days)",
            "Average QA score",
            "Average QA score (last 2 scores within last 90 days)",
            "Average QA marker",
            "Average QA marker (last 2 markers within last 90 days)"
        ]
        return r[cols]

    r_lat = load_rating(RATING_LATAM_SS)
    r_brz = load_rating(RATING_BRAZIL_SS)

    df = (
        df
        .merge(r_lat, on="Tutor ID", how="left", suffixes=("", "_drop"))
        .where(df["Region"]=="LATAM", df)
        .merge(r_brz, on="Tutor ID", how="left", suffixes=("", "_drop"))
        .where(df["Region"]=="Brazil", df)
    )

    # 3) QA
    def load_qa(ss_id):
        q = load_sheet_values(ss_id, QA_SHEET)
        q["Date"] = pd.to_datetime(q["B"], errors="coerce")
        return q[["A","E","Date","C","D"]].rename(columns={
            "A":"Tutor ID","E":"Group",
            "C":"QA score","D":"QA marker"
        })

    q_lat = load_qa(QA_LATAM_SS)
    q_brz = load_qa(QA_BRAZIL_SS)

    df = (
        df
        .merge(q_lat, on=["Tutor ID","Group","Date of the lesson"], how="left")
        .where(df["Region"]=="LATAM", df)
        .merge(q_brz, on=["Tutor ID","Group","Date of the lesson"], how="left")
        .where(df["Region"]=="Brazil", df)
    )

    # 4) Replacement
    rp = load_sheet_values(REPL_SS, REPL_SHEET)
    rp["Date"]  = pd.to_datetime(rp["D"], errors="coerce")
    rp["Group"] = rp["F"]
    rp = rp[["Date","Group"]].assign(**{"Replacement or not":"Replacement/Postponement"})

    df = df.merge(
        rp,
        left_on=["Date of the lesson","Group"],
        right_on=["Date","Group"],
        how="left"
    )
    df["Replacement or not"] = df["Replacement or not"].fillna("")

    return df

# === UI ===
st.set_page_config(layout="wide")
df = build_df()

st.sidebar.header("Filters")
filters = {}
for col in df.columns:
    if df[col].dtype == "object":
        opts = sorted(df[col].dropna().unique())
        filters[col] = st.sidebar.multiselect(col, opts, default=opts)

mask = pd.Series(True, index=df.index)
for c, sel in filters.items():
    mask &= df[c].isin(sel)

dff = df[mask]

st.title("QA & Rating Dashboard")
st.dataframe(dff, use_container_width=True)

csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_dashboard.csv", "text/csv")
