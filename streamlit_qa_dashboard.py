#!/usr/bin/env python3
import time, io, os
import streamlit as st
import pandas as pd
import requests
import gspread
from gspread.exceptions import APIError
from google.auth.exceptions import GoogleAuthError
from google.oauth2.service_account import Credentials

# === Constants ===
LESSONS_SS       = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
LATAM_GID        = "0"
BRAZIL_GID       = "835553195"

RATING_LATAM_SS  = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
RATING_BRAZIL_SS = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"
RATING_SHEET     = "Rating"

QA_LATAM_SS      = RATING_LATAM_SS
QA_BRAZIL_SS     = RATING_BRAZIL_SS
QA_SHEET         = "QA - Lesson evaluation"

REPL_SS          = "1LF2NrAm8J3c43wOoumtsyfQsX1z0_lUQVdByGSPe27U"
REPL_SHEET       = "Replacement"

# === Auth & helpers ===
@st.cache_data(show_spinner=False)
def get_client():
    import json

    sa_json = os.getenv("GCP_SERVICE_ACCOUNT")
    if sa_json:
        info = json.loads(sa_json)
    else:
        info = st.secrets["GCP_SERVICE_ACCOUNT"]

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_info(info, scopes=scopes)
    client = gspread.authorize(creds)
    return client


def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except (APIError, GoogleAuthError) as e:
            status = None
            if hasattr(e, "response"):
                status = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            # только на 5xx пробуем бэкофф
            if status and 500 <= int(status) < 600 and attempt < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            # иначе — пробрасываем
            raise

def load_public_lessons(ss_id: str, gid: str, region: str) -> pd.DataFrame:
    """Читаем публично через CSV-export, fallback на GSpread если пусто."""
    url = f"https://docs.google.com/spreadsheets/d/{ss_id}/export?format=csv&gid={gid}"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        raw = pd.read_csv(io.StringIO(resp.text), dtype=str)
    except (pd.errors.EmptyDataError, requests.RequestException):
        raw = load_sheet_values(ss_id, sheet_name=None, gid=gid)

    wanted = ["R","Q","B","J","N","G","H","Y"]
    for col in wanted:
        if col not in raw.columns:
            raw[col] = pd.NA
    df = raw[wanted]
    df.columns = [
        "Tutor name","Tutor ID","Date of the lesson","Group",
        "Course ID","Module","Lesson","Lesson Link"
    ]
    df["Region"] = region
    df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
    return df

def load_sheet_values(ss_id: str, sheet_name: str = None, gid: str = None) -> pd.DataFrame:
    client = get_client()
    sh     = api_retry(client.open_by_key, ss_id)
    if sheet_name:
        ws = api_retry(sh.worksheet, sheet_name)
    else:
        ws = next(w for w in api_retry(sh.worksheets) if str(w.id) == str(gid))
    rows = api_retry(ws.get_all_values)
    maxc = max(len(r) for r in rows)
    header = rows[0] + [""]*(maxc - len(rows[0]))
    data = [r + [""]*(maxc - len(r)) for r in rows[1:]]
    return pd.DataFrame(data, columns=header)

@st.cache_data(show_spinner=True)
def build_df() -> pd.DataFrame:
    # 1) Lessons
    df_lat = load_public_lessons(LESSONS_SS, LATAM_GID, "LATAM")
    df_brz = load_public_lessons(LESSONS_SS, BRAZIL_GID, "Brazil")
    df     = pd.concat([df_lat, df_brz], ignore_index=True)

    # 2) Rating
    def load_rating(ss_id: str) -> pd.DataFrame:
        client = get_client()
        sh     = api_retry(client.open_by_key, ss_id)
        ws     = api_retry(sh.worksheet, RATING_SHEET)
        rows   = api_retry(ws.get_all_values)
        header = rows[1]
        data   = rows[2:]
        maxc   = max(len(header), *(len(r) for r in data))
        header = header + [""]*(maxc - len(header))
        data   = [r + [""]*(maxc - len(r)) for r in data]
        r = pd.DataFrame(data, columns=header)
        cols = [
            "Tutor ID","Rating","Num of QA scores",
            "Num of QA scores (last 90 days)","Average QA score",
            "Average QA score (last 2 scores within last 90 days)",
            "Average QA marker","Average QA marker (last 2 markers within last 90 days)"
        ]
        if "ID" in r.columns and "Tutor ID" not in r.columns:
            r = r.rename(columns={"ID": "Tutor ID"})
        return r[cols]

    r_lat = load_rating(RATING_LATAM_SS)
    r_brz = load_rating(RATING_BRAZIL_SS)

    df = (
        df
        .merge(r_lat, on="Tutor ID", how="left").where(df["Region"]=="LATAM", df)
        .merge(r_brz, on="Tutor ID", how="left").where(df["Region"]=="Brazil", df)
    )

    # 3) QA
    def load_qa(ss_id: str) -> pd.DataFrame:
        q = load_sheet_values(ss_id, sheet_name=QA_SHEET)
        q["Date"] = pd.to_datetime(q["B"], errors="coerce")
        return (
            q[["A","E","Date","C","D"]]
             .rename(columns={"A":"Tutor ID","E":"Group","C":"QA score","D":"QA marker"})
        )

    q_lat = load_qa(QA_LATAM_SS)
    q_brz = load_qa(QA_BRAZIL_SS)

    df = (
        df
        .merge(q_lat, on=["Tutor ID","Group","Date of the lesson"], how="left").where(df["Region"]=="LATAM", df)
        .merge(q_brz, on=["Tutor ID","Group","Date of the lesson"], how="left").where(df["Region"]=="Brazil", df)
    )

    # 4) Replacement
    rp = load_sheet_values(REPL_SS, sheet_name=REPL_SHEET)
    rp["Date"]  = pd.to_datetime(rp["D"], errors="coerce")
    rp["Group"] = rp["F"]
    rp = rp[["Date","Group"]].assign(**{"Replacement or not":"Replacement/Postponement"})

    df = df.merge(rp, left_on=["Date of the lesson","Group"], right_on=["Date","Group"], how="left")
    df["Replacement or not"] = df["Replacement or not"].fillna("")
    return df

# === Streamlit UI ===
st.set_page_config(layout="wide")
df = build_df()

st.sidebar.header("Filters")
filters = {c: st.sidebar.multiselect(c, sorted(df[c].dropna().unique()), default=sorted(df[c].dropna().unique()))
           for c in df.columns if df[c].dtype == object}

mask = pd.Series(True, index=df.index)
for c, sel in filters.items():
    mask &= df[c].isin(sel)
dff = df[mask]

st.title("📊 QA & Rating Dashboard")
st.dataframe(dff, use_container_width=True)

csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_dashboard.csv", "text/csv")
