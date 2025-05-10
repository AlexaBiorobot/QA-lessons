#!/usr/bin/env python3
import time
import io

import streamlit as st
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError

# === Constants (жестко прописаны) ===
LESSONS_SS       = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
LATAM_GID        = "0"           # gid листа "lessons LATAM"
BRAZIL_GID       = "835553195"   # gid листа "lessons Brazil"

RATING_LATAM_SS  = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
RATING_BRAZIL_SS = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"
RATING_SHEET     = "Rating"

QA_LATAM_SS      = RATING_LATAM_SS
QA_BRAZIL_SS     = RATING_BRAZIL_SS
QA_SHEET         = "QA - Lesson evaluation"

REPL_SS          = "1LF2NrAm8J3c43wOoumtsyfQsX1z0_lUQVdByGSPe27U"
REPL_SHEET       = "Replacement"


@st.cache_data(show_spinner=False)
def get_client():
    """Авторизация сервис-аккаунтом для gspread."""
    # читаем JSON из переменной окружения или из Secrets Streamlit Cloud
    sa_json = (
        st.secrets.get("GCP_SERVICE_ACCOUNT")
        if not (env := st.secrets.get("GCP_SERVICE_ACCOUNT", None))
        else env
    )
    sa_json = sa_json or st.secrets["GCP_SERVICE_ACCOUNT"]
    import json
    info    = json.loads(sa_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        info,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ],
    )
    return gspread.authorize(creds)


def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    """Retry при 5xx APIError."""
    backoff = initial_backoff
    for i in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            code = getattr(e.response, "status_code", None) \
                   or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise


def load_public_lessons(ss_id: str, gid: str, region: str) -> pd.DataFrame:
    """Чтение public CSV-экспорта Lessons LATAM/Brazil."""
    url  = f"https://docs.google.com/spreadsheets/d/{ss_id}/export?format=csv&gid={gid}"
    resp = requests.get(url)
    resp.raise_for_status()
    df   = pd.read_csv(io.StringIO(resp.text), dtype=str)
    df   = df.iloc[:, [17,16,1,9,13,6,7,24]].copy()  # R,Q,B,J,N,G,H,Y
    df.columns = [
        "Tutor name","Tutor ID","Date of the lesson","Group",
        "Course ID","Module","Lesson","Lesson Link"
    ]
    df["Region"] = region
    df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
    return df


def load_sheet_with_header2(ss_id: str, sheet_name: str) -> pd.DataFrame:
    """То же, что load_sheet_values, но шапка во второй строке."""
    client = get_client()
    sh     = api_retry(client.open_by_key, ss_id)
    ws     = api_retry(sh.worksheet, sheet_name)
    rows   = api_retry(ws.get_all_values)

    if len(rows) < 2:
        raise RuntimeError(f"Лист «{sheet_name}» слишком короткий")
    # в rows[1] настоящая шапка, данные с 2-й строки (индекс 2 и дальше)
    header = rows[1]
    data   = rows[2:]

    maxc   = max(len(header), *(len(r) for r in data))
    header = header + [""]*(maxc - len(header))
    data   = [r + [""]*(maxc - len(r)) for r in data]

    return pd.DataFrame(data, columns=header)


@st.cache_data(show_spinner=True)
def build_df() -> pd.DataFrame:
    # 1) Lessons
    df_lat = load_public_lessons(LESSONS_SS, LATAM_GID,  "LATAM")
    df_brz = load_public_lessons(LESSONS_SS, BRAZIL_GID, "Brazil")
    df     = pd.concat([df_lat, df_brz], ignore_index=True)

    # 2) Rating
    def load_rating(ss_id: str) -> pd.DataFrame:
        # используем функцию с header_row=1
        r = load_sheet_with_header2(ss_id, RATING_SHEET)
        cols = [
            "Tutor",
            "ID",
            "Rating",
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
        .merge(r_lat, on="Tutor ID", how="left")
        .where(df["Region"]=="LATAM", df)
        .merge(r_brz, on="Tutor ID", how="left")
        .where(df["Region"]=="Brazil", df)
    )

    # 3) QA
    def load_qa(ss_id: str) -> pd.DataFrame:
        q = load_sheet_values(ss_id, QA_SHEET)
        q["Date"] = pd.to_datetime(q["B"], errors="coerce")
        return (
            q[["A","E","Date","C","D"]]
             .rename(columns={"A":"Tutor ID","E":"Group","C":"QA score","D":"QA marker"})
        )

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

    # Важно: в конце build_df() — вернуть df!
    return df


# === Streamlit UI ===
st.set_page_config(layout="wide")
df = build_df()

# Sidebar filters
st.sidebar.header("Filters")
filters = {}
for col in df.columns:
    if df[col].dtype == object:
        opts = sorted(df[col].dropna().unique())
        filters[col] = st.sidebar.multiselect(col, opts, default=opts)

mask = pd.Series(True, index=df.index)
for c, sel in filters.items():
    mask &= df[c].isin(sel)
dff = df[mask]

st.title("📊 QA & Rating Dashboard")
st.dataframe(dff, use_container_width=True)

# Download CSV
csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_dashboard.csv", "text/csv")
