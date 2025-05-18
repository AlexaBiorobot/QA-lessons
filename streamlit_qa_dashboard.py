#!/usr/bin/env python3
import os
import io
import time

import streamlit as st
st.set_page_config(layout="wide")

import pandas as pd
import requests
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials

# === Константы ===
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

# === Auth helpers ===
@st.cache_data(show_spinner=False)
def get_creds():
    import json
    raw = os.getenv("GCP_SERVICE_ACCOUNT")
    if raw:
        info = json.loads(raw)
    else:
        info = st.secrets["GCP_SERVICE_ACCOUNT"]
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    return Credentials.from_service_account_info(info, scopes=scopes)

def get_auth_header():
    creds = get_creds()
    creds.refresh(Request())
    return {"Authorization": f"Bearer {creds.token}"}

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for i in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except requests.HTTPError as e:
            code = e.response.status_code
            if 500 <= code < 600 and i < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

# === CSV-экспорт для публичных листов ===
def fetch_csv(ss_id: str, gid: str) -> pd.DataFrame:
    url = f"https://docs.google.com/spreadsheets/d/{ss_id}/export?format=csv&gid={gid}"
    headers = get_auth_header()
    resp = api_retry(requests.get, url, headers=headers, timeout=20)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text), dtype=str)

# === Google Sheets API v4 для приватных range ===
def fetch_values(ss_id: str, sheet_name: str) -> list[list[str]]:
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{ss_id}/values/{sheet_name}"
    headers = get_auth_header()
    resp = api_retry(requests.get, url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("values", [])

# === Ваши загрузчики ===
def load_public_lessons(ss_id: str, gid: str, region: str) -> pd.DataFrame:
    raw = fetch_csv(ss_id, gid)
    wanted = ["R","Q","B","J","N","G","H","Y"]
    for c in wanted:
        if c not in raw.columns:
            raw[c] = pd.NA
    df = raw[wanted]
    df.columns = [
        "Tutor name","Tutor ID","Date of the lesson","Group",
        "Course ID","Module","Lesson","Lesson Link"
    ]
    df["Region"] = region
    df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
    return df

def load_rating(ss_id: str) -> pd.DataFrame:
    cols = [
        "Tutor ID","Rating","Num of QA scores",
        "Num of QA scores (last 90 days)","Average QA score",
        "Average QA score (last 2 scores within last 90 days)",
        "Average QA marker","Average QA marker (last 2 markers within last 90 days)"
    ]

    # Если запрос упал — возвращаем пустой DataFrame с нужными колонками
    try:
        rows = fetch_values(ss_id, RATING_SHEET)
    except requests.HTTPError:
        return pd.DataFrame(columns=cols)

    # Если даже заголовков нет — тоже возвращаем пустой
    if len(rows) < 2:
        return pd.DataFrame(columns=cols)

    header = rows[1]
    data   = rows[2:]
    maxc   = max(len(header), *(len(r) for r in data))
    header = header + [""] * (maxc - len(header))
    data   = [r + [""] * (maxc - len(r)) for r in data]
    df     = pd.DataFrame(data, columns=header)

    # Переименование столбца ID, если нужно
    if "ID" in df.columns and "Tutor ID" not in df.columns:
        df = df.rename(columns={"ID": "Tutor ID"})

    # Добавляем недостающие колонки как пустые
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA

    return df[cols]

def load_qa(ss_id: str) -> pd.DataFrame:
    rows = fetch_values(ss_id, QA_SHEET)
    data = rows[1:]  # без заголовка
    df = pd.DataFrame({
        "Tutor ID":  [r[0] if len(r) > 0 else pd.NA for r in data],
        "Group":     [r[4] if len(r) > 4 else pd.NA for r in data],
        "QA score":  [r[2] if len(r) > 2 else pd.NA for r in data],
        "QA marker": [r[3] if len(r) > 3 else pd.NA for r in data],
        "Date":      pd.to_datetime([r[1] if len(r) > 1 else None for r in data], errors="coerce"),
    })
    return df[["Tutor ID","Group","Date","QA score","QA marker"]]

def load_replacements() -> pd.DataFrame:
    rows = fetch_values(REPL_SS, REPL_SHEET)
    df   = pd.DataFrame(rows[1:], columns=rows[0])
    df["Date"]  = pd.to_datetime(df["D"], errors="coerce")
    df["Group"] = df["F"]
    return df[["Date","Group"]].assign(**{"Replacement or not":"Replacement/Postponement"})

# === Собираем всё в один DataFrame ===
@st.cache_data(show_spinner=True)
def build_df():
    df_lat = load_public_lessons(LESSONS_SS, LATAM_GID, "LATAM")
    df_brz = load_public_lessons(LESSONS_SS, BRAZIL_GID, "Brazil")
    df     = pd.concat([df_lat, df_brz], ignore_index=True)

    r_lat  = load_rating(RATING_LATAM_SS)
    r_brz  = load_rating(RATING_BRAZIL_SS)
    df = (df
          .merge(r_lat, on="Tutor ID", how="left").where(df["Region"]=="LATAM", df)
          .merge(r_brz, on="Tutor ID", how="left").where(df["Region"]=="Brazil", df))

    q_lat = load_qa(QA_LATAM_SS)
    q_brz = load_qa(QA_BRAZIL_SS)
    df = (df
          .merge(q_lat, on=["Tutor ID","Group","Date of the lesson"], how="left").where(df["Region"]=="LATAM", df)
          .merge(q_brz, on=["Tutor ID","Group","Date of the lesson"], how="left").where(df["Region"]=="Brazil", df))

    rp = load_replacements()
    df = df.merge(rp, left_on=["Date of the lesson","Group"], right_on=["Date","Group"], how="left")
    df["Replacement or not"] = df["Replacement or not"].fillna("")
    return df

# === Streamlit UI ===
df = build_df()
st.sidebar.header("Filters")
filters = {
    c: st.sidebar.multiselect(c, sorted(df[c].dropna().unique()), default=sorted(df[c].dropna().unique()))
    for c in df.columns if df[c].dtype == object
}

mask = pd.Series(True, index=df.index)
for c, sel in filters.items():
    mask &= df[c].isin(sel)
dff = df[mask]

st.title("📊 QA & Rating Dashboard")
st.dataframe(dff, use_container_width=True)
csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_dashboard.csv", "text/csv")
