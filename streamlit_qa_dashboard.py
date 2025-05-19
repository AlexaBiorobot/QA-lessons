#!/usr/bin/env python3
import os
import io
import time

import streamlit as st
st.set_page_config(layout="wide")

import pandas as pd
import pandas.api.types as pt
import requests
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from urllib.parse import quote

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
    # Убираем headers=get_auth_header()
    resp = api_retry(requests.get, url, timeout=20)
    resp.raise_for_status()
    try:
        return pd.read_csv(io.StringIO(resp.text), dtype=str)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()

# === Google Sheets API v4 для приватных range ===
def fetch_values(ss_id: str, sheet_name: str) -> list[list[str]]:
    encoded = quote(sheet_name, safe='')
    url     = f"https://sheets.googleapis.com/v4/spreadsheets/{ss_id}/values/{encoded}"
    headers = get_auth_header()
    resp    = api_retry(requests.get, url, headers=headers)
    resp.raise_for_status()
    return resp.json().get("values", [])

# === Загрузчики ===
def load_public_lessons(ss_id: str, gid: str, region: str) -> pd.DataFrame:
    raw = fetch_csv(ss_id, gid)
    cols = [
        "teacher_name","teacher_id","lesson_date","group_title",
        "course_id","lesson_module","lesson_number","watch_url"
    ]
    for c in cols:
        if c not in raw.columns:
            raw[c] = pd.NA
    df = raw[cols]
    df.columns = [
        "Tutor name","Tutor ID","Date of the lesson","Group",
        "Course ID","Module","Lesson","Lesson Link"
    ]
    df["Region"] = region
    df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
    return df

def load_rating(ss_id: str) -> pd.DataFrame:
    want = [
        "Tutor ID","Rating","Num of QA scores",
        "Num of QA scores (last 90 days)","Average QA score",
        "Average QA score (last 2 scores within last 90 days)",
        "Average QA marker","Average QA marker (last 2 markers within last 90 days)"
    ]
    try:
        rows = fetch_values(ss_id, RATING_SHEET)
    except requests.HTTPError:
        return pd.DataFrame(columns=want)
    if not rows or len(rows)<2:
        return pd.DataFrame(columns=want)
    if "Tutor ID" in rows[0]:
        header, data = rows[0], rows[1:]
    else:
        header, data = rows[1], rows[2:]
    maxc   = max(len(header), *(len(r) for r in data))
    header = header + [""]*(maxc-len(header))
    data   = [r+[""]*(maxc-len(r)) for r in data]
    df     = pd.DataFrame(data, columns=header)
    if "ID" in df.columns and "Tutor ID" not in df.columns:
        df = df.rename(columns={"ID":"Tutor ID"})
    for c in want:
        if c not in df.columns:
            df[c] = pd.NA
    return df[want]

def load_qa(ss_id: str) -> pd.DataFrame:
    want = ["Tutor ID","Date of the lesson","QA score","QA marker"]
    try:
        rows = fetch_values(ss_id, QA_SHEET)
    except requests.HTTPError:
        return pd.DataFrame(columns=want)
    if not rows or len(rows)<2:
        return pd.DataFrame(columns=want)
    data = rows[1:]
    df = pd.DataFrame({
        "Tutor ID":           [r[6]  if len(r)>6 else pd.NA for r in data],
        "Date of the lesson": pd.to_datetime([r[1] if len(r)>1 else None for r in data],
                                              errors="coerce", dayfirst=True),
        "QA score":           [r[2]  if len(r)>2 else pd.NA for r in data],
        "QA marker":          [r[3]  if len(r)>3 else pd.NA for r in data],
    })
    return df

def load_replacements() -> pd.DataFrame:
    rows = fetch_values(REPL_SS, REPL_SHEET)
    if len(rows)<2:
        return pd.DataFrame(columns=["Date","Group","Replacement or not"])
    data = rows[1:]
    df   = pd.DataFrame({
        "Date":      pd.to_datetime([r[3] if len(r)>3 else None for r in data], errors="coerce"),
        "Group":     [r[5]  if len(r)>5 else pd.NA for r in data],
        "Replacement or not": "Replacement/Postponement"
    })
    return df

# === Собираем всё в один DF
@st.cache_data(show_spinner=True)
def build_df():
    df_lat = load_public_lessons(LESSONS_SS, LATAM_GID, "LATAM")
    df_brz = load_public_lessons(LESSONS_SS, BRAZIL_GID, "Brazil")
    df     = pd.concat([df_lat, df_brz], ignore_index=True)

    rating_cols = [
        "Rating","Num of QA scores","Num of QA scores (last 90 days)",
        "Average QA score","Average QA score (last 2 scores within last 90 days)",
        "Average QA marker","Average QA marker (last 2 markers within last 90 days)"
    ]
    r_lat = (load_rating(RATING_LATAM_SS)
             .rename(columns={c: c + "_lat" for c in rating_cols}))
    r_brz = (load_rating(RATING_BRAZIL_SS)
             .rename(columns={c: c + "_brz" for c in rating_cols}))
    df = df.merge(r_lat, on="Tutor ID", how="left") \
           .merge(r_brz, on="Tutor ID", how="left")

    # --- Склеиваем рейтинговые колонки обратно и отбрасываем суффиксы ---
    for c in rating_cols:
        df[c] = df[f"{c}_lat"].fillna(df[f"{c}_brz"])
    df.drop([f"{c}_lat" for c in rating_cols] + [f"{c}_brz" for c in rating_cols],
            axis=1, inplace=True)

    # QA: сначала LATAM, переименуем поля
    q_lat = load_qa(QA_LATAM_SS).rename(
        columns={"QA score":"QA score_lat","QA marker":"QA marker_lat"}
    )
    # потом Brazil
    q_brz = load_qa(QA_BRAZIL_SS).rename(
        columns={"QA score":"QA score_brz","QA marker":"QA marker_brz"}
    )
    df = df.merge(q_lat, on=["Tutor ID","Date of the lesson"], how="left") \
           .merge(q_brz, on=["Tutor ID","Date of the lesson"], how="left")

    # Объединяем QA
    for base in ["QA score","QA marker"]:
        df[base] = df[f"{base}_lat"].fillna(df[f"{base}_brz"])
        df.drop([f"{base}_lat", f"{base}_brz"], axis=1, inplace=True)

    # Replacements
    rp = load_replacements()
    df = df.merge(rp, left_on=["Date of the lesson","Group"],
                  right_on=["Date","Group"], how="left")
    df["Replacement or not"] = df["Replacement or not"].fillna("")
    df.drop(columns=["Date"], inplace=True)
    
    # === НОВЫЙ БЛОК: докидываем все записи из Lesson evaluation ===
    qa_all = pd.concat([load_qa(QA_LATAM_SS), load_qa(QA_BRAZIL_SS)], ignore_index=True)
    # создаем каркас с пустыми колонками, как в df
    empty = pd.DataFrame(columns=df.columns)
    # заполняем только наши четыре поля
    empty.loc[:, ["Tutor ID","Date of the lesson","QA score","QA marker"]] = qa_all[["Tutor ID","Date of the lesson","QA score","QA marker"]]
    # объединяем и убираем возможные дубликаты
    df = pd.concat([df, empty], ignore_index=True)
    df = df.drop_duplicates(subset=["Tutor ID","Date of the lesson","QA score","QA marker"], keep="first")

    df["Date of the lesson"] = df["Date of the lesson"].fillna(df["Eval Date"])

    # создаём отдельный столбец с датой именно из Lesson evaluation
    df["Eval Date"] = df["Date of the lesson"]

    return df

# === Streamlit UI ===
df = build_df()

# 1) Фильтр по диапазону дат урока (вставить перед блоком «Filters»)
st.sidebar.header("Lesson date")
# учитываем обе колонки — из публички и из QA-evaluation
min_date = min(df["Date of the lesson"].min(), df["Eval Date"].min())
max_date = max(df["Date of the lesson"].max(), df["Eval Date"].max())
start_date, end_date = st.sidebar.date_input(
    "Choose dates",
    value=[min_date, max_date],
    min_value=min_date,
    max_value=max_date
)
# маска: либо урок в публичке, либо запись есть только в QA
mask = (
    ((df["Date of the lesson"] >= pd.to_datetime(start_date)) &
     (df["Date of the lesson"] <= pd.to_datetime(end_date)))
  | ((df["Eval Date"]     >= pd.to_datetime(start_date)) &
     (df["Eval Date"]     <= pd.to_datetime(end_date)))
)

# 2) Остальные мультиселекты
st.sidebar.header("Filters")
filters = {
    c: st.sidebar.multiselect(
        c,
        sorted(df[c].dropna().unique()),
        default=[]
    )
    for c in df.columns
    if df[c].dtype == object or pt.is_numeric_dtype(df[c])
}

# добавляем остальные условия к той же маске
for c, sel in filters.items():
    if sel:
        mask &= df[c].isin(sel)

dff = df[mask]

st.title("📊 QA & Rating Dashboard")
st.dataframe(dff, use_container_width=True)
csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_dashboard.csv", "text/csv")
