#!/usr/bin/env python3
import time, io, os
import streamlit as st
import pandas as pd
import requests
import gspread
print("gspread version:", gspread.__version__)
from gspread.exceptions import APIError
from google.auth.exceptions import GoogleAuthError

from google.auth.transport.requests import AuthorizedSession
AuthorizedSession._auth_request = AuthorizedSession.request

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

    # 1) Сначала пробуем взять JSON из переменной окружения
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT")
    if sa_json:
        info = json.loads(sa_json)
    else:
        # 2) Иначе — из streamlit secrets (он уже dict)
        info = st.secrets["GCP_SERVICE_ACCOUNT"]

    # 3) Скоупы для Sheets + Drive
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    creds = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for i in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

def load_public_lessons(ss_id: str, gid: str, region: str) -> pd.DataFrame:
    """Читаем публично через CSV-export, fallback на GSpread если пусто.
       При отсутствии нужных колонок — подставляем пустые."""
    url = f"https://docs.google.com/spreadsheets/d/{ss_id}/export?format=csv&gid={gid}"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        raw = pd.read_csv(io.StringIO(resp.text), dtype=str)
    except (pd.errors.EmptyDataError, requests.RequestException):
        raw = load_sheet_values(ss_id, sheet_name=None, gid=gid)

    # нужные исходные заголовки в вашем GS-списке
    wanted = ["R", "Q", "B", "J", "N", "G", "H", "Y"]
    # если кто-то пропал — добавляем колонку с NaN (или пустой строкой)
    for col in wanted:
        if col not in raw.columns:
            raw[col] = pd.NA
    # теперь точно можно резать по ним
    df = raw[wanted]
    df.columns = [
        "Tutor name","Tutor ID","Date of the lesson","Group",
        "Course ID","Module","Lesson","Lesson Link"
    ]

    df["Region"] = region
    df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
    return df

def load_sheet_values(ss_id: str, sheet_name: str = None, gid: str = None) -> pd.DataFrame:
    """Универсальное чтение: если sheet_name задан — по имени, иначе — по gid."""
    client = get_client()
    sh     = api_retry(client.open_by_key, ss_id)
    if sheet_name:
        ws = api_retry(sh.worksheet, sheet_name)
    else:
        # ищем вкладку по numeric gid
        ws = next(w for w in api_retry(sh.worksheets) if str(w.id) == str(gid))
    rows = api_retry(ws.get_all_values)
    maxc = max(len(r) for r in rows)
    header = rows[0] + [""]*(maxc - len(rows[0]))
    data = [r + [""]*(maxc - len(r)) for r in rows[1:]]
    return pd.DataFrame(data, columns=header)

# === Основная сборка данных ===
@st.cache_data(show_spinner=True)
def build_df() -> pd.DataFrame:
    # 1) уроки
    df_lat = load_public_lessons(LESSONS_SS, LATAM_GID,  "LATAM")
    df_brz = load_public_lessons(LESSONS_SS, BRAZIL_GID, "Brazil")
    df     = pd.concat([df_lat, df_brz], ignore_index=True)

    # 2) рейтинг
    def load_rating(ss_id: str) -> pd.DataFrame:
        client = get_client()
        sh     = api_retry(client.open_by_key, ss_id)
        ws     = api_retry(sh.worksheet, RATING_SHEET)
        rows   = api_retry(ws.get_all_values)
        # заголовок — в строке 1, данные — со 2-й
        header = rows[1]
        data   = rows[2:]
        maxc   = max(len(header), *(len(r) for r in data))
        header = header + [""]*(maxc - len(header))
        data   = [r + [""]*(maxc - len(r)) for r in data]
        r = pd.DataFrame(data, columns=header)
        # выбрали нужные колонки (проверьте точные названия в вашей таблице!)
        cols = [
            "Tutor ID",   # или "ID" — подставьте то, что реально у вас во второй строке
            "Rating",
            "Num of QA scores",
            "Num of QA scores (last 90 days)",
            "Average QA score",
            "Average QA score (last 2 scores within last 90 days)",
            "Average QA marker",
            "Average QA marker (last 2 markers within last 90 days)",
        ]
        # если у вас там столбец называется "ID", то:
        if "ID" in r.columns and "Tutor ID" not in r.columns:
            r = r.rename(columns={"ID": "Tutor ID"})
        return r[cols]

    r_lat = load_rating(RATING_LATAM_SS)
    r_brz = load_rating(RATING_BRAZIL_SS)

    # merge по региону
    df = (
        df
        .merge(r_lat, on="Tutor ID", how="left")
        .where(df["Region"]=="LATAM", df)
        .merge(r_brz, on="Tutor ID", how="left")
        .where(df["Region"]=="Brazil", df)
    )

    # 3) QA оценки (шапка в первой строке)
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
        .merge(q_lat, on=["Tutor ID","Group","Date of the lesson"], how="left")
        .where(df["Region"]=="LATAM", df)
        .merge(q_brz, on=["Tutor ID","Group","Date of the lesson"], how="left")
        .where(df["Region"]=="Brazil", df)
    )

    # 4) Replacement
    rp = load_sheet_values(REPL_SS, sheet_name=REPL_SHEET)
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

# === Streamlit UI ===
st.set_page_config(layout="wide")
df = build_df()

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

# Скачать CSV
csv = dff.to_csv(index=False)
st.download_button("📥 Download CSV", csv, "qa_dashboard.csv", "text/csv")
