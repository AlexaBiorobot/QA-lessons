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

# Здесь реальные GID-ы листов lessons LATAM и lessons Brazil:
LATAM_GID         = "0"           # обычно первая вкладка
BRAZIL_GID        = "835553195"  # замените на ваш фактический gid

RATING_LATAM_SS   = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
RATING_BRAZIL_SS  = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"
RATING_SHEET      = "Rating"

QA_LATAM_SS       = RATING_LATAM_SS
QA_BRAZIL_SS      = RATING_BRAZIL_SS
QA_SHEET          = "QA - Lesson evaluation"

REPL_SS           = "1LF2NrAm8J3c43wOoumtsyfQsX1z0_lUQVdByGSPe27U"
REPL_SHEET        = "Replacement"
# —————————————————————————————

def load_public_sheet(ss_id: str, gid: str) -> pd.DataFrame:
    """Читает публичный лист через CSV-экспорт без авторизации."""
    url = f"https://docs.google.com/spreadsheets/d/{ss_id}/export?format=csv&gid={gid}"
    return pd.read_csv(url)

@st.cache_data
def get_client():
    import streamlit as _st
    scope  = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa_json = os.getenv("GCP_SERVICE_ACCOUNT") or _st.secrets["GCP_SERVICE_ACCOUNT"]
    sa     = json.loads(sa_json)
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(sa, scope)
    return gspread.authorize(creds)

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for i in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
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
        if sheet_name == LATAM_SHEET:
            raw = load_public_sheet(ss_id, LATAM_GID)
        elif sheet_name == BRAZIL_SHEET:
            raw = load_public_sheet(ss_id, BRAZIL_GID)
        else:
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

    # 2) рейтинг, QA, Replacement — остаётся без изменений, через load_sheet_values
    # … ваш код с merge r_lat, r_brz, q_lat, q_brz, df_rp …

    return df

# === UI ===
st.set_page_config(layout="wide")
df = build_df()
# … остальная часть вашего Streamlit-интерфейса …
