#!/usr/bin/env python3
import streamlit as st
import os, json, time
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Constants ===
LATAM_SS         = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
BRAZIL_SS        = LATAM_SS   # same spreadsheet, different sheets
RATING_LATAM_SS  = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
RATING_BRAZIL_SS = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"

# Auth
scope   = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
sa      = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
creds   = ServiceAccountCredentials.from_json_keyfile_dict(sa, scope)
client  = gspread.authorize(creds)

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for attempt in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except Exception:
            if attempt == max_attempts:
                raise
            time.sleep(backoff)
            backoff *= 2

def load_lessons(ss_id, sheet_name, region):
    sh   = api_retry(client.open_by_key, ss_id)
    ws   = api_retry(sh.worksheet, sheet_name)
    rows = api_retry(ws.get_all_values)

    # pad header+rows so all have same length
    max_cols = max(len(r) for r in rows)
    header   = rows[0] + [""]*(max_cols - len(rows[0]))
    data     = [r + [""]*(max_cols - len(r)) for r in rows[1:]]

    df = pd.DataFrame(data, columns=header)
    # select columns by index (zero-based): R=17, Q=16, B=1, J=9, N=13, G=6, H=7, Y=24
    df = df.iloc[:, [17,16,1,9,13,6,7,24]]
    df.columns = [
        "Tutor name","Tutor ID","Date of the lesson","Group",
        "Course ID","Module","Lesson","Lesson Link"
    ]
    df["Region"] = region
    df["Date of the lesson"] = pd.to_datetime(df["Date of the lesson"], errors="coerce")
    return df

# load and combine
df_latam  = load_lessons(LATAM_SS,  "lessons LATAM",  "LATAM")
df_brazil = load_lessons(BRAZIL_SS, "lessons Brazil", "Brazil")
df = pd.concat([df_latam, df_brazil], ignore_index=True)

# Streamlit display
st.title("QA & Rating Dashboard")
st.dataframe(df, use_container_width=True)
