import streamlit as st
import os, json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# === Constants ===
LATAM_SS = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
BRAZIL_SS = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"  # same doc, different sheets
RATING_LATAM_SS = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
RATING_BRAZIL_SS = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"
QA_LATAM_SS = RATING_LATAM_SS
QA_BRAZIL_SS = RATING_BRAZIL_SS
REPL_SS = "1LF2NrAm8J3c43wOoumtsyfQsX1z0_lUQVdByGSPe27U"

# authorize
def authorize():
    scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    sa = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa, scope)
    return gspread.authorize(creds)

client = authorize()

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for attempt in range(1, max_attempts+1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            code = getattr(e, 'response', None)
            if code and attempt < max_attempts:
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

# load lessons
def load_lessons(sheet_name, region):
    rows = api_retry(client.open_by_key, LATAM_SS if region=='LATAM' else BRAZIL_SS)
    ws = api_retry(rows.worksheet, sheet_name)
    df = pd.DataFrame(api_retry(ws.get_all_records))
    df['Region'] = region
    return df

df_latam = load_lessons('lessons LATAM', 'LATAM')
df_brazil = load_lessons('lessons Brazil', 'Brazil')

# select and rename
cols = ['R','Q','B','J','N','G','H','Y','Region']
names = ['Tutor name','Tutor ID','Date','Group','Course ID','Module','Lesson','Lesson Link','Region']
df = pd.concat([df_latam, df_brazil], ignore_index=True)[cols]
df.columns = names

# load ratings
def load_rating(ss_id, rating_col):
    ws = api_retry(client.open_by_key, ss_id).worksheet('Rating')
    r = pd.DataFrame(api_retry(ws.get_all_records))
    cols = ['A', rating_col] + list('FGHIJK')
    r = r[cols]
    r.columns = ['Tutor ID','Rating','F','G','H','I','J','K']
    return r

ratings = pd.concat([load_rating(RATING_LATAM_SS,'BU'), load_rating(RATING_BRAZIL_SS,'BO')], ignore_index=True)
# merge

df = df.merge(ratings, on='Tutor ID', how='left')

# load QA evals
def load_qa(ss_id):
    ws = api_retry(client.open_by_key, ss_id).worksheet('QA - Lesson evaluation')
    qa = pd.DataFrame(api_retry(ws.get_all_records))
    qa = qa[['E','B','C','D']]
    qa.columns = ['Group','Date','QA score','QA marker']
    qa['Date'] = pd.to_datetime(qa['Date'])
    return qa

qa = pd.concat([load_qa(QA_LATAM_SS), load_qa(QA_BRAZIL_SS)], ignore_index=True)

df['Date'] = pd.to_datetime(df['Date'])
df = df.merge(qa, on=['Group','Date'], how='left')

# load replacements
rep = pd.DataFrame(api_retry(client.open_by_key, REPL_SS).worksheet('Replacement').get_all_records())
rep['Date'] = pd.to_datetime(rep['D'])
rep_flag = rep[['F','Date']].drop_duplicates()
rep_flag.columns = ['Group','Date']
rep_flag['Replacement or not'] = 'Replacement/Postponement'

df = df.merge(rep_flag, on=['Group','Date'], how='left')
df['Replacement or not'] = df['Replacement or not'].fillna('')

# aggregates
now = datetime.now()
thresh = now - timedelta(days=90)
for col in ['QA score','QA marker']:
    df[f'# {col} total'] = df.groupby('Tutor ID')[col].transform('count')
    df[f'# {col} 90d'] = df[df['Date']>=thresh].groupby('Tutor ID')[col].transform('count')
    df[f'Avg {col} total'] = df.groupby('Tutor ID')[col].transform('mean')
    df[f'Avg {col} 90d'] = df[df['Date']>=thresh].groupby('Tutor ID')[col].transform('mean')

# Streamlit UI
st.set_page_config(layout='wide')
st.title('Tutor QA & Rating Dashboard')

# Sidebar filters
with st.sidebar:
    filters = {}
    for c in df.columns:
        if df[c].dtype == 'object' or c in ['Date','Region']:
            options = df[c].dropna().unique().tolist()
            sel = st.multiselect(c, options)
            filters[c] = sel
# apply filters
df_filtered = df.copy()
for c, sel in filters.items():
    if sel:
        df_filtered = df_filtered[df_filtered[c].isin(sel)]

# main table
st.dataframe(df_filtered, use_container_width=True)

# detail
st.write(df_filtered)
