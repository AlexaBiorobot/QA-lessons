#!/usr/bin/env python3
import os
import json
import logging
import time

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ——— Жёстко прописанные константы ———
SRC_SS_ID          = "1gk6AV3sKtrMVG8Oxyzf2cODv_vmAIThiX5cHCVxSNjE"
SRC_SHEET_NAME_1   = "QA Workspace"
SRC_SHEET_NAME_2   = "QA Workspace Graduations"

DST_SS_ID          = "1njy8V5lyG3vyENr1b50qGd3infU4VHYP4CfaD0H1AlM"
DST_SHEET_NAME     = "Lessons"
# ————————————————————————————————

SERVICE_ACCOUNT_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    """Retry any Google Sheets API call on 5xx errors."""
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            code = None
            if e.response:
                code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and attempt < max_attempts:
                logging.warning(f"API {code} on attempt {attempt}, retrying in {backoff:.1f}s…")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

def extract_next_after_tutor(rows, max_cols=15):
    """
    rows: список списков из диапазона A:O (или шире).
    Возвращает список строк (только A:O), которые идут сразу после строк, где A == 'Tutor'.
    """
    out = []
    for idx, row in enumerate(rows):
        if idx + 1 < len(rows) and (row[0] if row else "") == "Tutor":
            nxt = rows[idx + 1][:max_cols]
            if len(nxt) < max_cols:
                nxt = nxt + [""] * (max_cols - len(nxt))
            out.append(nxt)
    return out

def read_source_df(sh_src, sheet_name):
    """Читает один лист (A:O), вытаскивает нужные строки и возвращает DataFrame."""
    try:
        ws = api_retry(sh_src.worksheet, sheet_name)
    except WorksheetNotFound:
        logging.warning(f"Sheet '{sheet_name}' not found, skipping.")
        return None

    rows = api_retry(ws.get, "A:O")
    if not rows:
        logging.info(f"No data in '{sheet_name}'.")
        return None

    header = rows[0][:15]
    if len(header) < 15:
        header = header + [""] * (15 - len(header))

    out = extract_next_after_tutor(rows, max_cols=15)
    if not out:
        logging.info(f"No rows after 'Tutor' in '{sheet_name}'.")
        return None

    df = pd.DataFrame(out, columns=header)
    logging.info(f"✔ Collected {len(df)} rows from '{sheet_name}'.")
    return df

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Открываем источник
    sh_src = api_retry(client.open_by_key, SRC_SS_ID)

    # 3) Читаем оба листа и объединяем
    df_list = []
    for sheet_name in (SRC_SHEET_NAME_1, SRC_SHEET_NAME_2):
        df = read_source_df(sh_src, sheet_name)
        if df is not None:
            df_list.append(df)

    if not df_list:
        logging.info("No rows from any source sheets, nothing to write.")
        return

    df_all = pd.concat(df_list, ignore_index=True)

    # 4) Пишем в назначение (A2:O)
    sh_dst = api_retry(client.open_by_key, DST_SS_ID)
    ws_dst = api_retry(sh_dst.worksheet, DST_SHEET_NAME)

    existing = api_retry(ws_dst.get, "A2:O")
    end_row = 1 + len(existing)  # A2…A{end_row}
    if end_row >= 2:
        api_retry(ws_dst.batch_clear, [f"A2:O{end_row}"])

    api_retry(
        set_with_dataframe,
        ws_dst,
        df_all,
        row=2,
        col=1,
        include_index=False,
        include_column_header=False,
        resize=False
    )

    logging.info(f"✔ Written {len(df_all)} rows to '{DST_SHEET_NAME}' starting at A2:O")

if __name__ == "__main__":
    main()
