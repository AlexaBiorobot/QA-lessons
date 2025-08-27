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
SRC_SS_ID        = "1gk6AV3sKtrMVG8Oxyzf2cODv_vmAIThiX5cHCVxSNjE"
SRC_SHEET_NAME   = "QA Workspace"

DST_SS_ID        = "1njy8V5lyG3vyENr1b50qGd3infU4VHYP4CfaD0H1AlM"
DST_SHEET_NAME   = "Lessons"
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

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Читаем исходный лист с retry
    sh_src = api_retry(client.open_by_key, SRC_SS_ID)
    ws_src = api_retry(sh_src.worksheet, SRC_SHEET_NAME)
    rows   = api_retry(ws_src.get_all_values)

    # 3) Собираем строки сразу после каждой A="Tutor"
    out = []
    for idx, row in enumerate(rows):
        if idx + 1 < len(rows) and row[0] == "Tutor":
            out.append(rows[idx + 1])

    if not out:
        logging.info("No rows after 'Tutor', nothing to write.")
        return

    # 4) Формируем DataFrame (шапка из первой строки источника)
    header = rows[0]
    df     = pd.DataFrame(out, columns=header)

    # 5) Очищаем диапазон A2:Q (только до конца старых данных) и записываем новые
    sh_dst = api_retry(client.open_by_key, DST_SS_ID)
    ws_dst = api_retry(sh_dst.worksheet, DST_SHEET_NAME)
    
    # сколько сейчас строк занято в A2:Q
    existing = api_retry(ws_dst.get, "A2:Q")
    end_row = 1 + len(existing)  # A2…A{end_row}
    
    if end_row >= 2:
        api_retry(ws_dst.batch_clear, [f"A2:Q{end_row}"])
    
    api_retry(
        set_with_dataframe,
        ws_dst,
        df,
        row=2,
        col=1,
        include_index=False,
        include_column_header=False,
        resize=False   # ← чтобы не чистились столбцы справа
    )

    logging.info(f"✔ Written {len(df)} rows to '{DST_SHEET_NAME}' starting at A2")

if __name__ == "__main__":
    main()
