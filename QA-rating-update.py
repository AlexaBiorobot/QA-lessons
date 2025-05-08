#!/usr/bin/env python3
import os
import json
import logging
import time

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ——— Жёстко прописанные константы ———
SRC_SS_ID        = "1gk6AV3sKtrMVG8Oxyzf2cODv_vmAIThiX5cHCVxSNjE"
SRC_SHEET_NAME   = "QA Workspace"

DST_SS_ID        = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"
DST_SHEET_NAME   = "QA - Lesson evaluation"
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
    scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
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
    df_full = pd.DataFrame(out, columns=header)

    # 5) Оставляем только колонки A, B, O, L
    #    A → index 0, B → 1, L → 11, O → 14
    df = df_full.iloc[:, [0, 1, 14, 11]]

    # 6) Записываем в целевой лист
    sh_dst = api_retry(client.open_by_key, DST_SS_ID)
    ws_dst = api_retry(sh_dst.worksheet, DST_SHEET_NAME)

    #    очищаем только A2:D (четыре колонки под наши 4 столбца)
    api_retry(ws_dst.batch_clear, ["A2:D"])
    #    заливаем начиная с A2, без заголовков
    api_retry(
        set_with_dataframe,
        ws_dst,
        df,
        row=2,
        col=1,
        include_index=False,
        include_column_header=False
    )

    logging.info(f"✔ Written {len(df)} rows to '{DST_SHEET_NAME}' starting at A2")

if __name__ == "__main__":
    main()
