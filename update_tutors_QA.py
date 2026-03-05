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
from gspread.utils import rowcol_to_a1

# —————————————————————————————
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME = "Tutors"

DEST_SS_ID        = "1rS8JfkaqxQ56cEhGzKd30XR4WxIC5ZsmkIqMEfTCzRI"
DEST_SHEET_NAME   = "Tutors"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"open_by_key attempt {i}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise


def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"worksheet('{title}') attempt {i}/{max_attempts}")
            return sh.worksheet(title)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise


def fetch_columns(ws, cols_idx, max_attempts=5, backoff=1.0):
    """
    Скачиваем только нужные колонки (0-based indices) через batch_get().
    cols_idx — список индексов, напр. [0,1,2,21,4]
    """
    for attempt in range(1, max_attempts+1):
        try:
            # строим диапазоны A1:A, B1:B, C1:C, V1:V, E1:E
            ranges = []
            for idx in cols_idx:
                a1 = rowcol_to_a1(1, idx+1)                # "A1", "B1", ...
                col = ''.join(filter(str.isalpha, a1))     # "A", "B", ...
                ranges.append(f"{col}1:{col}")
            batch = ws.batch_get(ranges)
            # из batch получаем список колонок, каждая — список строк
            cols = [[row[0] if row else "" for row in col] for col in batch]
            headers = [c[0] for c in cols]
            data    = list(zip(*(c[1:] for c in cols)))
            return pd.DataFrame(data, columns=headers)
        except Exception as e:
            if attempt < max_attempts:
                logging.warning(f"batch_get error (attempt {attempt}): {e} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"batch_get failed after {attempt} attempts: {e}")
            raise


def main():
    # 1) Авторизация
    scope   = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(
                  json.loads(os.environ["GCP_SERVICE_ACCOUNT"]), scope
              )
    client  = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Открываем исходный лист
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)

    # 3) Тянем только нужные колонки
    cols_to_take = [0, 1, 2, 21, 4, 15, 16]  # A, B, C, V, E, P, Q
    df = fetch_columns(ws_src, cols_to_take)
    logging.info(f"→ Fetched columns {cols_to_take}, resulting shape={df.shape}")

    # 4) Запись в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.batch_clear(["A:G"])
    set_with_dataframe(ws_dst, df)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — {df.shape[0]} rows")


if __name__ == "__main__":
    main()
