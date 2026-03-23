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

# Источник
SRC_SS_ID = "1gV9STzFPKMeIkVO6MFILzC-v2O6cO3XZyi4sSstgd8A"
SRC_SHEET_NAME = "All lesson reviews NEW"

# Куда пишем
DEST_SS_ID = "1yJmskKLGinBNKIV3ewXsVEfnh-JRj_FhuKyElL93vM4"
DEST_SHEET_NAME = "data"


def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
        try:
            logging.info(f"open_by_key attempt {i}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"open_by_key got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise


def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
        try:
            logging.info(f"worksheet('{title}') attempt {i}/{max_attempts}")
            return sh.worksheet(title)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"worksheet got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise


def fetch_all_values_with_retries(ws, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
        try:
            logging.info(f"get_all_values() attempt {i}/{max_attempts}")
            return ws.get_all_values()
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"get_all_values got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"get_all_values failed: {e}")
            raise


def get_all_columns(client, ss_id, sheet_name):
    sh = api_retry_open(client, ss_id)
    ws = api_retry_worksheet(sh, sheet_name)
    all_vals = fetch_all_values_with_retries(ws)

    if not all_vals or len(all_vals) < 2:
        logging.error(f"Нет данных в листе {sheet_name}")
        return None

    df = pd.DataFrame(all_vals[1:], columns=all_vals[0])
    logging.info(f"→ Получены все колонки из {sheet_name}, shape={df.shape}")
    return df


def main():
    # Авторизация
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # Получаем данные только из одного источника
    df = get_all_columns(client, SRC_SS_ID, SRC_SHEET_NAME)
    if df is None or df.empty:
        logging.error("❌ Нет данных для записи.")
        return

    # Запись в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)

    ws_dst.clear()
    set_with_dataframe(
        ws_dst,
        df,
        row=1,
        col=1,
        include_index=False,
        include_column_header=True
    )

    logging.info(f"✔ Данные записаны в «{DEST_SHEET_NAME}» — {df.shape[0]} строк, {df.shape[1]} колонок")


if __name__ == "__main__":
    main()
