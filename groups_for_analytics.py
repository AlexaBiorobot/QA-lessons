#!/usr/bin/env python3
import os
import json
import logging
import time
import io

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Настройки
SOURCE_SS_ID = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
SOURCE_SHEET_NAME = "groups"
DEST_SS_ID = "1yJmskKLGinBNKIV3ewXsVEfnh-JRj_FhuKyElL93vM4"
DEST_SHEET_NAME = "group data"

def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"open_by_key attempt {i}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"open_by_key got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
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
                logging.warning(f"worksheet got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise

def fetch_all_values_with_retries(ws, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"get_all_values() attempt {i}/{max_attempts}")
            return ws.get_all_values()
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"get_all_values got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            logging.error(f"get_all_values failed: {e}")
            raise

def main():
    # Авторизация
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # Открываем исходный лист
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)

    # Грузим все значения
    all_vals = fetch_all_values_with_retries(ws_src)
    if not all_vals or len(all_vals) < 2:
        logging.error("❌ Нет данных для импорта.")
        return

    # В датафрейм
    df = pd.DataFrame(all_vals[1:], columns=all_vals[0])

    # Фильтруем по B
    values_to_keep = ["COL", "ESP", "CHI"]
    mask = df[df.columns[1]].str.contains('|'.join(values_to_keep), na=False)
    filtered_df = df[mask].reset_index(drop=True)
    logging.info(f"→ Получено строк после фильтрации: {filtered_df.shape[0]}")

    # Записываем в целевой лист
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.clear()  # Полностью очищаем лист
    set_with_dataframe(ws_dst, filtered_df, row=1, col=1,
                       include_index=False, include_column_header=True)
    logging.info(f"✔ Данные записаны в «{DEST_SHEET_NAME}» — {filtered_df.shape[0]} строк")

if __name__ == "__main__":
    main()
