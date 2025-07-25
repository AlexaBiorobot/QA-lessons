#!/usr/bin/env python3
import os
import json
import logging
import time
import io

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError, WorksheetNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Настройки
SOURCE_SS_ID = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
SOURCE_SHEET_NAME = "lessons LATAM"
DEST_SS_ID = "1XwyahhHC7uVzwfoErrvwrcruEjwewqIUp2u-6nvdSR0"
DEST_SHEET_NAME = "lesson data"

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
    # 1) Авторизация, чтение source (ваш код)
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)

    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)
    all_vals = fetch_all_values_with_retries(ws_src)
    if not all_vals or len(all_vals) < 2:
        logging.error("❌ Нет данных для импорта.")
        return

    # Заголовки и DataFrame новых данных
    headers = all_vals[0]
    df_new = pd.DataFrame(all_vals[1:], columns=headers)

    # Приведение числовых колонок к числам
    numeric_columns = [
        "timetable_id", "lesson_id", "lesson_number", "group_id",
        "course_id", "teacher_id", "students_payed",
        "new_group_duration_mins", "record_duration_mins"
    ]
    for col in numeric_columns:
        if col in df_new.columns:
            df_new[col] = pd.to_numeric(df_new[col], errors="coerce")
    
    # Приведение дат и времени к строкам, чтобы не было ошибок сериализации
    for col in ["lesson_date", "start_date"]:
        if col in df_new.columns:
            df_new[col] = pd.to_datetime(df_new[col], errors="coerce").dt.strftime("%Y-%m-%d")
    
    if "lesson_time" in df_new.columns:
        df_new["lesson_time"] = pd.to_datetime(df_new["lesson_time"], errors="coerce").dt.strftime("%H:%M")


    # 2) Читаем уже импортированные данные
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    old_vals = ws_dst.get_all_values()
    if old_vals and len(old_vals) > 1:
        df_old = pd.DataFrame(old_vals[1:], columns=old_vals[0])
    else:
        df_old = pd.DataFrame(columns=headers)

    # 3) Определяем ключ — колонка D с ID урока
    key = "lesson_id"

    # 4) Выбираем строки, которых нет в df_old
    df_new[key] = df_new[key].astype(str)       
    existing_ids = set(df_old[key].astype(str))  
    mask = ~df_new[key].isin(existing_ids)
    to_append = df_new.loc[mask]

    # 5) Добавляем только новые строки
    if not to_append.empty:
        logging.info(f"→ Добавляем {len(to_append)} новых строк")
        
        # 🔧 Убираем невалидные JSON-значения: NaN, inf, -inf
        to_append = to_append.replace([pd.NA, pd.NaT, float('inf'), float('-inf')], "").fillna("")
        
        # append_rows ожидает список списков без заголовков
        ws_dst.append_rows(
            to_append.values.tolist(),
            value_input_option="RAW"
        )
    else:
        logging.info("→ Новых строк не найдено")

    logging.info("✔ Импорт завершён")

if __name__ == "__main__":
    main()
