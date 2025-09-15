#!/usr/bin/env python3
import os
import json
import logging
import time
from datetime import datetime
import re

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError, WorksheetNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# Настройки
SOURCE_SS_ID = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
SOURCE_SHEET_NAME = "lessons LATAM"
DEST_SS_ID = "1LF2NrAm8J3c43wOoumtsyfQsX1z0_lUQVdByGSPe27U"
DEST_SHEET_NAME = "Lessons source"

KEY_COL = "lesson_id"  # ключ для дедупликации

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

def normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        (c if c is not None else "")
        .replace("\ufeff", "")
        .strip()
        .lower()
        for c in df.columns
    ]
    return df

def datetime_to_gsheet_number(dt: datetime) -> float:
    epoch = datetime(1899, 12, 30)
    delta = dt - epoch
    return delta.days + delta.seconds / 86400

def to_key_series(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
         .str.replace("\ufeff", "", regex=False)
         .str.strip()
         .replace({"nan": "", "None": ""})
    )

def looks_like_header(cells: list[str]) -> bool:
    """Эвристика: шапка — если >=40% ячеек содержат буквы/подчёркивание, а не только цифры/даты/время."""
    if not cells:
        return False
    letterish = 0
    for v in cells:
        s = (v or "").strip()
        if not s:
            continue
        # если есть буквы любого алфавита или подчёркивание — считаем «похоже на заголовок»
        if re.search(r"[A-Za-zА-Яа-яÁ-Úá-úÜüÑñÇçŠŽšžİıĞğİİÖöЁё_]", s):
            letterish += 1
    return (letterish / max(1, len(cells))) >= 0.4

def align_table_to_columns(rows: list[list[str]], columns: list[str]) -> pd.DataFrame:
    """Строит DataFrame из сырого массива rows, приводя ширину к числу колонок."""
    width = len(columns)
    fixed = []
    for r in rows:
        r = list(r)
        if len(r) < width:
            r = r + [""] * (width - len(r))
        else:
            r = r[:width]
        fixed.append(r)
    return pd.DataFrame(fixed, columns=columns)

def main():
    # 1) Авторизация
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)

    # 2) Source
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)
    records = ws_src.get_all_records()
    df_new = pd.DataFrame(records)
    df_new = normalize_cols(df_new)

    logging.info("Source columns (normalized): %s", df_new.columns.tolist())

    # Даты и время
    for col in ["lesson_date", "start_date"]:
        if col in df_new.columns:
            s = pd.to_datetime(df_new[col], errors="coerce")
            df_new[col] = s.apply(lambda x: datetime_to_gsheet_number(x) if pd.notnull(x) else "")
    if "lesson_time" in df_new.columns:
        s = pd.to_datetime(df_new["lesson_time"], errors="coerce")
        df_new["lesson_time"] = s.apply(
            lambda x: (x.hour / 24 + x.minute / 1440 + x.second / 86400) if pd.notnull(x) else ""
        )

    # 3) Destination
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    old_vals = ws_dst.get_all_values()

    # Определяем, есть ли шапка
    if old_vals and len(old_vals) > 0:
        raw_header = [ (h or "").replace("\ufeff","").strip() for h in old_vals[0] ]
        has_header = looks_like_header(raw_header)
    else:
        raw_header = []
        has_header = False

    if not old_vals:
        # Лист пуст
        df_old = pd.DataFrame(columns=df_new.columns)
        dest_columns = list(df_new.columns)
        logging.info("Destination is empty, will use source columns.")
    elif has_header:
        # Нормальная шапка
        df_old = pd.DataFrame(old_vals[1:], columns=[h.lower() for h in raw_header])
        df_old = normalize_cols(df_old)
        dest_columns = [c.replace("\ufeff","").strip().lower() for c in raw_header]
        logging.info("Destination columns (normalized): %s", dest_columns)
    else:
        # В первой строке данные (как у тебя в логе). Работаем без явной шапки.
        dest_columns = list(df_new.columns)
        df_old = align_table_to_columns(old_vals, dest_columns)
        df_old = normalize_cols(df_old)
        logging.info("Destination has no header; treating first row as data. Using source columns: %s", dest_columns)

        # ❗ Если хочешь один раз записать шапку в A1 — раскомментируй:
        # ws_dst.update('A1', [dest_columns])

    # 4) Проверки ключа
    if KEY_COL not in df_new.columns:
        logging.error("В source нет столбца '%s'. Колонки: %s", KEY_COL, df_new.columns.tolist())
        raise KeyError(f"'{KEY_COL}' отсутствует в source (лист '{SOURCE_SHEET_NAME}')")

    # В df_old ключ может отсутствовать только если лист пуст — это ок
    if (len(df_old.columns) > 0) and (KEY_COL not in df_old.columns):
        # Если сюда попали — это действительно рассинхрон порядка столбцов.
        logging.error("В destination не удаётся сопоставить столбец '%s'. Колонки: %s", KEY_COL, df_old.columns.tolist())
        raise KeyError(f"'{KEY_COL}' отсутствует в destination (лист '{DEST_SHEET_NAME}') и не удалось вывести порядок столбцов")

    # 5) Дедуп по ключу как строке
    old_keys = set(to_key_series(df_old[KEY_COL]).loc[lambda x: x != ""]) if KEY_COL in df_old.columns else set()
    new_keys_series = to_key_series(df_new[KEY_COL])

    mask_new = ~new_keys_series.isin(old_keys)
    to_append = df_new.loc[mask_new].copy()

    # 6) Выровнять порядок колонок под destination
    to_append = to_append.reindex(columns=dest_columns, fill_value="")

    # 7) Чистка и лог
    to_append = to_append.replace([float('inf'), float('-inf')], pd.NA).fillna("")
    logging.info("Новых строк к добавлению: %d", len(to_append))

    # 8) Запись
    if not to_append.empty:
        ws_dst.append_rows(
            to_append.values.tolist(),
            value_input_option="RAW"
        )
        logging.info("✔ Добавлено строк: %d", len(to_append))
    else:
        logging.info("→ Новых строк не найдено")

    logging.info("✔ Импорт завершён")

if __name__ == "__main__":
    main()
