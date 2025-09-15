#!/usr/bin/env python3
import os
import json
import logging
import time
from datetime import datetime
import re
import string

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError, WorksheetNotFound

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ==== Настройки ====
SOURCE_SS_ID = "1_S-NyaVKuOc0xK12PBAYvdIauDBq9mdqHlnKLfSYNAE"
SOURCE_SHEET_NAME = "lessons LATAM"
DEST_SS_ID = "1LF2NrAm8J3c43wOoumtsyfQsX1z0_lUQVdByGSPe27U"
DEST_SHEET_NAME = "Lessons source"

# Ключ можно указать ИМЕНЕМ колонки (если есть шапка) ИЛИ буквой столбца (позиция)
KEY_COL_NAME = "lesson_id"   # опционально
KEY_COL_LETTER = "D"         # приоритетно; D = 4-й столбец (0-индекс 3)
# ====================

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

def col_letter_to_index(letter: str) -> int:
    """A->0, B->1, ..., Z->25, AA->26 ..."""
    letter = letter.strip().upper()
    res = 0
    for ch in letter:
        if ch not in string.ascii_uppercase:
            raise ValueError(f"Bad column letter: {letter}")
        res = res * 26 + (ord(ch) - ord('A') + 1)
    return res - 1

def looks_like_header(cells: list[str]) -> bool:
    if not cells:
        return False
    letterish = 0
    for v in cells:
        s = (v or "").strip()
        if not s:
            continue
        if re.search(r"[A-Za-zА-Яа-яÁ-Úá-úÜüÑñÇçŠŽšžİıĞğÖöЁё_]", s):
            letterish += 1
    return (letterish / max(1, len(cells))) >= 0.4

def align_rows_to_width(rows: list[list[str]], width: int) -> list[list[str]]:
    fixed = []
    for r in rows:
        r = list(r)
        if len(r) < width:
            r = r + [""] * (width - len(r))
        else:
            r = r[:width]
        fixed.append(r)
    return fixed

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

    # Даты/время
    for col in ["lesson_date", "start_date"]:
        if col in df_new.columns:
            s = pd.to_datetime(df_new[col], errors="coerce")
            df_new[col] = s.apply(lambda x: datetime_to_gsheet_number(x) if pd.notnull(x) else "")
    if "lesson_time" in df_new.columns:
        s = pd.to_datetime(df_new["lesson_time"], errors="coerce")
        df_new["lesson_time"] = s.apply(
            lambda x: (x.hour / 24 + x.minute / 1440 + x.second / 86400) if pd.notnull(x) else ""
        )

    # 2a) Индекс ключевого столбца по имени (если возможно)
    key_idx_from_name = None
    if KEY_COL_NAME and KEY_COL_NAME.lower() in df_new.columns:
        key_idx_from_name = df_new.columns.get_loc(KEY_COL_NAME.lower())

    # 3) Destination
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    old_vals = ws_dst.get_all_values()

    # Определяем ширину таблицы по source
    src_width = len(df_new.columns)
    if not src_width:
        logging.info("Source is empty — nothing to do.")
        return

    if not old_vals:
        # Пустой лист — используем ширину source
        dest_has_header = False
        dest_header = []
        rows_data = []
        dest_width = src_width
        logging.info("Destination is empty.")
    else:
        first_row = [ (c or "").replace("\ufeff","").strip() for c in old_vals[0] ]
        dest_has_header = looks_like_header(first_row)
        dest_header = first_row if dest_has_header else []
        rows_data = old_vals[1:] if dest_has_header else old_vals
        dest_width = max(src_width, len(first_row)) if dest_has_header else max(src_width, max((len(r) for r in old_vals), default=src_width))

    # Выравниваем строки по ширине
    rows_data = align_rows_to_width(rows_data, dest_width)

    # 3a) Определяем индекс ключа в destination: приоритет — буква столбца
    key_idx = col_letter_to_index(KEY_COL_LETTER) if KEY_COL_LETTER else None

    # Если шапка есть и имя ключа совпало — можно переопределить по имени (на случай иных порядков)
    if dest_has_header and KEY_COL_NAME:
        norm_header = [h.lower() for h in dest_header]
        if KEY_COL_NAME.lower() in norm_header:
            key_idx = norm_header.index(KEY_COL_NAME.lower())

    # Если по букве не получилось (например, пусто), но есть индекс из source по имени — используем его
    if key_idx is None and key_idx_from_name is not None:
        key_idx = key_idx_from_name

    # Если всё равно None — fallback на букву D
    if key_idx is None:
        key_idx = col_letter_to_index("D")

    # Контроль диапазона
    if key_idx < 0 or key_idx >= dest_width:
        raise IndexError(f"KEY column index out of range: {key_idx} for width {dest_width}")

    # 4) Строим df_old из данных (без зависимости от заголовков)
    if rows_data:
        df_old = pd.DataFrame(rows_data)
    else:
        df_old = pd.DataFrame(columns=list(range(dest_width)))

    # 5) Множество уже существующих ключей (как строки)
    if not df_old.empty:
        old_keys = set(
            to_key_series(df_old.iloc[:, key_idx]).loc[lambda x: x != ""]
        )
    else:
        old_keys = set()

    # 6) Ключи в source: если есть имя — берём по имени, иначе по индексу key_idx_from_name (который вычислили выше)
    if KEY_COL_NAME and KEY_COL_NAME.lower() in df_new.columns:
        new_key_series = to_key_series(df_new[KEY_COL_NAME.lower()])
    elif key_idx_from_name is not None and key_idx_from_name < len(df_new.columns):
        new_key_series = to_key_series(df_new.iloc[:, key_idx_from_name])
    else:
        # Если даже в source нет столбца по имени — критическая ситуация
        raise KeyError(f"В source нет ключевого столбца '{KEY_COL_NAME}', а позиция по имени определить не удалось")

    mask_new = ~new_key_series.isin(old_keys)
    to_append = df_new.loc[mask_new].copy()

    # 7) Перед записью — просто отдаём значения как есть (позиция столбцов = порядок в таблице)
    # Если хочешь жёстко выровнять под ширину destination, можно дорезать/дополнить:
    values = to_append.values.tolist()
    values = align_rows_to_width(values, dest_width)

    logging.info("Новых строк к добавлению: %d", len(values))

    if values:
        ws_dst.append_rows(values, value_input_option="RAW")
        logging.info("✔ Добавлено строк: %d", len(values))
    else:
        l
