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

# ——— КОНСТАНТЫ ———
SRC_SS_ID      = "1njy8V5lyG3vyENr1b50qGd3infU4VHYP4CfaD0H1AlM"
SRC_SHEET_NAME = "Lessons"

DST_SS_ID      = "1HItT2-PtZWoldYKL210hCQOLg3rh6U1Qj6NWkBjDjzk"
DST_SHEET_NAME = "QA - Lesson evaluation"
# —————————————————

SERVICE_ACCOUNT_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])

def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            code = None
            if e.response:
                code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and attempt < max_attempts:
                logging.warning(f"Ошибка API {code}, повтор через {backoff}с...")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

def _extract_data(rows):
    extracted = []
    # Индекс 14 соответствует колонке O
    target_width = 15 
    
    for i, row in enumerate(rows):
        if not row: continue
        
        # Поиск ключевого слова "Tutor" в первой колонке (индекс 0)
        if str(row[0]).strip().lower() == "tutor":
            if i + 1 < len(rows):
                data_row = rows[i + 1]
                # Убеждаемся, что строка содержит достаточно колонок
                if len(data_row) < target_width:
                    data_row += [""] * (target_width - len(data_row))
                
                # Формируем итоговую строку для колонок A, B, C, D целевой таблицы
                # Источники: A(0), B(1), O(14), L(11)
                extracted_row = [
                    data_row[0],  # Идёт в A (Tutor)
                    data_row[1],  # Идёт в B (Student)
                    data_row[14], # Идёт в C (Mark)
                    data_row[11]  # Идёт в D (Lesson)
                ]
                extracted.append(extracted_row)
    return extracted

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизация выполнена")

    # 2) Получение данных из источника
    sh_src = api_retry(client.open_by_key, SRC_SS_ID)
    ws_src = sh_src.worksheet(SRC_SHEET_NAME)
    all_rows = api_retry(ws_src.get_all_values)
    
    logging.info(f"✔ Считано {len(all_rows)} строк из источника")
    clean_data = _extract_data(all_rows)
    
    if not clean_data:
        logging.warning("⚠️ Данные по фильтру 'Tutor' не найдены.")
        return
    
    df = pd.DataFrame(clean_data)

    # 3) Подготовка целевой таблицы
    sh_dst = api_retry(client.open_by_key, DST_SS_ID)
    ws_dst = sh_dst.worksheet(DST_SHEET_NAME)

    # ПОЛНАЯ ПЕРЕЗАПИСЬ КОЛОНОК A, B, C, D
    # Очищаем диапазон A2:D до максимально возможной строки (50000)
    # Это гарантирует удаление всех старых данных в этих четырех колонках.
    api_retry(ws_dst.batch_clear, ["A2:D50000"])
    logging.info("✔ Колонки A2:D50000 полностью очищены")

    # 4) Запись новых данных
    api_retry(
        set_with_dataframe,
        ws_dst,
        df,
        row=2,
        col=1,
        include_index=False,
        include_column_header=False,
        resize=False
    )

    logging.info(f"✔ Колонки A, B, C, D успешно перезаписаны ({len(df)} строк)")

if __name__ == "__main__":
    main()
