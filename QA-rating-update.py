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

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизация выполнена")

    # 2) Получение всех данных из источника (Лист Lessons)
    sh_src = api_retry(client.open_by_key, SRC_SS_ID)
    ws_src = sh_src.worksheet(SRC_SHEET_NAME)
    
    # Забираем всё содержимое листа (включая пустые строки)
    all_rows = api_retry(ws_src.get_all_values)
    if not all_rows:
        logging.warning("Источник пуст.")
        return

    # 3) Выбор колонок A(0), B(1), O(14), L(11)
    # Начинаем со 2-й строки (индекс [1:]), чтобы не тащить старые заголовки
    extracted_data = []
    for row in all_rows[1:]:
        # Добиваем строку до индекса 14, если она короче
        if len(row) < 15:
            row += [""] * (15 - len(row))
        
        # Берем данные по индексам
        extracted_data.append([
            row[0],  # A -> Tutor
            row[1],  # B -> Student
            row[14], # O -> Mark
            row[11]  # L -> Lesson
        ])

    df = pd.DataFrame(extracted_data)
    logging.info(f"✔ Подготовлено {len(df)} строк для переноса")

    # 4) Полная перезапись целевой таблицы (только колонки A-D)
    sh_dst = api_retry(client.open_by_key, DST_SS_ID)
    ws_dst = sh_dst.worksheet(DST_SHEET_NAME)

    # Очищаем колонки A, B, C, D полностью (до 50к строки)
    api_retry(ws_dst.batch_clear, ["A2:D50000"])
    logging.info("✔ Целевой диапазон A2:D50000 очищен")

    # Записываем новые данные начиная с A2
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

    logging.info(f"✔ Данные в колонках A, B, C, D успешно обновлены")

if __name__ == "__main__":
    main()
