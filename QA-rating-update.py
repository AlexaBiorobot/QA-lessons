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
# Источник: https://docs.google.com/spreadsheets/d/1njy8V5lyG3vyENr1b50qGd3infU4VHYP4CfaD0H1AlM/
SRC_SS_ID      = "1njy8V5lyG3vyENr1b50qGd3infU4VHYP4CfaD0H1AlM"
SRC_SHEET_NAME = "Lessons"

# Целевая таблица
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
                logging.warning(f"Ошибка API {code}, попытка {attempt}. Повтор через {backoff}с...")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise

def _extract_rows_after_tutor(rows):
    """
    Логика: ищем строку, где в колонке A написано 'Tutor'.
    Берем СЛЕДУЮЩУЮ за ней строку и вытягиваем индексы:
    A(0), B(1), O(14), L(11)
    """
    out = []
    width = 15 # Индекс 14 (O) требует как минимум 15 колонок
    
    for i, r in enumerate(rows):
        if not r: continue
        
        # Проверяем, что в текущей строке в колонке A стоит "Tutor"
        if str(r[0]).strip() == "Tutor":
            if i + 1 < len(rows):
                nxt = rows[i + 1]
                # Дополняем строку пустыми ячейками, если она слишком короткая
                if len(nxt) < width:
                    nxt += [""] * (width - len(nxt))
                
                # Собираем данные в порядке A, B, O, L
                # Индексы: 0=A, 1=B, 14=O, 11=L
                filtered_row = [nxt[0], nxt[1], nxt[14], nxt[11]]
                out.append(filtered_row)
    return out

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизация успешна")

    # 2) Чтение источника
    sh_src = api_retry(client.open_by_key, SRC_SS_ID)
    try:
        ws_src = api_retry(sh_src.worksheet, SRC_SHEET_NAME)
    except WorksheetNotFound:
        logging.error(f"Лист '{SRC_SHEET_NAME}' не найден в источнике!")
        return

    # Загружаем всё до колонки O
    rows = api_retry(ws_src.get, "A:O")
    if not rows:
        logging.info("Таблица пуста.")
        return

    picked_data = _extract_rows_after_tutor(rows)
    if not picked_data:
        logging.info("Не найдено строк для переноса (после меток 'Tutor').")
        return

    # Создаем DataFrame (используем технические имена колонок для стабильности)
    df = pd.DataFrame(picked_data, columns=['col_A', 'col_B', 'col_O', 'col_L'])
    logging.info(f"✔ Собрано строк: {len(df)}")

    # 3) Запись в цель
    sh_dst = api_retry(client.open_by_key, DST_SS_ID)
    ws_dst = api_retry(sh_dst.worksheet, DST_SHEET_NAME)

    # Очищаем старые данные в A2:D (чтобы не осталось "хвостов")
    existing = api_retry(ws_dst.get, "A2:D")
    if existing:
        end_row = 1 + len(existing)
        api_retry(ws_dst.batch_clear, [f"A2:D{max(2, end_row)}"])

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

    logging.info(f"✔ Данные успешно обновлены в '{DST_SHEET_NAME}' (A2:D)")

if __name__ == "__main__":
    main()
