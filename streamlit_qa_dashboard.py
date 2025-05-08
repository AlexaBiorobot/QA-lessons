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
from googleapiclient.errors import HttpError

# —————————————————————————————
# Ваши константы
SOURCE_SS_ID        = "1gV9STzFPKMeIkVO6MFILzC-v2O6cO3XZyi4sSstgd8A"
SOURCE_SHEET_NAME   = "All lesson reviews"
DEST_SS_ID          = "16QrbLtzLTV6GqyT8HYwzcwYIsXewzjUbM0Jy5i1fENE"
DEST_SHEET_NAME     = "QA - Lesson evaluation"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def get_http_code(exc):
    """
    Универсально извлекает HTTP-код из APIError или HttpError.
    """
    # gspread.exceptions.APIError
    if hasattr(exc, "response") and hasattr(exc.response, "status_code"):
        return exc.response.status_code
    # googleapiclient.errors.HttpError
    if hasattr(exc, "status"):
        return exc.status
    if hasattr(exc, "resp") and hasattr(exc.resp, "status"):
        return exc.resp.status
    return None


def fetch_with_retries(ws, max_attempts=8, initial_backoff=1.0):
    """
    Читает весь лист через get_all_values() с retry при ошибках 5xx.
    """
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            logging.info(f"Попытка #{attempt} чтения листа…")
            return ws.get_all_values()
        except (APIError, HttpError) as e:
            code = get_http_code(e)
            if code and 500 <= int(code) < 600 and attempt < max_attempts:
                logging.warning(f"Получили {code}, ждем {backoff:.1f}s и повторяем…")
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"Ошибка при чтении листа (код={code}): {e}")
            raise
    raise RuntimeError("Не удалось получить данные после нескольких попыток")


def api_retry(func, *args, max_attempts=5, initial_backoff=1.0, **kwargs):
    """
    Общий retry для любых API-вызовов, ловим ошибки 5xx.
    """
    backoff = initial_backoff
    for attempt in range(1, max_attempts + 1):
        try:
            return func(*args, **kwargs)
        except (APIError, HttpError) as e:
            code = get_http_code(e)
            if code and 500 <= int(code) < 600 and attempt < max_attempts:
                logging.warning(f"API {code} на попытке {attempt}, retry через {backoff}s…")
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"APIError на попытке {attempt} (код={code}): {e}")
            raise
    raise RuntimeError("API вызовы завершились неудачно после retries")


def main():
    # 1) Авторизация
    scope   = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds   = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client  = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Считываем исходный лист с retry
    sh_src   = client.open_by_key(SOURCE_SS_ID)
    ws_src   = sh_src.worksheet(SOURCE_SHEET_NAME)
    all_vals = fetch_with_retries(ws_src)

    if not all_vals or len(all_vals) < 2:
        logging.error("Исходный лист пуст или нет данных")
        return

    # 3) Формируем DataFrame и выбираем колонки C, D, O, M, F
    df_src = pd.DataFrame(all_vals[1:], columns=all_vals[0])
    df      = df_src.iloc[:, [2, 3, 14, 12, 5]]
    logging.info(f"→ Оставили колонки C,D,O,M,F: {df.shape[0]} строк")

    # 4) Записываем в целевой лист — очищаем A:E и вставляем df туда с retry
    sh_dst = client.open_by_key(DEST_SS_ID)
    ws_dst = sh_dst.worksheet(DEST_SHEET_NAME)

    api_retry(ws_dst.batch_clear, ["A:E"])
    api_retry(
        set_with_dataframe,
        ws_dst,
        df,
        row=1,
        col=1,
        include_index=False,
        include_column_header=True
    )
    logging.info(f"✔ Данные записаны в «{DEST_SHEET_NAME}» — {df.shape[0]} строк")


if __name__ == "__main__":
    main()
