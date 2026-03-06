#!/usr/bin/env python3
import os
import json
import logging
import time
import io

import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1
from requests.exceptions import RequestException, ReadTimeout

# —————————————————————————————
SOURCE_SS_ID      = "1gV9STzFPKMeIkVO6MFILzC-v2O6cO3XZyi4sSstgd8A"
SOURCE_SHEET_NAME = "All lesson reviews OLD"
DEST_SS_ID        = "1rS8JfkaqxQ56cEhGzKd30XR4WxIC5ZsmkIqMEfTCzRI"
DEST_SHEET_NAME   = "QA - Lesson evaluation"
# —————————————————————————————

# NEW — ключи дедупликации/приоритеты
# None => дубликат = полное совпадение по всем колонкам
DEDUPE_SUBSET = None
DEDUPE_KEEP = "first"  # при конфликте оставляем первую
SOURCE_PRIORITY = {"GRAD": 0, "ARCH": 1, "OLD": 2}  # GRAD > ARCH > OLD

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


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


def fetch_csv_with_retries(url: str, max_attempts=5, backoff=1.0) -> bytes:
    delay = backoff
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"CSV fetch attempt {i}/{max_attempts}")
            r = requests.get(url, timeout=(10, 120))
            if r.status_code >= 500:
                raise RequestException(f"{r.status_code} Server Error")
            r.raise_for_status()
            return r.content
        except (RequestException, ReadTimeout) as e:
            if i == max_attempts:
                logging.error(f"CSV fetch failed after {i} attempts: {e}")
                raise
            logging.warning(f"CSV fetch error ({e}), retrying in {delay:.1f}s…")
            time.sleep(delay); delay *= 2


def fetch_all_values_with_retries(ws, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts+1):
        try:
            logging.info(f"get_all_values() attempt {i}/{max_attempts}")
            return ws.get_all_values()
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attemptments:
                logging.warning(f"get_all_values got {code}, retrying in {backoff:.1f}s")
                time.sleep(backoff); backoff *= 2
                continue
            logging.error(f"get_all_values failed: {e}")
            raise


def fetch_columns(ws, cols_idx, max_attempts=3, backoff=1.0):
    """
    Пытаемся batch_get нужных колонок cols_idx (0-based).
    Если и он всё равно падает, выйдем с APIError и дадим main() обработать.
    """
    backoff_local = backoff
    for i in range(1, max_attempts+1):
        try:
            ranges = []
            for idx in cols_idx:
                a1 = rowcol_to_a1(1, idx+1)
                col = ''.join(filter(str.isalpha, a1))
                ranges.append(f"{col}1:{col}")
            logging.info(f"batch_get ranges {ranges} (attempt {i}/{max_attempts})")
            batch = ws.batch_get(ranges)
            cols = [[r[0] if r else "" for r in colblock] for colblock in batch]
            headers = [c[0] for c in cols]
            data = list(zip(*(c[1:] for c in cols)))
            return pd.DataFrame(data, columns=headers)
        except APIError as e:
            if i < max_attempts:
                logging.warning(f"batch_get got {e}, retrying in {backoff_local:.1f}s")
                time.sleep(backoff_local); backoff_local *= 2
                continue
            logging.error(f"batch_get failed after {i} attempts: {e}")
            raise


def get_selected_columns_from_sheet(client, ss_id, sheet_name, cols_to_take):
    sh = api_retry_open(client, ss_id)
    ws = api_retry_worksheet(sh, sheet_name)
    try:
        df = fetch_columns(ws, cols_to_take)
        logging.info(f"→ batch_get succeeded for {sheet_name}, shape={df.shape}")
    except APIError:
        logging.warning("batch_get не прошел, пробуем CSV-экспорт…")
        gid = ws.id
        creds = client.auth
        token = creds.get_access_token().access_token
        export_url = (
            f"https://docs.google.com/spreadsheets/d/{ss_id}/export"
            f"?format=csv&gid={gid}&access_token={token}"
        )
        try:
            csv_bytes = fetch_csv_with_retries(export_url)
            df_all = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
            logging.info(f"→ CSV-экспорт удался, shape={df_all.shape}")
        except Exception as e:
            logging.warning(f"CSV-экспорт упал ({e}), пробуем get_all_values()…")
            all_vals = fetch_all_values_with_retries(ws)
            if not all_vals or len(all_vals) < 2:
                logging.error("Нет данных ни одним способом – выхожу.")
                return None
            df_all = pd.DataFrame(all_vals[1:], columns=all_vals[0])
            logging.info(f"→ get_all_values() удался, shape={df_all.shape}")
        df = df_all.iloc[:, cols_to_take]
        logging.info(f"→ После fallback-выборки shape={df.shape}")
    return df


def main():
    # 1) Авторизация
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Авторизованы в Google Sheets")

    # 2) Тянем данные из первого источника
    cols_to_take_1 = [2, 3, 14, 12, 5]  # C, D, O, M, F
    df1 = get_selected_columns_from_sheet(client, SOURCE_SS_ID, SOURCE_SHEET_NAME, cols_to_take_1)

    # 3) Тянем данные из второго источника
    SOURCE2_SS_ID      = "1R8GzRVL58XxheG0FRtSRfE6Ib5E_GcZh1Ws_iaDOpbk"
    SOURCE2_SHEET_NAME = "QA Workspace Archive"
    cols_to_take_2 = [0, 1, 12, 10, 3]  # A, B, M, K, D
    df2 = get_selected_columns_from_sheet(client, SOURCE2_SS_ID, SOURCE2_SHEET_NAME, cols_to_take_2)

    # 3a) Тянем данные из третьего источника (новый лист!)
    SOURCE3_SS_ID      = "1R8GzRVL58XxheG0FRtSRfE6Ib5E_GcZh1Ws_iaDOpbk"
    SOURCE3_SHEET_NAME = "QA Workspace Graduation Archive"
    cols_to_take_3 = [0, 1, 12, 11, 3]  # A, B, M, L, D
    df3 = get_selected_columns_from_sheet(client, SOURCE3_SS_ID, SOURCE3_SHEET_NAME, cols_to_take_3)

    # NEW — приводим названия колонок и помечаем источник (для приоритета при дедупе)
    TARGET_COLUMNS = list(df1.columns) if df1 is not None else ['Col1', 'Col2', 'Col3', 'Col4', 'Col5']
    if df2 is not None:
        df2.columns = TARGET_COLUMNS
    if df3 is not None:
        df3.columns = TARGET_COLUMNS

    if df1 is not None:
        df1["_src"] = "OLD"
    if df2 is not None:
        df2["_src"] = "ARCH"
    if df3 is not None:
        df3["_src"] = "GRAD"

    # NEW — лёгкая нормализация строк (убираем лишние пробелы, чтобы не плодили псевдодубли)
    for d in (df1, df2, df3):
        if d is not None:
            obj_cols = d.select_dtypes(include="object").columns
            d[obj_cols] = d[obj_cols].apply(lambda s: s.str.strip())

    # 4) Объединяем (как раньше, но с проверками)
    if all(x is None for x in [df1, df2, df3]):
        logging.error("❌ Не удалось получить новые данные ни из одного источника. Старая таблица останется без изменений.")
        return

    dfs = [d for d in [df1, df2, df3] if d is not None and not d.empty]
    if not dfs:
        logging.error("❌ Нет данных для записи.")
        return

    df = pd.concat(dfs, ignore_index=True)

    # NEW — сортируем по приоритету источника → при drop_duplicates останется «лучшая» версия
    df["_prio"] = df["_src"].map(SOURCE_PRIORITY).fillna(9)
    df = df.sort_values("_prio", kind="stable")

    # NEW — дедупликация
    subset = TARGET_COLUMNS if DEDUPE_SUBSET is None else DEDUPE_SUBSET
    before = len(df)
    df = df.drop_duplicates(subset=subset, keep=DEDUPE_KEEP, ignore_index=True)
    after = len(df)
    logging.info(f"✔ Дедупликация: {before} → {after} строк (ключ: {subset})")

    # NEW — чистим служебные колонки и порядок столбцов
    df = df.drop(columns=["_src", "_prio"], errors="ignore")
    df = df[TARGET_COLUMNS]

    # 5) Запись в целевой лист (как было)
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    ws_dst.batch_clear(["A2:E"])
    set_with_dataframe(ws_dst, df, row=2, col=1, include_index=False, include_column_header=False)
    logging.info(f"✔ Данные записаны в «{DEST_SHEET_NAME}» — {df.shape[0]} строк")


if __name__ == "__main__":
    main()
