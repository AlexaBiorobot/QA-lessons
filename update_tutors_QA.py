#!/usr/bin/env python3
import os
import json
import logging
import time
from typing import List

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError, WorksheetNotFound
from gspread.utils import rowcol_to_a1

# —————————————————————————————
SOURCE_SS_ID      = "1xqGCXsebSmYL4bqAwvTmD9lOentI45CTMxhea-ZDFls"
SOURCE_SHEET_NAME = "Tutors"

DEST_SS_ID        = "1rS8JfkaqxQ56cEhGzKd30XR4WxIC5ZsmkIqMEfTCzRI"
DEST_SHEET_NAME   = "Tutors"
# —————————————————————————————

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(1, max_attempts + 1):
        try:
            logging.info(f"open_by_key attempt {i}/{max_attempts}")
            return client.open_by_key(key)
        except APIError as e:
            code = getattr(e.response, "status_code", None) or getattr(e.response, "status", None)
            if code and 500 <= int(code) < 600 and i < max_attempts:
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
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
                logging.warning(f"Received {code} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            raise
        except WorksheetNotFound:
            logging.error(f"Worksheet '{title}' not found")
            raise


def dedupe_preserve_order(seq: List[int]) -> List[int]:
    seen = set()
    out = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


def fetch_columns(ws, cols_idx, max_attempts=5, backoff=1.0) -> pd.DataFrame:
    """
    Скачиваем только нужные колонки (0-based indices) через batch_get().
    Важно: паддим колонки до одинаковой длины, чтобы zip не обрезал данные.
    """
    cols_idx = dedupe_preserve_order(cols_idx)

    for attempt in range(1, max_attempts + 1):
        try:
            ranges = []
            col_letters = []

            for idx in cols_idx:
                a1 = rowcol_to_a1(1, idx + 1)             # "A1", "B1", ...
                col = ''.join(filter(str.isalpha, a1))    # "A", "B", ...
                col_letters.append(col)
                ranges.append(f"{col}1:{col}")

            batch = ws.batch_get(ranges)  # list of columns, each: [[v1],[v2],...] or []

            cols = []
            for col_vals in batch:
                flat = [(r[0] if r else "") for r in col_vals] if col_vals else []
                cols.append(flat)

            max_len = max((len(c) for c in cols), default=0)
            if max_len == 0:
                return pd.DataFrame()

            # pad to max_len
            padded_cols = []
            for c in cols:
                if len(c) < max_len:
                    c = c + [""] * (max_len - len(c))
                padded_cols.append(c)

            # headers from row 1
            headers = []
            for i, c in enumerate(padded_cols):
                h = c[0] if c else ""
                if not str(h).strip():
                    h = f"__{col_letters[i]}__"
                headers.append(h)

            data_rows = list(zip(*(c[1:] for c in padded_cols)))
            return pd.DataFrame(data_rows, columns=headers)

        except Exception as e:
            if attempt < max_attempts:
                logging.warning(f"batch_get error (attempt {attempt}): {e} — retrying in {backoff:.1f}s")
                time.sleep(backoff)
                backoff *= 2
                continue
            logging.error(f"batch_get failed after {attempt} attempts: {e}")
            raise


def main():
    # 1) Auth
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        json.loads(os.environ["GCP_SERVICE_ACCOUNT"]),
        scope,
    )
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Source
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)
    logging.info(f"Source: {sh_src.title} / {ws_src.title}")

    # 3) Columns to take (0-based) — existing + added AFTER them (BH не нужен)
    cols_to_take = [0, 1, 2, 21, 4, 15, 16]             # A, B, C, V, E, P, Q
    cols_to_take += list(range(6, 15))                  # G..O  (6..14)
    cols_to_take += [25, 31, 41, 46]                    # Z, AF, AP, AU
    cols_to_take += list(range(49, 56))                 # AX..BD (49..55)

    cols_to_take = dedupe_preserve_order(cols_to_take)

    df = fetch_columns(ws_src, cols_to_take)
    logging.info(f"→ Fetched {len(cols_to_take)} cols, df shape={df.shape}")
    if df.empty:
        raise ValueError("Fetched DataFrame is empty — check source sheet/ranges.")

    # 4) Destination
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)
    logging.info(f"Dest: {sh_dst.title} / {ws_dst.title}")

    # Определяем ширину записи (сколько колонок реально пишем в DEST)
    target_cols = int(df.shape[1])
    end_col = ''.join(filter(str.isalpha, rowcol_to_a1(1, target_cols)))  # e.g. "AK"
    logging.info(f"Will write into DEST range: A:{end_col} (only these columns will be cleared/overwritten)")

    # ⚠️ ВАЖНО: чистим ТОЛЬКО колонки, которые перезаписываем.
    # Это не тронет ничего правее end_col.
    ws_dst.batch_clear([f"A:{end_col}"])

    # Никогда НЕ уменьшаем лист по ширине/высоте — чтобы не потерять “ручные” колонки справа
    # (если нужно, расширим высоту под данные)
    needed_rows = df.shape[0] + 1  # header + data
    if ws_dst.row_count < needed_rows:
        ws_dst.resize(rows=needed_rows)

    # Пишем df начиная с A1
    set_with_dataframe(ws_dst, df, include_index=False, include_column_header=True)
    logging.info(f"✔ Written to '{DEST_SHEET_NAME}' — rows={df.shape[0]} cols={df.shape[1]}")

    # Контроль: покажем первые заголовки в DEST
    a1 = ws_dst.acell("A1").value
    b1 = ws_dst.acell("B1").value
    logging.info(f"Dest headers now: A1={a1!r}, B1={b1!r}")


if __name__ == "__main__":
    main()
