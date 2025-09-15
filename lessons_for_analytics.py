#!/usr/bin/env python3
import os
import json
import logging
from datetime import datetime

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
# ====================

def api_retry_open(client, key, max_attempts=5, backoff=1.0):
    for i in range(max_attempts):
        try:
            return client.open_by_key(key)
        except APIError as e:
            if getattr(e.response, "status_code", None) in (500, 502, 503, 504) and i+1 < max_attempts:
                logging.warning("open_by_key failed, retrying…")
            else:
                raise

def api_retry_worksheet(sh, title, max_attempts=5, backoff=1.0):
    for i in range(max_attempts):
        try:
            return sh.worksheet(title)
        except APIError as e:
            if getattr(e.response, "status_code", None) in (500, 502, 503, 504) and i+1 < max_attempts:
                logging.warning("worksheet failed, retrying…")
            else:
                raise
        except WorksheetNotFound:
            logging.error(f"Worksheet {title} not found")
            raise

def datetime_to_gsheet_number(dt: datetime) -> float:
    epoch = datetime(1899, 12, 30)
    delta = dt - epoch
    return delta.days + delta.seconds / 86400

def main():
    # Авторизация
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    sa_info = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(sa_info, scope)
    client = gspread.authorize(creds)

    # Source
    sh_src = api_retry_open(client, SOURCE_SS_ID)
    ws_src = api_retry_worksheet(sh_src, SOURCE_SHEET_NAME)
    rows_src = ws_src.get_all_values()
    if not rows_src:
        logging.info("Source empty, nothing to do.")
        return

    header_src = [c.strip().lower() for c in rows_src[0]]
    df_new = pd.DataFrame(rows_src[1:], columns=header_src)

    # Обработка дат/времени (опционально)
    for col in ["lesson_date", "start_date"]:
        if col in df_new.columns:
            s = pd.to_datetime(df_new[col], errors="coerce")
            df_new[col] = s.apply(lambda x: datetime_to_gsheet_number(x) if pd.notnull(x) else "")
    if "lesson_time" in df_new.columns:
        s = pd.to_datetime(df_new["lesson_time"], errors="coerce")
        df_new["lesson_time"] = s.apply(
            lambda x: (x.hour/24 + x.minute/1440 + x.second/86400) if pd.notnull(x) else ""
        )

    values = [list(df_new.columns)] + df_new.values.tolist()

    # Destination
    sh_dst = api_retry_open(client, DEST_SS_ID)
    ws_dst = api_retry_worksheet(sh_dst, DEST_SHEET_NAME)

    # Полностью очищаем и перезаписываем
    ws_dst.clear()
    ws_dst.update("A1", values)

    logging.info(f"✔ Полностью перезаписали {len(df_new)} строк (плюс заголовок)")

if __name__ == "__main__":
    main()
