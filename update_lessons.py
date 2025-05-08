#!/usr/bin/env python3
import os
import json
import logging

import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import APIError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ——— Жёстко прописанные константы ———
SRC_SS_ID        = "1gk6AV3sKtrMVG8Oxyzf2cODv_vmAIThiX5cHCVxSNjE"
SRC_SHEET_NAME   = "QA Workspace"

DST_SS_ID        = "1njy8V5lyG3vyENr1b50qGd3infU4VHYP4CfaD0H1AlM"
DST_SHEET_NAME   = "Lessons"
# ————————————————————————————————

SERVICE_ACCOUNT_JSON = json.loads(os.environ["GCP_SERVICE_ACCOUNT"])

def main():
    # 1) Авторизация
    scope  = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(SERVICE_ACCOUNT_JSON, scope)
    client = gspread.authorize(creds)
    logging.info("✔ Authenticated to Google Sheets")

    # 2) Читаем исходный лист
    sh_src = client.open_by_key(SRC_SS_ID)
    ws_src = sh_src.worksheet(SRC_SHEET_NAME)
    rows   = ws_src.get_all_values()

    # 3) Собираем строки сразу после каждой A="Tutor"
    out = []
    for idx, row in enumerate(rows):
        if idx + 1 < len(rows) and row[0] == "Tutor":
            out.append(rows[idx + 1])

    if not out:
        logging.info("No rows after 'Tutor', nothing to write.")
        return

    # 4) Формируем DataFrame (заголовки берём из первой строки листа)
    header = rows[0]
    df = pd.DataFrame(out, columns=header)

    # 5) Пишем в целевой лист (заменяем всё)
    sh_dst = client.open_by_key(DST_SS_ID)
    ws_dst = sh_dst.worksheet(DST_SHEET_NAME)

    ws_dst.clear()
    set_with_dataframe(ws_dst, df, include_index=False, include_column_header=True)
    logging.info(f"✔ Written {len(df)} rows to '{DST_SHEET_NAME}'")

if __name__ == "__main__":
    main()
