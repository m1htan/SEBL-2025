import os
import re

import numpy as np
import pandas as pd

DATA_RAW_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/data_raw/FL549/Dataset SEBL_FL549"
os.makedirs(DATA_RAW_DIR, exist_ok=True)

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/data_clean/volume_A"
os.makedirs(OUTPUT_DIR, exist_ok=True)

raw_volume_A = os.path.join(DATA_RAW_DIR, 'SMEs resource efficiency green markets_fl_549_volume_A.xlsx')
xls = pd.ExcelFile(raw_volume_A)

# Danh sách sheet cần loại trừ
excluded_keywords = ["note", "table of contents"]

# Lặp từng sheet
for sheet_name in xls.sheet_names:
    sheet_name_clean = sheet_name.strip().lower()

    # Kiểm tra nếu sheet nằm trong danh sách loại trừ hoặc chứa "(a)"
    if any(keyword in sheet_name_clean for keyword in excluded_keywords) or "(a)" in sheet_name_clean:
        print(f"[SKIP] Bỏ qua sheet: {sheet_name}")
        continue

    try:
        # Đọc sheet, bỏ 10 dòng đầu
        df = pd.read_excel(raw_volume_A, sheet_name=sheet_name, skiprows=10)

        # Xoá dòng và cột rỗng hoàn toàn (nếu có)
        df.dropna(how='all', inplace=True)
        df.dropna(axis=1, how='all', inplace=True)

        # Chuẩn hoá tên cột
        df.columns = [str(col).strip() for col in df.columns]

        question_code_pattern = r'\([A-Z]{2}\)'
        df = df[~df.apply(lambda row: row.astype(str).str.contains(question_code_pattern).any(), axis=1)]

        df.columns = df.iloc[0]
        df.columns.values[0] = "criteria"

        rows_to_drop = [i for i in [0, 1] if i in df.index]
        df = df.drop(rows_to_drop).reset_index(drop=True)

        # Chuẩn hóa tên cột đầu tiên
        # Bước 1: Chuẩn hóa chuỗi trắng thành NaN
        df.iloc[:, 0] = df.iloc[:, 0].replace(r'^\s*$', np.nan, regex=True)
        # Bước 2: Lưu giá trị gốc để xác định dòng nào ban đầu là NaN
        original_first_col = df.iloc[:, 0].copy()
        # Bước 3: forward fill giá trị
        df.iloc[:, 0] = df.iloc[:, 0].ffill()
        # Bước 4: Đánh dấu dòng mới được điền (NaN ban đầu)
        mask_new_filled = original_first_col.isna()
        # Bước 5: Format toàn bộ cột về snake_case
        df.iloc[:, 0] = df.iloc[:, 0].astype(str).str.strip().str.lower().str.replace(r'\W+', '_', regex=True)
        # Bước 6: Chỉ thêm '_percentage' cho dòng mới được điền
        df.loc[mask_new_filled, df.columns[0]] = df.loc[mask_new_filled, df.columns[0]] + "_percentage"

        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Tạo tên file CSV an toàn
        safe_name = re.sub(r"\s*\(.*?\)", "", sheet_name)
        safe_name = safe_name.replace(" ", "_").replace("/", "_")
        output_path = os.path.join(OUTPUT_DIR, f"{safe_name}.csv")

        # Bước 7: Tạo cột file code
        df["file_code"] = safe_name
        file_code_col = df.pop("file_code")
        df.insert(1, "file_code", file_code_col)

        # Lưu CSV
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        print(f"[PROCESS] Đã lưu sheet '{sheet_name}' thành file: {output_path}")
    except Exception as e:
        print(f"[ERROR] Sheet '{sheet_name}' lỗi: {e}")
