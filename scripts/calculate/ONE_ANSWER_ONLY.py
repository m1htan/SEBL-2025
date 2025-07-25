import os
import re
from collections import defaultdict

import pandas as pd
import numpy as np

from config_db import config_sql_server

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "one_answer_scores.csv")

# --- Kết nối DB ---
def init_db():
    return config_sql_server(section='sqlserver')

def get_meta_data(table_name: str, conn):
    query = """
    SELECT * 
    FROM metadata_549 
    WHERE target_name = ?
    """
    df_meta = pd.read_sql(query, conn, params=[table_name])

    if df_meta.empty:
        raise ValueError(f"Không tìm thấy metadata cho target_name = '{table_name}'")

    return df_meta

def create_scoring_dict(df_filtered):
    scoring_dict = {}

    criteria_col = df_filtered.columns[0]

    # Bỏ dòng đầu tiên (thường là Q1, Q2,...)
    df_valid = df_filtered.drop(index=0).copy()

    # Lấy danh sách lựa chọn trả lời (dòng) – không chứa NaN
    answer_choices = df_valid[criteria_col].dropna().unique().tolist()

    # Gán điểm đều từ 0 → 10 theo thứ tự tăng dần
    scores = np.linspace(1, 10, num=len(answer_choices))
    answer_score_map = dict(zip(answer_choices, scores))

    return answer_score_map

def process_table(table_name: str, conn):
    import pandas as pd

    # 1. Đọc bảng dữ liệu từ DB
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # 2. Bỏ các dòng có hậu tố "_percentage" hoặc "__percentage"
    df_filtered = df[~df.iloc[:, 0].str.contains(r"_+percentage", regex=True, na=False)]

    # 3. Đọc metadata
    df_meta = get_meta_data(table_name, conn)
    file_code = df_meta["file_code"].values[0]
    type = df_meta["type"].values[0]

    # 4. Tạo scoring dict (gán điểm cho từng lựa chọn)
    scoring_dict = create_scoring_dict(df_filtered)
    if not scoring_dict:
        print(f"[SKIP] {table_name}: Không có scoring dict.")
        return None

    # 5. Xác định cột quốc gia và cột tiêu chí
    country_cols = df.select_dtypes(include='number').columns.tolist()
    criteria_col = df_filtered.columns[0]

    # 6. Bỏ dòng tiêu đề (header)
    df_valid = df_filtered.drop(index=0).copy()

    # 7. Tính điểm từng quốc gia
    score_sum = pd.Series(0.0, index=country_cols)
    valid_count = 0

    for _, row in df_valid.iterrows():
        label = row[criteria_col]
        if label not in scoring_dict:
            continue

        score = scoring_dict[label]
        try:
            weights = row[country_cols].astype(float)  # tỷ lệ chọn %
            score_sum += weights * score / 100
            valid_count += 1
        except ValueError:
            print(f"[CẢNH BÁO] {table_name}, nhãn '{label}': không thể chuyển về float")
            continue

    # 8. Tính điểm trung bình thay vì cộng dồn
    if valid_count == 0:
        print(f"[SKIP] {table_name}: Không có dòng hợp lệ để tính.")
        return None

    avg_score = score_sum / valid_count
    result = avg_score.to_frame().T
    result.insert(0, "file_code", file_code)

    return result

if __name__ == "__main__":
    conn = init_db()
    # Lấy danh sách các bảng MULTIPLE ANSWERS POSSIBLE
    query = "SELECT DISTINCT target_name FROM metadata_549 WHERE type = 'ONE ANSWER ONLY'"
    table_list = pd.read_sql(query, conn)['target_name'].tolist()

    result_rows = []

    for table in table_list:
        print(f"Đang xử lý bảng: {table}")
        row = process_table(table, conn)
        if row is not None:
            result_rows.append(row)

    conn.close()

    if result_rows:
        df_result = pd.concat(result_rows, ignore_index=True)
        df_result.to_csv(OUTPUT_FILE, index=False)
        print(f"Đã lưu kết quả tại: {OUTPUT_FILE}")
    else:
        print("Không có bảng nào được xử lý.")
