import os
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

from config_db import config_sql_server

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "multiple_answer_scores.csv")

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

    # Lấy giá trị hàng đầu tiên
    type = df_meta['type'].values[0]
    file_code = df_meta['file_code'].values[0]

    return type, file_code

def process_table(table_name: str, conn):
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # Chỉ giữ các cột số (giả định là các cột quốc gia)
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    if not numeric_cols:
        print(f"[SKIP] {table_name}: Không có cột số.")
        return None

    df_filtered = df[~df.iloc[:, 0].str.contains(r"_+percentage", regex=True, na=False)]

    # Lấy base_total ở dòng đầu tiên
    base_total = df_filtered.loc[0, numeric_cols]
    df_valid = df_filtered.drop(index=0)

    # Tính tổng tất cả dòng còn lại theo từng quốc gia
    country_totals = df_valid[numeric_cols].sum()

    # Tính tỉ lệ: tổng / base_total
    ratio_scores = country_totals / base_total

    # Scale toàn bộ quốc gia về thang điểm 1–10
    scaler = MinMaxScaler(feature_range=(1, 10))
    scaled_scores = pd.Series(
        scaler.fit_transform(ratio_scores.values.reshape(-1, 1)).flatten(),
        index=ratio_scores.index
    )

    # Ghép kết quả vào DataFrame đầu ra
    _, file_code = get_meta_data(table_name, conn)
    result = scaled_scores.to_frame().T
    result.insert(0, 'file_code', file_code)

    # Tính trung bình các cột numeric_cols (KHÔNG có EU27)
    mean_without_eu27 = df_valid[numeric_cols].mean()

    # Tìm cột EU27 (nếu có)
    eu27_cols = [col for col in df_valid.columns if "EU27" in col]

    # Tính trung bình các cột numeric_cols + EU27 (nếu có)
    if eu27_cols:
        numeric_with_eu27 = numeric_cols + eu27_cols
        mean_with_eu27 = df_valid[numeric_with_eu27].mean()
    else:
        print("Không có cột EU27 nào trong dữ liệu.")
        mean_with_eu27 = mean_without_eu27  # fallback để không lỗi

    # So sánh hai trung bình
    mean_diff = (mean_with_eu27 - mean_without_eu27).abs()
    mean_pct_diff = (mean_diff / mean_without_eu27.replace(0, np.nan)) * 100

    # In kết quả
    print("Chênh lệch tuyệt đối trung bình giữa có và không có EU27:")
    print(mean_diff.describe())

    print("\nChênh lệch tương đối (theo phần trăm):")
    print(mean_pct_diff.describe())

    return result

if __name__ == "__main__":
    conn = init_db()
    # Lấy danh sách các bảng MULTIPLE ANSWERS POSSIBLE
    query = "SELECT DISTINCT target_name FROM metadata_549 WHERE type = 'MULTIPLE ANSWERS POSSIBLE'"
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


