import os
import pandas as pd

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

    # Chỉ giữ các cột số
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    if not numeric_cols:
        print(f"[SKIP] {table_name}: Không có cột số.")
        return None

    df_filtered = df[
        ~df.iloc[:, 0].str.contains(r"_+percentage", regex=True, na=False) &
        ~df.iloc[:, 0].isin(["base_weighted_total"])
        ]
    print(df_filtered)

    # Lấy base_total ở dòng đầu tiên
    base_total = df_filtered.loc[0, numeric_cols]
    df_valid = df_filtered.drop(index=0)

    # Tính tổng tất cả dòng còn lại theo từng quốc gia
    country_totals = df_valid[numeric_cols].sum()

    # Tính tỉ lệ: tổng / base_total
    ratio_scores = country_totals / base_total

    # Ghép kết quả vào DataFrame đầu ra
    _, file_code = get_meta_data(table_name, conn)
    result = ratio_scores.to_frame().T
    result.insert(0, 'file_code', file_code)

    pd.set_option("display.float_format", lambda x: f"{x:.10f}")
    print(result.head())

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
