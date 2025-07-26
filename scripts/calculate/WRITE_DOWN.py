import os
import pandas as pd

from config_db import config_sql_server

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "write_down_answer_scores.csv")

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
    score_mapping = {
        '0_employees': 0,
        '1_5_employees': 2,
        '6_9_employees': 4,
        '10_50_employees': 6,
        '51_100_employees': 8,
        '101_employees': 10
    }

    # Giữ lại các nhãn có trong score_mapping
    valid_labels = df_valid[criteria_col].dropna()
    answer_score_map = {
        label: score_mapping[label]
        for label in valid_labels
        if label in score_mapping
    }

    return answer_score_map

def process_table(table_name: str, conn):
    import pandas as pd

    # 1. Đọc bảng dữ liệu từ DB
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    # Lấy danh sách cột số (các cột quốc gia)
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    if not numeric_cols:
        print(f"[SKIP] {table_name}: Không có cột số.")
        return None

    # 2. Bỏ các dòng không cần thiết: chứa "_percentage", "__percentage", hoặc là "base_weighted_total"
    df_filtered = df[
        ~df.iloc[:, 0].str.contains(r"_+percentage", regex=True, na=False) &
        ~df.iloc[:, 0].isin(["base_weighted_total"])
    ].copy()

    # 3. Đọc metadata
    df_meta = get_meta_data(table_name, conn)
    file_code = df_meta["file_code"].values[0]
    q_type = df_meta["type"].values[0]

    # 4. Tạo scoring dict (gán điểm cho từng lựa chọn)
    scoring_dict = create_scoring_dict(df_filtered)
    if not scoring_dict:
        print(f"[SKIP] {table_name}: Không có scoring dict.")
        return None

    # 5. Xác định cột quốc gia và cột tiêu chí
    criteria_col = df_filtered.columns[0]

    # 6. Bỏ dòng tiêu đề (nếu có)
    df_valid = df_filtered.copy()
    if df_valid.iloc[0][criteria_col].strip().lower() == "base_total":
        df_valid = df_valid.drop(index=0)

    # 7. Tách dòng base_total riêng để lấy mẫu số
    base_row = df[df[criteria_col].str.lower() == "base_total"]
    if base_row.empty:
        print(f"[SKIP] {table_name}: Không tìm thấy dòng base_total.")
        return None
    base_total = base_row[numeric_cols].iloc[0]

    # 8. Tính tổng điểm cho từng quốc gia
    score_result = {}
    for country in numeric_cols:
        total_score = 0.0
        for idx, row in df_valid.iterrows():
            key = row[criteria_col]
            if key in scoring_dict:
                count = row[country]
                point = scoring_dict[key]
                total_score += count * point
        # Chia lại cho base_total của quốc gia đó
        score_result[country] = (total_score / base_total[country]*1) if base_total[country] != 0 else None

    # 9. Tạo dataframe kết quả
    result = pd.DataFrame([score_result])
    result.insert(0, "file_code", file_code)
    return result

if __name__ == "__main__":
    conn = init_db()
    # Lấy danh sách các bảng MULTIPLE ANSWERS POSSIBLE
    query = "SELECT DISTINCT target_name FROM metadata_549 WHERE type = 'WRITE DOWN'"
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
