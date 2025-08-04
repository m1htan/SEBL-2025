import os
import pandas as pd
import re

from config import config_sql_server

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "max_2-3_answer_scores.csv")

# --- Kết nối DB ---
def init_db():
    return config_sql_server(section='sqlserver')

def extract_max_answers(type_str):
    match = re.search(r'MAX(?:\.|\s)?\s*(\d+)', type_str)
    if match:
        return int(match.group(1))
    else:
        raise ValueError(f"Không tách được số lựa chọn từ type: {type_str}")

def get_meta_data(table_name: str, conn):
    query = "SELECT * FROM metadata_549 WHERE target_name = ?"
    df_meta = pd.read_sql(query, conn, params=[table_name])
    if df_meta.empty:
        raise ValueError(f"Không tìm thấy metadata cho target_name = '{table_name}'")
    type_str = df_meta['type'].values[0]
    file_code = df_meta['file_code'].values[0]

    # Trích số lựa chọn tối đa (MAX. 2 ANSWERS POSSIBLE -> 2)
    max_answers = extract_max_answers(type_str)
    return max_answers, file_code

def process_table_max_answers(table_name: str, conn):
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)

    try:
        max_answers, file_code = get_meta_data(table_name, conn)
    except ValueError as e:
        print(e)
        return None

    criteria_col = df.columns[0]

    df_filtered = df[
        ~df.iloc[:, 0].str.contains(r"_+percentage", regex=True, na=False) &
        ~df.iloc[:, 0].isin(["base_weighted_total"])
        ]

    # Tách hàng base_total ra riêng
    base_row = df_filtered[df_filtered[criteria_col] == 'base_total']
    df_data = df_filtered[df_filtered[criteria_col] != 'base_total']

    if base_row.empty:
        print(f"[WARNING] Không có hàng 'base_total' trong {table_name}")
        return None

    result = {"file_code": file_code}
    # Chỉ duyệt các cột kiểu số
    numeric_cols = df.select_dtypes(include='number').columns.tolist()
    for col in numeric_cols:
        try:
            total_selected = df_data[col].astype(float).sum()
            base_value = float(base_row[col].values[0])
            score = (total_selected / (base_value * max_answers)) * 10 if base_value > 0 else None
            result[col] = round(score, 3) if score is not None else None
        except Exception as e:
            print(f"Lỗi xử lý {col} trong {table_name}: {e}")
            result[col] = None

    return result

if __name__ == "__main__":
    conn = init_db()

    query = """
    SELECT DISTINCT target_name 
    FROM metadata_549 
    WHERE type IN ('MAX. 2 ANSWERS POSSIBLE', 'MAX. 3 ANSWERS POSSIBLE')
    """
    table_list = pd.read_sql(query, conn)['target_name'].tolist()

    result_rows = []
    for table in table_list:
        print(f"Đang xử lý bảng: {table}")
        row = process_table_max_answers(table, conn)
        if row is not None:
            result_rows.append(row)
    conn.close()

    if result_rows:
        df_result = pd.DataFrame(result_rows)
        df_result.to_csv(OUTPUT_FILE, index=False)
        print(f"Đã lưu kết quả tại: {OUTPUT_FILE}")
    else:
        print("Không có bảng nào được xử lý.")
