import os
import pandas as pd


from config_db import config_sql_server

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/processed"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "max_2-3_answer_scores.csv")

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



if __name__ == "__main__":
    conn = init_db()

    conn.close()
