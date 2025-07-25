import pandas as pd
import os

# --- Cấu hình ---
CSV_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/processed"
CSV_FILES = ["max_2-3_answer_scores.csv", "multiple_answer_scores.csv",
             "one_answer_scores.csv", "write_down_answer_scores.csv"]
TABLE_METADATA = "metadata_549"

from sqlalchemy import create_engine
import urllib

def init_db():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=SEBL-2025;"
        "UID=sa;"
        "PWD=Minhtan0410@;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
        "Connection Timeout=5;"
    )
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    return engine


# --- 1. Merge các file CSV ---
def merge_csv_files(csv_dir, file_list):
    df_all = []
    for file in file_list:
        path = os.path.join(csv_dir, file)
        df = pd.read_csv(path)
        df_all.append(df)
    df_merged = pd.concat(df_all, ignore_index=True)
    return df_merged


# --- 2. Kết nối DB & đọc metadata ---
def get_metadata(conn, table_name):
    query = f"SELECT file_code, group_id FROM {table_name}"
    df_meta = pd.read_sql(query, conn)
    return df_meta


# --- 3. Gán group_id cho từng dòng dựa trên file_code ---
def assign_group_id(df_data, df_meta):
    return df_data.merge(df_meta, on="file_code", how="left")

def minmax_scale_row(row, scale_cols):
    values = row[scale_cols].astype(float)
    min_val = values.min()
    max_val = values.max()
    if max_val == min_val:
        scaled = pd.Series([0.0]*len(scale_cols), index=scale_cols)
    else:
        scaled = 1 + 9 * (values - min_val) / (max_val - min_val)
    return pd.concat([row.drop(scale_cols), scaled])

def scale_each_row(df):
    df_scaled = df.copy()
    exclude_cols = ['file_code', 'group_id', 'EU27']
    scale_cols = [col for col in df.columns if col not in exclude_cols]
    df_scaled = df_scaled.apply(lambda row: minmax_scale_row(row, scale_cols), axis=1)
    return df_scaled

# --- 4. Ghi các nhóm vào DB ---
def save_groups_to_db(df_full, conn, if_exists="replace"):
    grouped = df_full.groupby("group_id")
    for group_id, group_df in grouped:
        table_name = f"group{int(group_id)}"
        group_df.drop(columns=["group_id"], inplace=True)
        print(f"→ Ghi bảng: {table_name}, số dòng: {len(group_df)}")
        group_df.to_sql(table_name, con=conn, index=False, if_exists=if_exists)


if __name__ == "__main__":
    # Bước 1: Merge các file CSV
    df_data = merge_csv_files(CSV_DIR, CSV_FILES)
    print(f"[+] Dữ liệu sau khi merge: {df_data.shape}")

    # Bước 2: Khởi tạo kết nối DB
    conn = init_db()

    # Nếu dùng SQLAlchemy, đảm bảo `conn` là dạng engine hoặc connectable
    df_meta = get_metadata(conn, TABLE_METADATA)
    print(f"[+] Metadata: {df_meta.shape}")

    # Bước 3: Gán group_id
    df_grouped = assign_group_id(df_data, df_meta)
    print(f"[+] Sau khi gán group_id: {df_grouped.shape}")

    if df_grouped["group_id"].isnull().any():
        raise ValueError("Một số dòng không có group_id, vui lòng kiểm tra file_code!")

    # Bước 4: Lưu vào từng bảng group{group_id}
    df_scaled = scale_each_row(df_grouped)
    save_groups_to_db(df_scaled, conn)

    print("Hoàn tất chia và lưu bảng group")