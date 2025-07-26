import pandas as pd
import os
from sqlalchemy import create_engine
import urllib

CSV_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/processed"
output_dir = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/merged_data"
CSV_FILES = ["max_2-3_answer_scores.csv", "multiple_answer_scores.csv",
             "one_answer_scores.csv", "write_down_answer_scores.csv"]
TABLE_METADATA = "metadata_549"

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

# Merge các file CSV
def merge_csv_files(csv_dir, file_list):
    df_all = []
    for file in file_list:
        path = os.path.join(csv_dir, file)
        df = pd.read_csv(path)
        df_all.append(df)
    df_merged = pd.concat(df_all, ignore_index=True)
    return df_merged

# Kết nối DB & đọc metadata
def get_metadata(conn, table_name):
    query = f"SELECT file_code, group_id FROM {table_name}"
    df_meta = pd.read_sql(query, conn)
    return df_meta

# Gán group_id cho từng dòng dựa trên file_code
def assign_group_id(df_data, df_meta):
    return df_data.merge(df_meta, on="file_code", how="left")

def reshape_to_long_format(df):
    # Xác định các cột id (không xoay)
    id_vars = ['file_code', 'group_id']

    # Loại bỏ cột 'EU27' nếu tồn tại
    if 'EU27' in df.columns:
        df = df.drop(columns=['EU27'])

    # Các cột cần xoay (quốc gia)
    value_vars = [col for col in df.columns if col not in id_vars]

    # Thực hiện melt (xoay dọc)
    df_long = df.melt(id_vars=id_vars,
                      value_vars=value_vars,
                      var_name='country_code',
                      value_name='score')

    # Di chuyển country_code lên ngay sau file_code
    cols = df_long.columns.tolist()
    cols.insert(1, cols.pop(cols.index("country_code")))
    df_long = df_long[cols]

    df_long = df_long.sort_values(by=["file_code", "country_code"]).reset_index(drop=True)

    return df_long

# Ghi các nhóm vào DB
def save_groups_to_db(df_full, conn, if_exists="replace"):
    grouped = df_full.groupby("group_id")
    for group_id, group_df in grouped:
        table_name = f"group{int(group_id)}"

        cols = group_df.columns.tolist()
        ordered_cols = ['file_code', 'group_id']
        if 'EU27' in cols:
            ordered_cols.append('EU27')
        # Thêm các cột còn lại
        other_cols = [col for col in cols if col not in ordered_cols]
        final_cols = ordered_cols + other_cols
        group_df = group_df[final_cols]

        csv_path = os.path.join(output_dir, f"group{int(group_id)}.csv")
        group_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[+] Đã lưu file: {csv_path}, số dòng: {len(group_df)}")

        print(f"Ghi bảng: {table_name}, số dòng: {len(group_df)}")
        group_df.to_sql(table_name, con=conn, index=False, if_exists=if_exists)

if __name__ == "__main__":
    # Merge các file CSV
    df_data = merge_csv_files(CSV_DIR, CSV_FILES)
    print(f"[+] Dữ liệu sau khi merge: {df_data.shape}")

    conn = init_db()

    # Nếu dùng SQLAlchemy, đảm bảo `conn` là dạng engine hoặc connectable
    df_meta = get_metadata(conn, TABLE_METADATA)
    print(f"[+] Metadata: {df_meta.shape}")

    # Gán group_id
    df_grouped = assign_group_id(df_data, df_meta)
    print(f"[+] Sau khi gán group_id: {df_grouped.shape}")

    if df_grouped["group_id"].isnull().any():
        raise ValueError("Một số dòng không có group_id, vui lòng kiểm tra file_code!")

    # Lưu vào từng bảng group{group_id}
    df_long = reshape_to_long_format(df_grouped)
    save_groups_to_db(df_long, conn)

    print("Hoàn tất chia và lưu bảng group")