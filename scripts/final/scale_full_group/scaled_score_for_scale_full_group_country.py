import os
import urllib
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/scale_full_group"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Kết nối DB
def init_engine():
    params = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=localhost;"
        "DATABASE=SEBL-2025;"
        "UID=sa;"
        "PWD=Minhtan0410@;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Lấy metadata trọng số
def get_metadata_with_weight(engine, table_name='metadata_549'):
    query = f"SELECT group_id, group_weight FROM {table_name}"
    return pd.read_sql(query, engine)

# Đọc danh sách quốc gia có splits = 4
def get_countries_with_splits_4(engine, country_table='country'):
    query = f"""
        SELECT country_code 
        FROM {country_table}
        WHERE splits = 4
    """
    return pd.read_sql(query, engine)['country_code'].tolist()

# Đọc dữ liệu từ tất cả bảng group
def read_all_group_tables(engine, metadata_df: pd.DataFrame) -> pd.DataFrame:
    all_dfs = []
    for group_id in metadata_df["group_id"].unique():
        table_name = f"group{group_id}"
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            df["group_id"] = group_id
            all_dfs.append(df)
        except Exception as e:
            print(f"[WARNING] Không thể đọc bảng {table_name}: {e}")
    return pd.concat(all_dfs, ignore_index=True)

# Tính điểm có trọng số và chuẩn hóa toàn cục
def compute_weighted_scaled_scores_global(df_score: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    df_merged = df_score.merge(df_meta, on="group_id", how="left")
    df_merged["weighted_score"] = df_merged["score"] * df_merged["group_weight"]

    # Chuẩn hóa toàn bộ dataframe (không theo group)
    scaler = MinMaxScaler(feature_range=(1, 10))
    df_merged["scaled_score"] = scaler.fit_transform(df_merged[["weighted_score"]])

    return df_merged

# Lưu kết quả tổng vào DB
def save_final_score_to_db(df_final: pd.DataFrame, engine):
    table_name = "scaled_score_for_scale_full_group_country"
    df_final.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Đã lưu bảng tổng: {table_name} ({len(df_final)} dòng)")

# MAIN
if __name__ == "__main__":
    engine = init_engine()

    # Bước 1: Lấy metadata & danh sách quốc gia splits = 4
    df_meta = get_metadata_with_weight(engine)
    valid_country_codes = get_countries_with_splits_4(engine)

    # Bước 2: Đọc dữ liệu từ các bảng group
    df_score = read_all_group_tables(engine, df_meta)

    # Bước 3: Lọc theo danh sách quốc gia có splits = 4
    df_score = df_score[df_score['country_code'].isin(valid_country_codes)]

    # Bước 4: Tính điểm có trọng số + chuẩn hóa global
    df_final = compute_weighted_scaled_scores_global(df_score, df_meta)
    df_final = df_final.drop_duplicates()

    # Bước 5: Lưu vào DB và CSV
    save_final_score_to_db(df_final, engine)

    output_path = os.path.join(OUTPUT_DIR, "scaled_score_for_scale_full_group_country.csv")
    df_final.to_csv(output_path, index=False)
    print(f"Đã lưu file CSV tổng: {output_path}")

    engine.dispose()