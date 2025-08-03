import os
import urllib
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent"
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

# Đọc metadata trọng số
def get_metadata_with_weight(engine, table_name='metadata_549'):
    query = f"SELECT group_id, group_weight FROM {table_name}"
    return pd.read_sql(query, engine)

# Đọc danh sách quốc gia có splits = 4
def get_countries_with_splits_4(engine, country_table='country'):
    query = f"""
        SELECT country_code 
        FROM {country_table}
        WHERE splits = 6
    """
    return pd.read_sql(query, engine)['country_code'].tolist()

# Đọc toàn bộ các bảng group
def read_all_group_tables(engine, metadata_df: pd.DataFrame, valid_country_codes: list) -> pd.DataFrame:
    all_dfs = []
    for group_id in metadata_df["group_id"].unique():
        table_name = f"group{group_id}"
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            df = df[df['country_code'].isin(valid_country_codes)]  # Lọc quốc gia có splits = 4
            df["group_id"] = group_id
            all_dfs.append(df)
        except Exception as e:
            print(f"[WARNING] Không thể đọc bảng {table_name}: {e}")
    return pd.concat(all_dfs, ignore_index=True)

# Tính điểm có trọng số và chuẩn hóa
def compute_weighted_scaled_scores(df_score: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    df_merged = df_score.merge(df_meta, on="group_id", how="left")
    df_merged["weighted_score"] = df_merged["score"] * df_merged["group_weight"]

    df_result = []
    for group_id, group_df in df_merged.groupby("group_id"):
        scaler = MinMaxScaler(feature_range=(1, 10))
        group_df["scaled_score"] = scaler.fit_transform(group_df[["weighted_score"]])
        df_result.append(group_df)

    return pd.concat(df_result, ignore_index=True)

# Lưu ra bảng mới trong DB
def save_groups_to_db(df_final: pd.DataFrame, engine):
    for group_id, group_df in df_final.groupby("group_id"):
        table_name = f"scaled_score_for_6_part_ratio_country_group{group_id}"
        group_df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Đã lưu bảng: {table_name} ({len(group_df)} dòng)")

# MAIN
if __name__ == "__main__":
    engine = init_engine()

    df_meta = get_metadata_with_weight(engine)
    valid_countries = get_countries_with_splits_4(engine)

    df_score = read_all_group_tables(engine, df_meta, valid_countries)

    df_final = compute_weighted_scaled_scores(df_score, df_meta)
    df_final = df_final.drop_duplicates()

    save_groups_to_db(df_final, engine)

    # Lưu từng file CSV theo group
    for group_id, group_df in df_final.groupby("group_id"):
        output_path = os.path.join(OUTPUT_DIR, f"scaled_score_for_6_part_ratio_country_group{group_id}.csv")
        group_df.to_csv(output_path, index=False)
        print(f"Đã lưu: {output_path}")

    engine.dispose()