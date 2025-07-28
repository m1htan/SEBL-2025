import os
import urllib
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/all"
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

def get_metadata_with_weight(engine, table_name='metadata_549'):
    query = f"SELECT group_id, group_weight FROM {table_name}"
    return pd.read_sql(query, engine)

def read_all_group_tables(engine, metadata_df: pd.DataFrame) -> pd.DataFrame:
    all_dfs = []
    for group_id in metadata_df["group_id"].unique():
        table_name = f"group{group_id}"
        try:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, engine)
            df["group_id"] = group_id  # Đảm bảo có group_id
            all_dfs.append(df)
        except Exception as e:
            print(f"[WARNING] Không thể đọc bảng {table_name}: {e}")
    return pd.concat(all_dfs, ignore_index=True)

def compute_weighted_scaled_scores(df_score: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    # Merge trọng số
    df_merged = df_score.merge(df_meta, on="group_id", how="left")

    # Nhân điểm với trọng số
    df_merged["weighted_score"] = df_merged["score"] * df_merged["group_weight"]

    # Chuẩn hóa min-max theo từng group_id
    df_result = []
    for group_id, group_df in df_merged.groupby("group_id"):
        scaler = MinMaxScaler(feature_range=(1, 10))
        group_df["scaled_score"] = scaler.fit_transform(group_df[["weighted_score"]])
        df_result.append(group_df)

    return pd.concat(df_result, ignore_index=True)

def save_groups_to_db(df_final: pd.DataFrame, engine):
    for group_id, group_df in df_final.groupby("group_id"):
        table_name = f"final_score_group{group_id}"
        group_df.to_sql(table_name, engine, if_exists="replace", index=False)
        print(f"Đã lưu bảng: {table_name} ({len(group_df)} dòng)")

if __name__ == "__main__":
    engine = init_engine()

    # Lấy metadata
    df_meta = get_metadata_with_weight(engine)

    # Đọc tất cả các bảng group
    df_score = read_all_group_tables(engine, df_meta)

    # Tính điểm
    df_final = compute_weighted_scaled_scores(df_score, df_meta)
    df_final = df_final.drop_duplicates()

    save_groups_to_db(df_final, engine)

    # Lưu từng group_id
    for group_id, group_df in df_final.groupby("group_id"):
        output_path = os.path.join(OUTPUT_DIR, f"final_score_group{group_id}.csv")
        group_df.to_csv(output_path, index=False)
        print(f"Đã lưu: {output_path}")

    engine.dispose()
