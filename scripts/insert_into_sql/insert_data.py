import os
import pandas as pd
from config import config_sql_server

def init_db():
    return config_sql_server(section='sqlserver')

def infer_sql_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "NVARCHAR(MAX)"

def create_table_from_df(df, table_name, cursor, conn):
    columns = []
    for col in df.columns:
        sql_type = infer_sql_dtype(df[col].dtype)
        safe_col = f"[{col}]"
        columns.append(f"{safe_col} {sql_type}")
    column_defs = ", ".join(columns)
    create_sql = f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ({column_defs})"
    cursor.execute(create_sql)
    conn.commit()

def insert_df_to_table(df, table_name, cursor, conn):
    placeholders = ", ".join(["?"] * len(df.columns))
    col_names = ", ".join([f"[{col}]" for col in df.columns])
    insert_sql = f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})"
    for _, row in df.iterrows():
        cursor.execute(insert_sql, tuple(row))
    conn.commit()

def process_from_metadata(conn):
    query = "SELECT * FROM metadata_549"
    df_meta = pd.read_sql(query, conn)

    cursor = conn.cursor()

    for idx, row in df_meta.iterrows():
        source_path = row['source_path']
        source_name = row['source_name']
        target_name = row['target_name']

        file_path = os.path.join(source_path)
        if not os.path.exists(file_path):
            print(f"[WARNING] File không tồn tại: {file_path}")
            continue

        print(f"[PROCESSING] {file_path} -> table [{target_name}]")

        df = pd.read_csv(file_path)

        create_table_from_df(df, target_name, cursor, conn)
        insert_df_to_table(df, target_name, cursor, conn)

    print("Hoàn tất insert toàn bộ dữ liệu từ metadata.")

if __name__ == "__main__":
    conn = init_db()
    print("Kết nối database thành công!")

    process_from_metadata(conn)

    conn.close()