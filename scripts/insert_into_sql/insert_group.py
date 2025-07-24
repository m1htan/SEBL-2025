import pyodbc
import pandas as pd
from collections import defaultdict
from config_db import config_sql_server

# 1. Kết nối DB
def init_db():
    return config_sql_server(section='sqlserver')

def get_table_names(conn):
    cursor = conn.cursor()
    cursor.execute("""
    SELECT TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
    WHERE TABLE_TYPE = 'BASE TABLE'
    """)
    return [row[0] for row in cursor.fetchall()]

def merge_group():
    table_names = get_table_names(conn)

    # Nhóm các bảng theo prefix "group1", "group2", v.v.
    group_tables = defaultdict(list)
    for table in table_names:
        if table.startswith("group"):
            group_prefix = table.split("_")[0]  # ví dụ: group1
            group_tables[group_prefix].append(table)

    # Gộp từng group và lưu vào bảng mới trong DB
    for group, tables in group_tables.items():
        dfs = []
        for tbl in tables:
            df = pd.read_sql(f"SELECT * FROM {tbl}", conn)
            df['source_table'] = tbl  # Ghi chú nguồn dữ liệu
            dfs.append(df)

        merged_df = pd.concat(dfs, ignore_index=True)

        # Tên bảng mới (ví dụ: group1)
        new_table_name = group

        # Tạo bảng mới (DROP nếu đã tồn tại)
        cursor.execute(f"IF OBJECT_ID('{new_table_name}', 'U') IS NOT NULL DROP TABLE {new_table_name}")
        conn.commit()

        # Tạo bảng mới từ dataframe
        create_table_sql = f"CREATE TABLE {new_table_name} ("
        for col in merged_df.columns:
            coltype = "NVARCHAR(MAX)" if merged_df[col].dtype == "object" else "FLOAT"
            create_table_sql += f"[{col}] {coltype},"
        create_table_sql = create_table_sql.rstrip(",") + ")"
        cursor.execute(create_table_sql)
        conn.commit()

        # Insert dữ liệu
        insert_sql = f"INSERT INTO {new_table_name} ({', '.join(f'[{c}]' for c in merged_df.columns)}) VALUES ({', '.join(['?' for _ in merged_df.columns])})"
        for _, row in merged_df.iterrows():
            cursor.execute(insert_sql, tuple(row))
        conn.commit()

        print(f"Đã tạo và lưu bảng {new_table_name}: {merged_df.shape[0]} dòng")

if __name__ == "__main__":
    conn = init_db()
    cursor = conn.cursor()
    print("Kết nối database thành công!")

    merge_group()

    conn.close()