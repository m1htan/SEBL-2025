import os
import pandas as pd
import pyodbc
from config_db import config_sql_server

CSV_DIR = "/data/data_clean/volume_A"
os.makedirs(CSV_DIR, exist_ok=True)

csv_files = ("Q1.csv", "Q2.csv", "Q3.csv", "Q4.csv", "Q5.csv", "Q6.csv", "Q7.csv", "Q8.csv",
             "N1a.csv", "N1b.csv", "N2.csv", "N3.csv",
             "Q14.csv", "Q15.csv",
             "Q9.csv", "Q10.csv", "Q13.csv", "DX1.csv", "DX3.csv", "DX4.csv", "DX5.csv",
             "NACEA.csv", "NACEB.csv", "D1b.csv", "SCR10.csv", "SCR11a.csv", "SCR11b.csv",
             "SCR12.csv", "SCR13a.csv", "SCR14.csv", "brk_SCR14.csv", "SCR16.csv")

def init_db():
    conn = config_sql_server(section='sqlserver')
    return conn

def create_table_from_csv(cursor, df, table_name):
    columns_with_types = []
    for col in df.columns:
        dtype = df[col].dtype
        if dtype == "int64":
            sql_type = "INT"
        elif dtype == "float64":
            sql_type = "FLOAT"
        elif dtype == "bool":
            sql_type = "BIT"
        else:
            sql_type = "NVARCHAR(MAX)"
        columns_with_types.append(f"[{col}] {sql_type}")

    # Drop if exists, then create table
    cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE [{table_name}]")
    cursor.execute(f"CREATE TABLE [{table_name}] ({', '.join(columns_with_types)})")

def insert_data(cursor, df, table_name):
    insert_sql = (f"INSERT INTO [{table_name}] ({', '.join([f'[{col}]' for col in df.columns])}) "
                  f"VALUES ({', '.join(['?' for _ in df.columns])})")
    for _, row in df.iterrows():
        cursor.execute(insert_sql, *row)

def process_csv_file(conn, csv_file):
    file_path = os.path.join(CSV_DIR, csv_file)
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    df = pd.read_csv(file_path)
    cursor = conn.cursor()
    create_table_from_csv(cursor, df, table_name)
    insert_data(cursor, df, table_name)
    conn.commit()
    print(f"Đã xử lý xong file: {file_path}")

if __name__ == '__main__':
    conn = init_db()
    print("Kết nối database thành công!")

    for csv_file in csv_files:
        full_path = os.path.join(CSV_DIR, csv_file)
        if os.path.exists(full_path):
            process_csv_file(conn, csv_file)
        else:
            print(f"[SKIP] File không tồn tại: {csv_file}")

    conn.close()
    print("Hoàn tất.")