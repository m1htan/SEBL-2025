import pandas as pd
import pyodbc
from config import config_sql_server

# Tên file và bảng
csv_file = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/metadata/metadata_549.csv"
table_name = "metadata_549"

# Đọc từ CSV
df_csv = pd.read_csv(csv_file)

# Kết nối DB
conn = config_sql_server(section="sqlserver")

# Đọc từ SQL Server
query = f"SELECT * FROM [{table_name}]"
df_sql = pd.read_sql(query, conn)

# Bước 1: Sắp xếp và reset index để dễ so sánh
df_csv_sorted = df_csv.sort_values(by=df_csv.columns.tolist()).reset_index(drop=True)
df_sql_sorted = df_sql.sort_values(by=df_sql.columns.tolist()).reset_index(drop=True)

# Bước 2: So sánh
if df_csv_sorted.equals(df_sql_sorted):
    print("Dữ liệu trong bảng SQL GIỐNG hoàn toàn với file CSV.")
else:
    print("Dữ liệu KHÁC nhau giữa bảng SQL và file CSV.")
    # Tùy chọn: ghi ra phần khác biệt
    diff = pd.concat([df_csv_sorted, df_sql_sorted]).drop_duplicates(keep=False)
    print("❤Khác biệt:")
    print(diff)
