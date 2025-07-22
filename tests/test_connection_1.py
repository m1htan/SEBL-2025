import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;"
    "DATABASE=SEBL-2025;"
    "UID=sa;"
    "PWD=Minhtan0410@;"
    "Encrypt=no;"
    "TrustServerCertificate=yes;"
    "Connection Timeout=5;"
)

print("Kết nối thành công:", conn.getinfo(pyodbc.SQL_SERVER_NAME))
