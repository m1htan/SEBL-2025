import pandas as pd

df = pd.read_csv("/Users/minhtan/Documents/GitHub/SEBL-2025/data/metadata_549.csv")
print(df.dtypes)

import pyodbc
from config_db import config_sql_server

conn = config_sql_server(section='sqlserver')
cursor = conn.cursor()

table_name = 'metadata_549'
query1 = f"""
SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = '{table_name}'
"""
query2 = f"""
SELECT 
    c.COLUMN_NAME, 
    c.DATA_TYPE, 
    c.CHARACTER_MAXIMUM_LENGTH, 
    c.IS_NULLABLE,
    k.CONSTRAINT_TYPE
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
    ON c.TABLE_NAME = ku.TABLE_NAME AND c.COLUMN_NAME = ku.COLUMN_NAME
LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS k
    ON ku.CONSTRAINT_NAME = k.CONSTRAINT_NAME
WHERE c.TABLE_NAME = 'metadata_549'
"""
cursor.execute(query1)
cursor.execute(query2)

rows = cursor.fetchall()

for col in rows:
    print(f"{col.COLUMN_NAME}: {col.DATA_TYPE} ({col.CHARACTER_MAXIMUM_LENGTH})")
