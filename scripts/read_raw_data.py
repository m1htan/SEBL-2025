import os
import pandas as pd

DATA_RAW_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/raw_data/FL549/Dataset SEBL_FL549"
os.makedirs(DATA_RAW_DIR, exist_ok=True)

# Đọc file xlsx volume A
raw_volume_A = os.path.join(DATA_RAW_DIR, 'SMEs resource efficiency green markets_fl_549_volume_A.xlsx')
df_raw_volume_A = pd.read_excel(raw_volume_A, sheet_name=None)

# Đọc file xlsx volume AA
raw_volume_AA = os.path.join(DATA_RAW_DIR, 'SMEs resource efficiency green markets_fl_549_volume_AA.xlsx')
df_raw_volume_AA = pd.read_excel(raw_volume_AA, sheet_name=None)

# Đọc file xlsx volume AAP
raw_volume_AAP = os.path.join(DATA_RAW_DIR, 'SMEs resource efficiency green markets_fl_549_volume_AAP.xlsx')
df_raw_volume_AAP = pd.read_excel(raw_volume_AAP, sheet_name=None)

# Đọc file xlsx volume AP
raw_volume_AP = os.path.join(DATA_RAW_DIR, 'SMEs resource efficiency green markets_fl_549_volume_AP.xlsx')
df_raw_volume_AP = pd.read_excel(raw_volume_AP, sheet_name=None)

# Đọc file xlsx volume B
raw_volume_B = os.path.join(DATA_RAW_DIR, 'SMEs resource efficiency green markets_fl_549_volume_B.xlsx')
df_raw_volume_B = pd.read_excel(raw_volume_B, sheet_name=None)

# Đọc file xlsx volume BP
raw_volume_BP = os.path.join(DATA_RAW_DIR, 'SMEs resource efficiency green markets_fl_549_volume_BP.xlsx')
df_raw_volume_BP = pd.read_excel(raw_volume_BP, sheet_name=None)

list_data_raw = {
    "volume_A": df_raw_volume_A,
    "volume_AA": df_raw_volume_AA,
    "volume_AAP": df_raw_volume_AAP,
    "volume_AP": df_raw_volume_AP,
    "volume_B": df_raw_volume_B,
    "volume_BP": df_raw_volume_BP
}

for workbook_name, workbook in list_data_raw.items():
    print(f"\nWorkbook: {workbook_name}")
    for sheet_name, df in workbook.items():
        print(f"Sheet: {sheet_name}")
        print(df.head())
