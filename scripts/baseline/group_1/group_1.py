import pandas as pd
import os

csv_dir = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/cleaned_data/volume_A"

OUTPUT_DIR = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/merged_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)
output_path = os.path.join(OUTPUT_DIR, f"Group_1.csv")

# Danh sách tên file
csv_files = [f"Q{i}.csv" for i in range(1, 9)]

# Danh sách DataFrame
dfs = []

for file_name in csv_files:
    path = os.path.join(csv_dir, file_name)
    try:
        df = pd.read_csv(path)

        # Đảm bảo cột đầu tiên là chuỗi
        df.iloc[:, 0] = df.iloc[:, 0].astype(str)

        if "file_code" in df.columns:
            df["file_code"] = df["file_code"].astype(str)
            file_code_col = df.pop("file_code")
            df.insert(1, "file_code", file_code_col)

        # Loại bỏ hàng rỗng hoặc chỉ toàn NaN
        df.dropna(how="all", inplace=True)

        # Ép các cột còn lại về float
        numeric_cols = df.columns.difference(["file_code", "criteria"])
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        dfs.append(df)

    except Exception as e:
        print(f"[ERROR] Lỗi khi đọc {file_name}: {e}")

# Gộp các DataFrame lại
merged_df = pd.concat(dfs, ignore_index=True)

print(merged_df)
merged_df.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"[PROCESS] Đã lưu dataframe thành file: {output_path}")