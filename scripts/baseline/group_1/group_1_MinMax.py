import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm
import statsmodels.api as sm
from scipy import stats

# Thư mục chứa các file CSV
csv_dir = "/Users/minhtan/Documents/GitHub/SEBL-2025/data/data_clean/volume_A"
# Danh sách tên file
csv_files = [f"Q{i}_(b).csv" for i in range(1, 9)]

# Danh sách DataFrame
dfs = []

for file_name in csv_files:
    path = os.path.join(csv_dir, file_name)
    try:
        df = pd.read_csv(path)

        # Đảm bảo cột đầu tiên là chuỗi
        df.iloc[:, 0] = df.iloc[:, 0].astype(str)

        # Loại bỏ hàng rỗng hoặc chỉ toàn NaN
        df.dropna(how="all", inplace=True)

        # Ép các cột còn lại về float
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        dfs.append(df)

    except Exception as e:
        print(f"[ERROR] Lỗi khi đọc {file_name}: {e}")

# Gộp các DataFrame lại
merged_df = pd.concat(dfs, ignore_index=True)


# Gán cột tiêu chí làm index
merged_df.set_index('Unnamed: 0', inplace=True)
# Bỏ cột 'euro27' nếu tồn tại
if 'EU27' in merged_df.columns:
    merged_df.drop(columns='EU27', inplace=True)
# Lọc các hàng KHÔNG chứa '_percentage'
df_no_pct = merged_df[~merged_df.index.str.contains('_percentage')].copy()
# Scale theo từng hàng (axis=1)
df_scaled = df_no_pct.apply(lambda row: 1 + 9 * ((row - row.min()) / (row.max() - row.min())), axis=1)
# Reset index để giữ lại tên tiêu chí
df_scaled = df_scaled.reset_index()
# Scale lại hàng tổng
df_total_scaled = df_scaled.tail(1).copy()
df_total_scaled.iloc[:, 1:] = df_total_scaled.iloc[:, 1:].apply(
    lambda row: 1 + 9 * ((row - row.min()) / (row.max() - row.min())),
    axis=1
)
# Đặt lại tên chỉ mục cho rõ ràng
df_total_scaled.iloc[0, 0] = 'Scaled_Total_Group_1'

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
print(df_total_scaled)
print(df_total_scaled.index)

# Vẽ biểu đồ cột
df_total_scaled.set_index('Unnamed: 0', inplace=True)
# Lấy giá trị hàng Total_Group_1
row = df_total_scaled.loc['Scaled_Total_Group_1']
row_sorted = row.sort_values()


# Shapiro-Wilk Test
shapiro_stat, shapiro_p = stats.shapiro(row_sorted)
print(f"[Shapiro-Wilk] Statistic={shapiro_stat:.4f}, p-value={shapiro_p:.4f}")

# D’Agostino and Pearson’s Test
dagostino_stat, dagostino_p = stats.normaltest(row_sorted)
print(f"[D’Agostino & Pearson] Statistic={dagostino_stat:.4f}, p-value={dagostino_p:.4f}")

# Anderson-Darling Test
anderson_result = stats.anderson(row_sorted, dist='norm')
print("[Anderson-Darling]")
print(f"Statistic: {anderson_result.statistic:.4f}")
for i, (cv, sig) in enumerate(zip(anderson_result.critical_values, anderson_result.significance_level)):
    print(f"  - {sig}%: critical value = {cv:.4f}")

# Vẽ biểu đồ cột
plt.figure(figsize=(12, 6))
row_sorted.plot(kind='bar', color='skyblue', edgecolor='black')

plt.title("Biểu đồ cột - Giá trị đã chuẩn hóa của Scaled_Total_Group_1")
plt.xlabel("Quốc gia / Đối tượng")
plt.ylabel("Giá trị đã chuẩn hóa (1-10)")
plt.xticks(rotation=45, ha='right')
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.tight_layout()
plt.show()

plt.figure(figsize=(10, 6))
sns.histplot(row_sorted, bins=10, kde=True, stat='density', color='skyblue', edgecolor='black')
mean, std = np.mean(row_sorted), np.std(row_sorted)
xmin, xmax = plt.xlim()
x = np.linspace(xmin, xmax, 100)
p = norm.pdf(x, mean, std)
plt.plot(x, p, 'r--', linewidth=2, label='Normal Distribution')
plt.title("Phân phối của Scaled_Total_Group_1 (Min-Max Scaled)")
plt.xlabel("Giá trị (1-10)")
plt.ylabel("Mật độ")
plt.legend()
plt.grid(True)
plt.show()

# Q-Q plot (kiểm tra phân phối chuẩn)
plt.subplot(1, 2, 2)
stats.probplot(row_sorted, dist="norm", plot=plt)
plt.title("Q-Q Plot - Tổng Z-score")
plt.tight_layout()
plt.show()