import pandas as pd
import os
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
from scipy.stats import norm, zscore, shapiro, normaltest
import statsmodels.api as sm
import seaborn as sns
import matplotlib.pyplot as plt
import scipy.stats as stats

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
# Bỏ cột 'EU27' nếu có
if 'EU27' in merged_df.columns:
    merged_df.drop(columns='EU27', inplace=True)
# Lọc các hàng KHÔNG chứa '_percentage'
df_no_pct = merged_df[~merged_df.index.str.contains('_percentage')].copy()
# Tính z-score cho từng hàng (axis=1)
df_zscore = df_no_pct.apply(lambda row: pd.Series(zscore(row, nan_policy='omit'), index=row.index), axis=1)
# Cộng điểm z-score theo cột (mỗi quốc gia)
df_zscore_sum = df_zscore.sum(axis=0).to_frame().T
df_zscore_sum.index = ['Total_ZScore_Sum']
# Chuẩn hóa hàng tổng đó (z-score theo chiều ngang)
scaled_total = zscore(df_zscore_sum.values.flatten(), nan_policy='omit')
df_scaled_total = pd.DataFrame([scaled_total], columns=df_zscore.columns, index=['Scaled_Total_Group_1'])

# In kết quả
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
print("\n>> Hàng tổng đã scale lại theo z-score:")
print(df_scaled_total)



data = df_scaled_total.loc['Scaled_Total_Group_1'].values
plt.figure(figsize=(12, 5))

# Shapiro-Wilk Test
shapiro_stat, shapiro_p = stats.shapiro(data)
print(f"[Shapiro-Wilk] Statistic={shapiro_stat:.4f}, p-value={shapiro_p:.4f}")

# D’Agostino and Pearson’s Test
dagostino_stat, dagostino_p = stats.normaltest(data)
print(f"[D’Agostino & Pearson] Statistic={dagostino_stat:.4f}, p-value={dagostino_p:.4f}")

# Anderson-Darling Test
anderson_result = stats.anderson(data, dist='norm')
print("[Anderson-Darling]")
print(f"Statistic: {anderson_result.statistic:.4f}")
for i, (cv, sig) in enumerate(zip(anderson_result.critical_values, anderson_result.significance_level)):
    print(f"  - {sig}%: critical value = {cv:.4f}")

# Vẽ biểu đồ phân phối (hist + KDE)
plt.figure(figsize=(10, 5))
sns.histplot(data, kde=True, color='skyblue', edgecolor='black')
plt.title("Phân phối chuẩn hóa (Z-score) của Scaled_Total_Group_1")
plt.xlabel("Z-score")
plt.ylabel("Tần suất")
plt.grid(True)
plt.tight_layout()
plt.show()

# Q-Q plot (kiểm tra phân phối chuẩn)
plt.subplot(1, 2, 2)
stats.probplot(data, dist="norm", plot=plt)
plt.title("Q-Q Plot - Tổng Z-score")
plt.tight_layout()
plt.show()
