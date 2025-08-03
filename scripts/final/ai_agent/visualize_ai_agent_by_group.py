import numpy as np
import pandas as pd
import os
import seaborn as sns
from matplotlib import pyplot as plt
from scipy.stats import norm, shapiro, normaltest
import statsmodels.api as sm

# Danh sách file và tên biến tương ứng
csv_files = {
    "group1": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group1.csv",
    "group2": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group2.csv",
    "group3": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group3.csv",
    "group4": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group4.csv"
}

output_plot_dir = "/Users/minhtan/Documents/GitHub/SEBL-2025/output/ai_agent/by_group/plots"
os.makedirs(output_plot_dir, exist_ok=True)

output_stats_dir = "/Users/minhtan/Documents/GitHub/SEBL-2025/output/ai_agent/by_group/descriptive_statistics"
os.makedirs(output_stats_dir, exist_ok=True)

summary_stats = []

# Hàm xử lý từng file
for group, path in csv_files.items():
    if not os.path.exists(path):
        print(f"[SKIP] Không tìm thấy file: {path}")
        continue

    df = pd.read_csv(path)

    if 'scaled_score' not in df.columns:
        print(f"[SKIP] Cột 'scaled_score' không tồn tại trong {group}")
        continue

    print(f"\nThống kê mô tả cho {group}")
    desc = df['scaled_score'].describe()
    print(desc)

    desc.to_csv(os.path.join(output_stats_dir, f"ai_agent_by_group_{group}_describe.csv"), header=True)

    # Biểu đồ histogram + fitted normal
    plt.figure(figsize=(8, 5))
    sns.histplot(df['scaled_score'], kde=True, stat="density", bins=20, color='skyblue', label='Histogram')
    mu, std = norm.fit(df['scaled_score'])
    xmin, xmax = plt.xlim()
    x = np.linspace(xmin, xmax, 100)
    p = norm.pdf(x, mu, std)
    plt.plot(x, p, 'r', linewidth=2, label='Normal Fit')
    plt.title(f"{group} - Histogram + Normal Fit (μ={mu:.2f}, σ={std:.2f})")
    plt.xlabel("Scaled Score")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_plot_dir, f"ai_agent_by_group_{group}_hist.png"))
    plt.close()

    # QQ Plot
    sm.qqplot(df['scaled_score'], line='s')
    plt.title(f"{group} - QQ Plot")
    plt.tight_layout()
    plt.savefig(os.path.join(output_plot_dir, f"ai_agent_by_group_{group}_qq.png"))
    plt.close()

    # Test phân phối chuẩn
    stat1, p1 = shapiro(df['scaled_score'])
    stat2, p2 = normaltest(df['scaled_score'])

    print(f"Shapiro-Wilk test: statistic={stat1:.4f}, p-value={p1:.4f}")
    print(f"D’Agostino-Pearson test: statistic={stat2:.4f}, p-value={p2:.4f}")

    is_normal = (p1 > 0.05) and (p2 > 0.05)
    normal_msg = "Có thể giả định phân phối chuẩn." if is_normal else "KHÔNG tuân theo phân phối chuẩn."
    print(normal_msg)

    # Ghi kết quả kiểm định ra dict
    summary_stats.append({
        "group": group,
        "count": desc["count"],
        "mean": desc["mean"],
        "std": desc["std"],
        "min": desc["min"],
        "25%": desc["25%"],
        "50%": desc["50%"],
        "75%": desc["75%"],
        "max": desc["max"],
        "shapiro_stat": stat1,
        "shapiro_p": p1,
        "dagostino_stat": stat2,
        "dagostino_p": p2,
        "is_normal": is_normal
    })

# Lưu bảng tổng hợp tất cả nhóm
summary_df = pd.DataFrame(summary_stats)
summary_df.to_csv(os.path.join(output_stats_dir, "ai_agent_by_group_summary_all_groups.csv"), index=False)

print(f"\nĐã lưu tất cả thống kê mô tả và kiểm định vào thư mục: {output_stats_dir}")
