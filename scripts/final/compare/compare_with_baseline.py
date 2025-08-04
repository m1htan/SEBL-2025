import pandas as pd
import numpy as np
import os
import seaborn as sns
from matplotlib import pyplot as plt
from scipy.stats import norm

# Đường dẫn đến file
ratio_46_files = {
    "group1": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ratio_46/scaled_score_for_4_part_ratio_country_group1.csv",
    "group2": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ratio_46/scaled_score_for_4_part_ratio_country_group2.csv",
    "group3": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ratio_46/scaled_score_for_4_part_ratio_country_group3.csv",
    "group4": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ratio_46/scaled_score_for_4_part_ratio_country_group4.csv"
}

ai_agent_files = {
    "group1": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group1.csv",
    "group2": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group2.csv",
    "group3": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group3.csv",
    "group4": "/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_6_part_ratio_country_group4.csv"
}

# Thư mục lưu biểu đồ overlay
plot_dir = "/Users/minhtan/Documents/GitHub/SEBL-2025/output/compare/plots"
os.makedirs(plot_dir, exist_ok=True)

output_dir = "/Users/minhtan/Documents/GitHub/SEBL-2025/output/compare"
os.makedirs(output_dir, exist_ok=True)

# -----------------------------
# Vẽ overlay từng group (hist + 2 đường + vùng bóng + quartiles + so sánh vùng)
# -----------------------------
for group in ratio_46_files.keys():
    path_ratio = ratio_46_files[group]
    path_ai = ai_agent_files[group]

    if not os.path.exists(path_ratio) or not os.path.exists(path_ai):
        print(f"[SKIP] Không tìm thấy dữ liệu cho {group}")
        continue

    df_ratio = pd.read_csv(path_ratio)
    df_ai = pd.read_csv(path_ai)

    if "scaled_score" not in df_ratio.columns or "scaled_score" not in df_ai.columns:
        print(f"[SKIP] Thiếu cột scaled_score ở {group}")
        continue

    # Fit các phân phối chuẩn
    mu_ai, std_ai = norm.fit(df_ai['scaled_score'])
    mu_ratio, std_ratio = norm.fit(df_ratio['scaled_score'])

    # Tạo trục x và các phân phối
    x = np.linspace(0, 10, 500)
    p_ai = norm.pdf(x, mu_ai, std_ai)
    p_ratio = norm.pdf(x, mu_ratio, std_ratio)

    # Các mốc phần tư từ RATIO_46 (baseline)
    desc = df_ratio['scaled_score'].describe()
    q25, q50, q75 = desc['25%'], desc['50%'], desc['75%']

    # Phân vùng cho từng điểm
    def get_zone(val):
        if val < q25:
            return "0-Q1"
        elif val < q50:
            return "Q1-Q2"
        elif val < q75:
            return "Q2-Q3"
        else:
            return "Q3-10"

    df_ratio['zone'] = df_ratio['scaled_score'].apply(get_zone)
    df_ai['zone'] = df_ai['scaled_score'].apply(get_zone)

    # -----------------------------
    # Gán vùng phân vị cho từng quốc gia trong AI Agent theo baseline
    # -----------------------------
    if 'splits' in df_ai.columns:
        df_ai_filtered = df_ai[df_ai['splits'] == 6].copy()
    else:
        print(f"[WARNING] Không tìm thấy cột 'splits' trong AI Agent {group}, dùng toàn bộ.")
        df_ai_filtered = df_ai.copy()

    df_country_zone = df_ai_filtered[['country_code', 'scaled_score']].copy()
    df_country_zone['zone'] = df_country_zone['scaled_score'].apply(get_zone)

    # Xoá trùng lặp country_code nếu có
    df_country_zone = df_country_zone.drop_duplicates(subset=['country_code'])

    # Thêm cột baseline_mean_zone và position_vs_baseline cho từng quốc gia
    zone_means = df_ratio.groupby('zone')['scaled_score'].mean().to_dict()

    df_country_zone['baseline_mean_zone'] = df_country_zone['zone'].map(zone_means)
    df_country_zone['position_vs_baseline'] = df_country_zone.apply(
        lambda row: 'above' if row['scaled_score'] > row['baseline_mean_zone'] else 'below',
        axis=1
    )

    # Lưu vào file CSV
    df_country_zone.to_csv(os.path.join(output_dir, f"ai_agent_country_zone_{group}.csv"), index=False)
    print(f"[OK] Đã lưu vùng và so sánh baseline của từng quốc gia AI Agent cho {group}")


    df_country_zone.to_csv(os.path.join(output_dir, f"ai_agent_country_zone_{group}.csv"), index=False)
    print(f"[OK] Đã lưu vùng của từng quốc gia AI Agent cho {group}")

    # Tính chênh lệch trung bình giữa các vùng
    comparison = []
    for zone in ['0-Q1', 'Q1-Q2', 'Q2-Q3', 'Q3-10']:
        mean_ratio = df_ratio[df_ratio['zone'] == zone]['scaled_score'].mean()
        mean_ai = df_ai[df_ai['zone'] == zone]['scaled_score'].mean()
        diff = mean_ai - mean_ratio
        comparison.append((zone, mean_ratio, mean_ai, diff))

    print(f"\n--- So sánh theo vùng cho {group} ---")
    for zone, r, a, d in comparison:
        print(f"{zone}: Ratio46 = {r:.2f}, AI Agent = {a:.2f} → Lệch = {d:.2f}")

    # Vẽ biểu đồ
    plt.figure(figsize=(10, 6))

    # Histogram của baseline
    sns.histplot(df_ratio['scaled_score'], kde=False, stat="density", bins=20, color='lightgray', label='Ratio 46 Histogram')

    # Fitted curves
    plt.plot(x, p_ai, 'r', linewidth=2, label=f'AI Agent Fit (μ={mu_ai:.2f}, σ={std_ai:.2f})')
    plt.plot(x, p_ratio, 'b--', linewidth=2, label=f'Ratio 46 Fit (μ={mu_ratio:.2f}, σ={std_ratio:.2f})')

    # Vùng bóng giữa 2 đường
    plt.fill_between(x, np.minimum(p_ai, p_ratio), np.maximum(p_ai, p_ratio), color='orange', alpha=0.3, label='Difference Area')

    # Tô màu vùng phần tư
    plt.axvspan(0, q25, color='cyan', alpha=0.1, label='0–Q1')
    plt.axvspan(q25, q50, color='yellow', alpha=0.2, label='Q1–Q2')
    plt.axvspan(q50, q75, color='green', alpha=0.2, label='Q2–Q3')
    plt.axvspan(q75, 10, color='purple', alpha=0.1, label='Q3–10')

    # Các đường dọc và chú thích phần tư
    for q, label, ypos in zip([q25, q50, q75], ['Q1 (25%)', 'Q2 (50%)', 'Q3 (75%)'], [0.9, 0.85, 0.8]):
        plt.axvline(q, color='red', linestyle='--', linewidth=1.2)
        plt.text(q + 0.05, plt.ylim()[1] * ypos, f'{label}\n{q:.2f}', color='red', fontsize=9)

    # Labels & legend
    plt.title(f"Overlay Histogram with Quartiles - {group}")
    plt.xlabel("Scaled Score")
    plt.ylabel("Density")
    plt.legend(loc='upper right')
    plt.tight_layout()
    plt.savefig(os.path.join(plot_dir, f"overlay_hist_{group}.png"))
    plt.close()

    print(f"[OK] Đã lưu biểu đồ overlay với phần tư cho {group}")

    # Lưu bảng so sánh ra CSV
    df_comp = pd.DataFrame(comparison, columns=["Zone", "Mean_Ratio46", "Mean_AI_Agent", "Difference"])
    df_comp.to_csv(os.path.join(output_dir, f"comparison_zones_{group}.csv"), index=False)

    print(f"[OK] Đã lưu bảng so sánh vùng cho {group}")