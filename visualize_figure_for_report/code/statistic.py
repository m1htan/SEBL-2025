import pandas as pd

# Đọc hai file CSV
df1 = pd.read_csv('/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/ai_agent/scaled_score_for_scale_full_group_country_6_part_ratio.csv')
df2 = pd.read_csv('/Users/minhtan/Documents/GitHub/SEBL-2025/data/final/scale_full_group/scaled_score_for_scale_full_group_country.csv')

# Merge theo cột chung, ví dụ 'id'
df_combined = pd.concat([df1, df2], ignore_index=True).sort_values(by=['group_id', 'file_code'], ascending=[True, True])

# Xuất kết quả ra file mới (nếu cần)
df_combined.to_csv('/Users/minhtan/Documents/GitHub/SEBL-2025/visualize_figure_for_report/output/merged_file.csv', index=False)
df_combined.to_excel('/Users/minhtan/Documents/GitHub/SEBL-2025/visualize_figure_for_report/output/merged_file.xlsx', index=False)

print(df_combined)