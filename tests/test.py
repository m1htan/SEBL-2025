# 3. Vẽ Q-Q plot
sm.qqplot(z_scores, line='s')
plt.title("Q-Q Plot of Z-score (Total_Group_1)")
plt.grid(True)
plt.show()

# 4. Kiểm định Shapiro-Wilk
stat_shapiro, p_shapiro = shapiro(z_scores)
print(f"Shapiro-Wilk test: Statistic = {stat_shapiro:.4f}, p-value = {p_shapiro:.4f}")
if p_shapiro > 0.05:
    print("Dữ liệu có thể là phân phối chuẩn (không bác bỏ H0).")
else:
    print("Dữ liệu không phải là phân phối chuẩn (bác bỏ H0).")

# 5. Kiểm định D’Agostino-Pearson
stat_normal, p_normal = normaltest(z_scores)
print(f"D’Agostino-Pearson test: Statistic = {stat_normal:.4f}, p-value = {p_normal:.4f}")
if p_normal > 0.05:
    print("Dữ liệu có thể là phân phối chuẩn.")
else:
    print("Dữ liệu không có phân phối chuẩn.")