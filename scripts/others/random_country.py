import csv
import random

# Danh sách 38 quốc gia ví dụ
countries = [
    "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czechia",
    "Denmark", "Estonia", "Finland", "France", "Germany", "Greece",
    "Hungary", "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg",
    "Malta", "Netherlands", "Poland", "Portugal", "Romania", "Slovakia",
    "Slovenia", "Spain", "Sweden",

    "United Kingdom", "Iceland", "Norway", "Switzerland", "Montenegro",
    "Moldova", "North Macedonia", "Albania", "Serbia", "Türkiye", "United States"]

# Shuffle ngẫu nhiên rồi chia theo 4/10 và 6/10
random.shuffle(countries)
group_4_10 = countries[:15]
group_6_10 = countries[15:]

# In kết quả
print("Nhóm 4/10 (15 quốc gia):")
for country in group_4_10:
    print("-", country)

print("\nNhóm 6/10 (23 quốc gia):")
for country in group_6_10:
    print("-", country)

# Lưu vào CSV
with open("/data/metadata/splits_country.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Group", "Country"])
    for country in group_4_10:
        writer.writerow(["4", country])
    for country in group_6_10:
        writer.writerow(["6", country])

print("Đã lưu kết quả vào file country_groups.csv.")