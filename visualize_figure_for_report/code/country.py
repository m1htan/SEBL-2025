import pandas as pd
import matplotlib.pyplot as plt
import pycountry
import geopandas as gpd

df = pd.read_csv("/data/metadata/country.csv")
df = df[df['country_code'] != 'EU27']

# ======= BIỂU ĐỒ CỘT =======
# Đếm số lượng quốc gia EU và không EU
counts = df['EU_or_not'].value_counts().sort_index()
labels = ['Non-EU', 'EU']

# Vẽ biểu đồ
plt.figure(figsize=(6, 5))
plt.bar(labels, counts, color=['red', 'blue'])
plt.title('Number of EU vs. non-EU countries')
plt.ylabel('Number of countries')
plt.grid(axis='y', linestyle='--', alpha=0.7)

# Thêm số trên đầu cột
for i, val in enumerate(counts):
    plt.text(i, val + 0.1, str(val), ha='center', va='bottom', fontweight='bold')

plt.tight_layout()
plt.show()

# ======= BIỂU ĐỒ TRÒN =======
eu_count = df['EU_or_not'].value_counts()
labels = ['Non-EU', 'EU']
colors = ['#ff6666', '#66b3ff']

plt.figure(figsize=(6,6))
plt.pie(eu_count, labels=labels, autopct='%1.1f%%', startangle=140, colors=colors, wedgeprops={'edgecolor': 'black'})
plt.title('EU and non-EU country ratio')
plt.tight_layout()
plt.show()

# ======= BẢN ĐỒ CHÂU ÂU =======
# Lấy dữ liệu bản đồ thế giới
shapefile_path = "/Users/minhtan/Documents/GitHub/SEBL-2025/visualize_figure_for_report/code/110m_cultural/ne_110m_admin_0_countries.shp"

world = gpd.read_file(shapefile_path)

# Chuẩn hóa tên quốc gia cho phù hợp với bản đồ
def get_alpha3(code):
    try:
        if code == 'EL':
            return 'GRC'
        elif code == 'UK':
            return 'GBR'
        elif code == 'CY':
            return 'CYP'
        elif code == 'MK':
            return 'MKD'
        elif code == 'RS':
            return 'SRB'
        elif code == 'MD':
            return 'MDA'
        else:
            return pycountry.countries.get(alpha_2=code).alpha_3
    except:
        return None

df['alpha_3'] = df['country_code'].apply(get_alpha3)
# Merge dữ liệu bảng với bản đồ
merged = world.merge(df, left_on='ADM0_A3', right_on='alpha_3', how='left')

# Vẽ bản đồ
fig, ax = plt.subplots(1, 1, figsize=(10, 8))
world.boundary.plot(ax=ax, linewidth=0.8, color='gray')  # bản đồ nền

# Tô màu các nước theo EU_or_not
merged[merged['EU_or_not'].notna()].plot(
    column='EU_or_not',
    ax=ax,
    legend=True,
    cmap='bwr',  # xanh với đỏ
    missing_kwds={'color': 'lightgrey'},
    legend_kwds={'label': "EU Status", 'orientation': "horizontal"}
)

plt.title('Map of Countries in the Survey Dataset')
plt.axis('off')
plt.tight_layout()
plt.show()