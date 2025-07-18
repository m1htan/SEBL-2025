# Xóa hàng chứa chữ trong các cột country
country_cols = [col for col in df.columns if "country" in col.lower()]
for col in country_cols:
    df = df[~df[col].astype(str).str.contains(r'[a-zA-Z]', na=False)]

# Chuyển các cột thành float nếu có thể
for col in df.columns:
    if df[col].dtype == 'object':
        try:
            df[col] = df[col].str.replace(',', '.', regex=False)
        except:
            pass
    df[col] = pd.to_numeric(df[col], errors='coerce')
