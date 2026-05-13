import pandas as pd

# 1. Đọc dữ liệu và dọn dẹp cơ bản giống bước MAP của Spark
df = pd.read_csv('data/input/Amazon Sale Report.csv', low_memory=False)
df = df.dropna(subset=['Date', 'SKU', 'Style', 'Size', 'ship-state'])
df['ship-state'] = df['ship-state'].str.strip().str.upper()
df['Month'] = pd.to_datetime(df['Date'], format='%m-%d-%y', errors='coerce').dt.strftime('%Y-%m')

# 2. Lọc riêng trường hợp CHANDIGARH tháng 03-2022
sample = df[(df['ship-state'] == 'CHANDIGARH') & (df['Month'] == '2022-03')]

print("="*60)
print("KIỂM CHỨNG THỦ CÔNG: CHANDIGARH, Tháng 03-2022")
print("="*60)

print("\n[1] Bảng dữ liệu gốc sau khi lọc (Filter):")
print(sample[['Date', 'Style', 'SKU', 'Size']].to_string(index=False))

print("\n[2] Phân tích từng Style:")
styles = sample.groupby('Style')
qualifying_varieties = []

for name, group in styles:
    skus = group['SKU'].unique()
    sizes = group['Size'].str.strip().str.upper().unique()
    
    # Kiểm tra xem có size >= XXL không
    xl_sizes = {'XXL', '2XL', 'XXXL', '3XL', '4XL', '5XL', '6XL'}
    has_xxl = any(s in xl_sizes or str(s).endswith('XL') and str(s)[:-2].isdigit() for s in sizes)
    
    # Tính variety (số lượng SKU distinct)
    variety = len(skus)
    
    status = 'HỢP LỆ (Có size >= XXL)' if has_xxl else 'BỎ QUA (Không có XXL)'
    if has_xxl:
        qualifying_varieties.append(variety)
        
    print(f" -> Style: {name}")
    print(f"    + Danh sách SKUs ({variety} SKUs): {list(skus)}")
    print(f"    + Danh sách Sizes: {list(sizes)}")
    print(f"    => {status}\n")

print("[3] Kết quả cuối cùng:")
qualifying_varieties.sort()
print(f" -> Tập các variety hợp lệ để tính Median: {qualifying_varieties}")

if len(qualifying_varieties) > 0:
    median = pd.Series(qualifying_varieties).median()
    print(f" -> MEDIAN VARIETY tính bằng tay: {median:.4f}")
else:
    print(" -> Không có style nào hợp lệ.")
print("="*60)
