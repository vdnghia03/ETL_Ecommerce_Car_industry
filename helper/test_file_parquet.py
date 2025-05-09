# Step 1: Import thư viện cần thiết
import pandas as pd

# Step 2: Đọc file Parquet
# Thay 'path_to_your_file.parquet' bằng đường dẫn tới file Parquet của bạn
file_path = '/home/ubunchuu/Documents/DE/ETL_ERM/Docs/silver_crm_cust_info.pq'
df = pd.read_parquet(file_path)

# Step 3: In dữ liệu và kiểu dữ liệu
print("First 5 rows of the Parquet file:")
print(df.head().to_string())
print("\nData types of each column:")
print(df.dtypes)