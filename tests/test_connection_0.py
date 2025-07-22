import pyodbc

# --- THÔNG TIN KẾT NỐI ---
SERVER = 'localhost'
DATABASE = 'SEBL-2025'
USERNAME = 'sa'
PASSWORD = 'Minhtan0410@'

# Driver ODBC đã cài đặt
# Windows: '{ODBC Driver 17 for SQL Server}' hoặc '{ODBC Driver 18 for SQL Server}'
# macOS/Linux: '{ODBC Driver 17 for SQL Server}' (hoặc phiên bản khác bạn đã cài)

DRIVER = '{ODBC Driver 17 for SQL Server}'

# Chuỗi kết nối
CONNECTION_STRING = (
    f'DRIVER={DRIVER};'
    f'SERVER={SERVER},1433;'
    f'DATABASE={DATABASE};'
    f'UID={USERNAME};'
    f'PWD={PASSWORD};'
    f'Encrypt=no;'
    f'TrustServerCertificate=yes;'
    f'Connection Timeout=5;'
)

try:
    # Thiết lập kết nối
    cnxn = pyodbc.connect(CONNECTION_STRING)
    cursor = cnxn.cursor()
    print("Kết nối thành công! Đã kết nối đến SQL Server.")

    # Thực hiện một truy vấn đơn giản để xác nhận kết nối hoạt động
    cursor.execute("SELECT @@version;")
    row = cursor.fetchone()
    if row:
        print(f"Phiên bản SQL Server: {row[0]}")

except pyodbc.Error as ex:
    sqlstate = ex.args[0]
    if sqlstate == '28000':
        print("Lỗi xác thực: Tên người dùng hoặc mật khẩu không đúng.")
    elif sqlstate.startswith('08'): # Lỗi kết nối
        print(f"Lỗi kết nối đến SQL Server. Kiểm tra tên server, port, hoặc firewall: {ex}")
    else:
        print(f"Lỗi không xác định khi kết nối: {ex}")