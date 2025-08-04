from config import config_sql_server

def init_db():
    conn = config_sql_server(section='sqlserver')
    return conn

if __name__ == '__main__':
    conn = init_db()

    print("Connect database thành công!")

    conn.close()
    conn = None