import sqlite3

def init_db(db_name):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS azure_spot_prices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unit_price REAL,
            currency_code TEXT,
            region TEXT,
            service_family TEXT,
            service_id TEXT,
            service_name TEXT,
            product_id TEXT,
            product_name TEXT,
            sku_id TEXT,
            sku_name TEXT,
            effective_start_date TEXT,
            retrieved_at TEXT
        )
    ''')
    conn.commit()
    return conn, cursor