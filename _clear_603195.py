import sqlite3
conn = sqlite3.connect('data/stock_cache.db')
cursor = conn.cursor()
cursor.execute("DELETE FROM qualitative_results WHERE stock_code='603195'")
print(f'603195 deleted: {cursor.rowcount} rows')
conn.commit()
conn.close()
