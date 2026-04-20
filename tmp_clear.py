from buffett_analyzer.data_warehouse.database import Database
db = Database()
count = db.execute("DELETE FROM qualitative_results WHERE stock_code='603195'")
print(f'已删除 {count} 条定性缓存')
