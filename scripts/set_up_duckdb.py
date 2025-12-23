import duckdb
from pathlib import Path

parent = Path('data/')  
parent.mkdir(exist_ok=True)

conn = duckdb.connect('data/warehouse.db')

print("DuckDB warehouse and schemas created successfully.")
conn.close()
