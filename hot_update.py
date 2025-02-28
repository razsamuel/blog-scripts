import os
import psycopg2
import time
import pandas as pd

PERCENT_FROM_TABLE_TO_UPDATE = 30
DATA_SIZE = 1000000
# Database connection parameters (modify as needed)
DB_CONFIG = {
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"]
}

# Fillfactor values to test
FILLFACTOR_VALUES = [100, 90, 80, 70]
TABLE_TEMPLATE = "benchmark_fillfactor_{}"

# Establish connection
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True  # Enable autocommit for updates and vacuuming
cur = conn.cursor()

# Create and populate tables with different fillfactor settings
for fillfactor in FILLFACTOR_VALUES:
    table_name = TABLE_TEMPLATE.format(fillfactor)
    cur.execute(f"""  DROP TABLE IF EXISTS {table_name};""")
    cur.execute(f"""
        CREATE TABLE {table_name} (
            id SERIAL PRIMARY KEY,
            name TEXT,
            category TEXT,
            value BIGINT,
            updated_at TIMESTAMP DEFAULT now(),
            total_cost numeric,
            description TEXT,
            status TEXT
        ) WITH (fillfactor = {fillfactor});

        CREATE INDEX idx_status_{table_name} ON {table_name}(status);
        CREATE INDEX idx_category_{table_name} ON {table_name}(category);
    """)

    print(f"Inserting 1 million rows into {table_name}...")
    cur.execute(f"""
        INSERT INTO {table_name} (name, category, value, total_cost, description, status)
        SELECT 
            'Item ' || i, 
            CASE WHEN i % 2 = 0 THEN 'A' ELSE 'B' END, 
            i % {round(100/PERCENT_FROM_TABLE_TO_UPDATE)}, 
            i % 100, 
            'Description for item' || i, 
            CASE WHEN i % 2 = 0 THEN 'active' ELSE 'inactive' END
        FROM generate_series(1, {DATA_SIZE}) AS i;
    """)
    print(f"Data inserted into {table_name}.")

# Perform updates on total_cost for each table and measure HOT updates
results = []
for fillfactor in FILLFACTOR_VALUES:
    table_name = TABLE_TEMPLATE.format(fillfactor)
    print(f"Updating total_cost in {table_name}...")
    start_time = time.time()
    cur.execute(f"UPDATE {table_name} SET total_cost = total_cost + 1 where (value::int) = 0;")
    update_time = time.time() - start_time

    cur.execute(f"""
        SELECT n_tup_hot_upd, n_tup_upd, 
               ROUND(100.0 * n_tup_hot_upd / NULLIF(n_tup_upd, 0), 2) AS hot_update_ratio
        FROM pg_stat_user_tables WHERE relname = '{table_name}';
    """)
    hot_update_stats = cur.fetchone()

    results.append((fillfactor, round(update_time, 2), hot_update_stats[2]))

df = pd.DataFrame(results, columns=["Fill Factor", "Time to Update (s)", "HOT Update Percentage"])
print(df.to_string(index=False))
cur.close()
conn.close()

