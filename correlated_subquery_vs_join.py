import psycopg2
import time
import os
# Database connection parameters
DB_CONFIG = {
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"]
}

# Connect to PostgreSQL
conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = True
cur = conn.cursor()

# Step 1: Create Tables
cur.execute("""
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS customers;

    CREATE TABLE customers (
        id SERIAL PRIMARY KEY,
        name TEXT,
        email TEXT
    );

    CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        customer_id INT REFERENCES customers(id),
        order_date DATE,
        amount NUMERIC
    );
""")
print("Tables created.")

# Step 2: Insert Sample Data
print("Inserting 1 million customers...")
cur.execute("""
    INSERT INTO customers (name, email)
    SELECT
        'Customer ' || i,
        'customer' || i || '@example.com'
    FROM generate_series(1, 1000000) AS i;
""")

print("Inserting 10 million orders...")
cur.execute("""
    INSERT INTO orders (customer_id, order_date, amount)
    SELECT
        floor(random() * 1000000 + 1)::INT,  -- Random customer_id
        NOW() - (random() * INTERVAL '365 days'),
        round((random() * 1000)::numeric, 2)  -- Random amount up to 1000
    FROM generate_series(1, 10000000);
""")
print("Data inserted.")

# Step 3: Benchmark Queries
def run_query(query):
    """Executes and times a query."""
    start_time = time.time()
    cur.execute(query)
    result = cur.fetchall()
    end_time = time.time()
    return round(end_time - start_time, 2), result

# Correlated Subquery
print("Running Correlated Subquery...")
correlated_query = """
    EXPLAIN ANALYZE
    SELECT
        o.id,
        o.order_date,
        o.amount,
        (SELECT c.name FROM customers c WHERE c.id = o.customer_id) AS customer_name
    FROM orders o;
"""
correlated_time, correlated_result = run_query(correlated_query)
print(f"Correlated Subquery Execution Time: {correlated_time} seconds")

# LEFT JOIN Query
print("Running LEFT JOIN Query...")
join_query = """
    EXPLAIN ANALYZE
    SELECT
        o.id,
        o.order_date,
        o.amount,
        c.name AS customer_name
    FROM orders o
    LEFT JOIN customers c ON o.customer_id = c.id;
"""
join_time, join_result = run_query(join_query)
print(f"LEFT JOIN Execution Time: {join_time} seconds")

# Step 4: Display Results
print("\nBenchmark Results:")
print(f"Correlated Subquery: {correlated_time} seconds")
print(f"LEFT JOIN: {join_time} seconds")

# Close connections
cur.close()
conn.close()