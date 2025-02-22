import os
import psycopg2
import time

TEST_SIZE = 10000000

# Load database configuration from environment variables (without defaults)
DB_CONFIG = {
    "dbname": os.environ["DB_NAME"],
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "host": os.environ["DB_HOST"],
    "port": os.environ["DB_PORT"]
}

def measure_time(func, cursor, name):
    print(f"running {name}...")
    start_time = time.time()
    result = func(cursor)
    end_time = time.time()
    elapsed_time = end_time - start_time
    return result, elapsed_time

def drop_tables(cursor):
    cursor.execute("""
        DROP TABLE IF EXISTS eav_table CASCADE;
        DROP TABLE IF EXISTS jsonb_table CASCADE;
    """)

def create_tables(cursor):
    cursor.execute("""
        CREATE TABLE eav_table (
            entity_id INT,
            attribute TEXT,
            value TEXT,
            PRIMARY KEY (entity_id, attribute)
        );

        CREATE TABLE jsonb_table (
            entity_id INT PRIMARY KEY,
            attributes JSONB
        );

        CREATE INDEX eav_attr_val_idx ON eav_table (attribute, value);
        CREATE INDEX jsonb_gin_idx ON jsonb_table USING GIN (attributes);
            """)

def insert_eav(cursor):
    cursor.execute(f"""
        INSERT INTO eav_table (entity_id, attribute, value)
        SELECT
            g.i,
            attr.name,
            CASE 
                WHEN attr.name = 'height' THEN (150 + (g.i % 40))::text
                WHEN attr.name = 'weight' THEN (50 + (g.i % 40))::text
                WHEN attr.name = 'salary' THEN (5000 + (g.i % 40) * 1000)::text
                WHEN attr.name = 'age' THEN (20 + (g.i % 40))::text
                WHEN attr.name = 'city' THEN 'city_' || ((g.i % 50)::text)
                WHEN attr.name = 'gender' THEN CASE WHEN (g.i % 2) = 0 THEN 'male' ELSE 'female' END
                WHEN attr.name = 'name' THEN ('user_' || (g.i)::text)
                ELSE '' 
            END
        FROM generate_series(1, {TEST_SIZE}) AS g(i),
            (VALUES ('age'), ('height'), ('weight'), ('salary'), ('city'), ('gender'), ('name')) AS attr(name);
    """)

def insert_jsonb(cursor):
    cursor.execute(f"""
        INSERT INTO jsonb_table (entity_id, attributes)
        SELECT 
            g.i AS entity_id,
            jsonb_build_object(
                'age', (20 + (g.i % 40))::text,
                'height', (150 + (g.i % 40))::text,
                'weight', (50 + (g.i % 40))::text,
                'salary', (5000 + (g.i % 40) * 1000)::text,
                'city', 'city_' || ((g.i % 50)::text),
                'gender', CASE WHEN (g.i % 2) = 0 THEN 'male' ELSE 'female' END,
                'name', ('user_' || (g.i)::text)
            ) AS attributes
        FROM generate_series(1, {TEST_SIZE}) AS g(i);
    """)

def get_storage(cursor):
    cursor.execute("""
        SELECT
            relname AS table_name,
            (pg_total_relation_size(relid))/1024/1024 AS total_size_mb,
            (pg_relation_size(relid))/1024/1024 AS table_size_mb,
            (pg_indexes_size(relid))/1024/1024 AS index_size_mb
        FROM pg_stat_user_tables
        WHERE relname IN ('eav_table', 'jsonb_table');
    """)
    return cursor.fetchall()

def query_eav(cursor):
    cursor.execute("""
        EXPLAIN (ANALYZE)
        SELECT e1.entity_id, 
               e1.value as height, 
               e2.value as weight, 
               e3.value as salary, 
               e4.value AS age, 
               e5.value as city,
               e6.value AS gender,
               e7.value AS name 
        FROM eav_table e1
        JOIN eav_table e2 
        ON e1.entity_id = e2.entity_id AND e2.attribute = 'weight'
        JOIN eav_table e3 
        ON e1.entity_id = e3.entity_id AND e3.attribute = 'salary'
        JOIN eav_table e4 
        ON e1.entity_id = e4.entity_id AND e4.attribute = 'age'
        JOIN eav_table e5 
        ON e1.entity_id = e5.entity_id AND e5.attribute = 'city'
        JOIN eav_table e6 
        ON e1.entity_id = e6.entity_id AND e6.attribute = 'gender'
        JOIN eav_table e7 
        ON e1.entity_id = e7.entity_id AND e7.attribute = 'name'
        WHERE e1.attribute = 'height'
        LIMIT 1000;
    """)
    return cursor.fetchall()

def query_jsonb(cursor):
    cursor.execute("""
    EXPLAIN (ANALYZE)
    SELECT 
        entity_id,
        attributes->>'height' AS height,
        attributes->>'weight' AS weight,
        attributes->>'salary' AS salary,
        attributes->>'age' AS age,
        attributes->>'city' AS city,
        attributes->>'gender' AS gender,
        attributes->>'name' AS name
FROM jsonb_table
LIMIT 1000;
    """)
    return cursor.fetchall()

def count_eav_height_or_weight_range(cursor):
    cursor.execute("""
        EXPLAIN (ANALYZE)
        SELECT COUNT(DISTINCT entity_id)
        FROM eav_table
        WHERE (attribute = 'height' AND value::INT BETWEEN 170 AND 180)
           OR (attribute = 'weight' AND value::INT BETWEEN 70 AND 80);
    """)
    return cursor.fetchall()

def count_jsonb_height_or_weight_range(cursor):
    cursor.execute("""
        EXPLAIN (ANALYZE)
        SELECT COUNT(*)
        FROM jsonb_table
        WHERE (attributes->>'height')::INT BETWEEN 170 AND 180
           OR (attributes->>'weight')::INT BETWEEN 70 AND 80;
    """)
    return cursor.fetchall()


def find_eav_by_non_specific_non_exist_value(cursor):
    cursor.execute("""
        EXPLAIN (ANALYZE)
        SELECT entity_id
        FROM eav_table
        WHERE attribute = 'name' AND value = 'not_exist'
        LIMIT 1000;
    """)
    return cursor.fetchall()

def find_jsonb_by_non_specific_non_exist_value(cursor):
    cursor.execute("""
        EXPLAIN (ANALYZE)
        SELECT entity_id
        FROM jsonb_table
        WHERE attributes @> '{"name":"not_exist"}'
        LIMIT 1000;
    """)
    return cursor.fetchall()

def sort_and_filter_jsonb(cursor):
    cursor.execute("""
        EXPLAIN (ANALYZE)
        SELECT *
        FROM jsonb_table
        WHERE (attributes ->>'age')::INT = 50
        ORDER BY (attributes->>'height')::INT, entity_id
        LIMIT 1000;
    """)
    return cursor.fetchall()

def sort_and_filter_eav(cursor):
    cursor.execute(
        """
    EXPLAIN (ANALYZE)
SELECT e1.entity_id, 
       e1.value AS height, 
       e2.value AS weight, 
       e3.value AS salary, 
       e4.value AS age, 
       e5.value AS city, 
       e6.value AS gender, 
       e7.value AS name
FROM eav_table e1
JOIN eav_table e2 ON e1.entity_id = e2.entity_id AND e2.attribute = 'weight'
JOIN eav_table e3 ON e1.entity_id = e3.entity_id AND e3.attribute = 'salary'
JOIN eav_table e4 ON e1.entity_id = e4.entity_id AND e4.attribute = 'age'
JOIN eav_table e5 ON e1.entity_id = e5.entity_id AND e5.attribute = 'city'
JOIN eav_table e6 ON e1.entity_id = e6.entity_id AND e6.attribute = 'gender'
JOIN eav_table e7 ON e1.entity_id = e7.entity_id AND e7.attribute = 'name'
WHERE e1.attribute = 'height'
AND e4.attribute = 'age' AND e4.value::INT = 50
ORDER BY e1.value::INT , entity_id
LIMIT 1000;
        """
    )

def main():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        cursor = conn.cursor()

        drop_tables(cursor)
        create_tables(cursor)

        _, eav_insert_time = measure_time(insert_eav, cursor, "Insert into EAV Table")
        _, jsonb_insert_time = measure_time(insert_jsonb, cursor, "Insert into JSONB Table")

        cursor.execute('analyze')

        storage_results = get_storage(cursor)

        # The for loop is for warming the cache
        for i in range(0,2):
            eav_result, eav_query_time = measure_time(query_eav, cursor, "Query EAV Table")

            jsonb_result, jsonb_query_time = measure_time(query_jsonb, cursor, "Query JSONB Table")

            eav_height_weight_count, eav_height_weight_time = measure_time(count_eav_height_or_weight_range, cursor, "EAV Height OR Weight Count")
            jsonb_height_weight_count, jsonb_height_weight_time = measure_time(count_jsonb_height_or_weight_range, cursor, "JSONB Height OR Weight Count")

            _, eav_find_time = measure_time(find_eav_by_non_specific_non_exist_value, cursor, "Find EAV by non exist value")
            _, jsonb_find_time = measure_time(find_jsonb_by_non_specific_non_exist_value, cursor, "Find JSONB by non exist value")

            _,eav_sort_time = measure_time(sort_and_filter_eav, cursor, "Sort EAV Table")
            _,jsonb_sort_time = measure_time(sort_and_filter_jsonb, cursor, "Sort JSONB Table")

        print(f"{'Metric':<40} | {'EAV':<15} | {'JSONB':<15}")
        print("-" * 80)
        print(f"{'Storage Size (megabytes)':<40} | {storage_results[0][1]:>10.3f} mb | {storage_results[0][2]:>10.3f} mb")
        print(f"{'Insert Time (seconds)':<40} | {eav_insert_time:>10.3f} se     | {jsonb_insert_time:>10.3f} se")

        print(
            f"{'Top 1000 (milliseconds)':<40} | {eav_query_time * 1000:>10.3f} ms | {jsonb_query_time * 1000:>10.3f} ms")
        print(
            f"{'Count with filters (milliseconds)':<40} | {eav_height_weight_time * 1000:>10.3f} ms | {jsonb_height_weight_time * 1000:>10.3f} ms")
        print(
            f"{'Find by Non-Existing Name (milliseconds)':<40} | {eav_find_time * 1000:>10.3f} ms | {jsonb_find_time * 1000:>10.3f} ms")
        print(
            f"{'Sorting with filter (milliseconds)':<40} | {eav_sort_time * 1000:>10.3f} ms | {jsonb_sort_time * 1000:>10.3f} ms")
        cursor.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
