import time
import psycopg2
from app.extensions import get_db_connection
from concurrent.futures import ThreadPoolExecutor
from tabulate import tabulate  

def round_time(elapsed_time, unit="seconds"):
    if unit == "seconds":
        return round(elapsed_time, 2)  
    elif unit == "minutes":
        return round(elapsed_time / 60, 2)  
    return elapsed_time

def measure_query_time(query, params=None):
    print(f"Executing query: {query}")
    connection = get_db_connection()
    cursor = connection.cursor()

    start_time = time.time()
    cursor.execute(query, params)
    connection.commit()  
    end_time = time.time()

    elapsed_time = end_time - start_time
    connection.close()

    return round_time(elapsed_time, "seconds")

def select_query():
    query = "SELECT * FROM items LIMIT 1000;"  
    print("Running SELECT query...")
    return measure_query_time(query)

def insert_query(num_records):
    query = "INSERT INTO items (name, description, price) VALUES (%s, %s, %s);"
    params = [("Item", f"Description for Item {i}", 100) for i in range(num_records)]
    connection = get_db_connection()
    cursor = connection.cursor()

    print(f"Running INSERT query for {num_records} records...")
    start_time = time.time()
    batch_size = 1000  
    for i in range(0, num_records, batch_size):
        cursor.executemany(query, params[i:i+batch_size])  
    connection.commit()
    end_time = time.time()
    connection.close()

    return round_time(end_time - start_time, "seconds")

def update_query(num_records):
    query = "UPDATE items SET price = price + 10 WHERE id = %s;"
    ids = tuple(range(1, num_records + 1))
    
    chunk_size = 10000
    chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    print(f"Running UPDATE query for {num_records} records...")
    def update_chunk(chunk):
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(query, (chunk,))
        connection.commit()
        connection.close()

    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        executor.map(update_chunk, chunks)
    end_time = time.time()

    return round_time(end_time - start_time, "seconds")

def delete_query(num_records):
    query = "DELETE FROM items WHERE id = %s;"
    ids = tuple(range(1, num_records + 1))
    

    chunk_size = 10000
    chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]

    print(f"Running DELETE query for {num_records} records...")
    def delete_chunk(chunk):
        connection = get_db_connection()
        cursor = connection.cursor()
        cursor.execute(query, (chunk,))
        connection.commit()
        connection.close()

    start_time = time.time()
    with ThreadPoolExecutor() as executor:
        executor.map(delete_chunk, chunks)
    end_time = time.time()

    return round_time(end_time - start_time, "seconds")

if name == "main":
    headers = ["Records", "Select Time", "Insert Time", "Update Time", "Delete Time"]
    table_data = []

    for records in [10, 100, 1000, 2000]:
        print(f"\nStarting measurements for {records} records:")
        select_time = select_query()
        insert_time = insert_query(records)
        update_time = update_query(records)
        delete_time = delete_query(records)

        table_data.append([records, select_time, insert_time, update_time, delete_time])

    print("\nResults:")
    print(tabulate(table_data, headers=headers, tablefmt="grid"))
