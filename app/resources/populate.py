import random
from app.extensions import get_db_connection

SIZES = [1000, 10000, 100000, 1000000]

def populate_db(size):
    connection = get_db_connection()
    cursor = connection.cursor()

    for _ in range(size):
        name = f"Item {random.randint(1, 1000)}"
        description = f"Description for {name}"
        price = random.randint(1, 1000)
        
        cursor.execute(
            "INSERT INTO items (name, description, price) VALUES (%s, %s, %s);",
            (name, description, price)
        )

    connection.commit()
    connection.close()
    print(f"Inserted {size} items into the database.")

if __name__ == "__main__":
    for size in SIZES:
        populate_db(size)
