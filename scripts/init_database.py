import psycopg2
import pandas as pd
import os

def init_database():
    """Инициализация БД и загрузка тестовых данных"""
    
    conn = psycopg2.connect(
        host="postgres-dbt",
        database="dbt",
        user="dbt",
        password="dbt"
    )
    cur = conn.cursor()
    conn.autocommit = False 
    
    try:
        # Создаем схемы
        schemas = ['raw']
        for schema in schemas:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
        
        # Создаем таблицы с PRIMARY KEY
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.customers (
                customer_id INTEGER PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                email VARCHAR(100),
                registration_date DATE,
                country VARCHAR(50),
                updated_at DATE,
                total_orders INTEGER DEFAULT 0
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.products (
                product_id INTEGER PRIMARY KEY,
                product_name VARCHAR(100),
                category VARCHAR(50),
                price DECIMAL(10,2),
                created_at DATE
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw.orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER REFERENCES raw.customers(customer_id),
                product_id INTEGER REFERENCES raw.products(product_id),
                quantity INTEGER,
                order_date DATE,
                status VARCHAR(20),
                total_amount DECIMAL(10,2)
            );
        """)
        
        # Очищаем таблицы (CASCADE для внешних ключей)
        cur.execute("TRUNCATE TABLE raw.orders, raw.customers, raw.products RESTART IDENTITY CASCADE;")
        
        # Загружаем данные
        data_path = "/opt/airflow/data/source/"
        
        # Загрузка customers
        customers_df = pd.read_csv(os.path.join(data_path, "customers.csv"), skipinitialspace=True)
        for _, row in customers_df.iterrows():
            cur.execute("""
                INSERT INTO raw.customers (customer_id, first_name, last_name, email, 
                                          registration_date, country, updated_at, total_orders)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (row['customer_id'], row['first_name'], row['last_name'], 
                  row['email'], row['registration_date'], row['country'], 
                  row['updated_at'], row['total_orders']))
        
        # Загрузка products
        products_df = pd.read_csv(os.path.join(data_path, "products.csv"))
        for _, row in products_df.iterrows():
            cur.execute("""
                INSERT INTO raw.products (product_id, product_name, category, price, created_at)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['product_id'], row['product_name'], row['category'],
                  row['price'], row['created_at']))
        
        # Загрузка orders
        orders_df = pd.read_csv(os.path.join(data_path, "orders.csv"))
        for _, row in orders_df.iterrows():
            cur.execute("""
                INSERT INTO raw.orders (order_id, customer_id, product_id, quantity, 
                                       order_date, status, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (row['order_id'], row['customer_id'], row['product_id'],
                  row['quantity'], row['order_date'], row['status'], row['total_amount']))
        
        conn.commit()
        print("✅ Database initialized successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    init_database()