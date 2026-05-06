# scripts/generate_realistic_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_realistic_ecommerce_data(num_clients, num_orders):
    """Генерация данных"""
    
    # Настройки
    np.random.seed(42)
    random.seed(42)
    
    # Генерация клиентов
    
    first_names_en = ['John', 'Sarah', 'Michael', 'Emma', 'James', 
                      'Emily', 'Robert', 'Linda', 'David', 'Lisa']
    last_names_en = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 
                     'Miller', 'Davis', 'Wilson', 'Moore', 'Taylor']
    
    domains = ['mail.ru', 'gmail.com', 'yahoo.com', 'yandex.ru', 'outlook.com']
    countries = ['Russia', 'USA', 'UK', 'Germany']
    
    customers = []
    for i in range(1, num_clients + 1):  # 200 клиентов
        
        first = random.choice(first_names_en)
        last = random.choice(last_names_en)
        country = random.choice(countries)
        
        email = f"{first.lower()}.{last.lower()}{random.randint(1, 999)}@{random.choice(domains)}"
        
        
        reg_days_ago = np.random.exponential(100)  # Больше новых клиентов
        registration_date = datetime.now() - timedelta(days=min(reg_days_ago, 730))
        
        
        updated_at = registration_date + timedelta(days=random.randint(1, 365))
        
        customers.append({
            'customer_id': i,
            'first_name': first,
            'last_name': last,
            'email': email,
            'registration_date': registration_date.strftime('%Y-%m-%d'),
            'country': country,
            'updated_at': updated_at.strftime('%Y-%m-%d'),
            'total_orders': 0  # Будет обновлено позже
        })
    
    # Продукты
    categories = {
        'Electronics': {
            'items': ['iPhone 15', 'Samsung Galaxy', 'MacBook Pro', 'iPad', 'AirPods',
                     'Ноутбук Lenovo', 'Монитор Dell', 'Клавиатура Logitech', 'Мышь Razer',
                     'Наушники Sony', 'Колонка JBL', 'Power Bank', 'SSD Samsung'],
            'price_range': (29.99, 2499.99)
        },
        'Clothing': {
            'items': ['Футболка', 'Джинсы', 'Куртка', 'Платье', 'Рубашка',
                     'Кроссовки Nike', 'Ботинки', 'Шорты', 'Свитер', 'Пальто',
                     'Спортивный костюм', 'Кепка', 'Шарф'],
            'price_range': (9.99, 299.99)
        },
        'Books': {
            'items': ['Python для начинающих', 'SQL Advanced', 'Data Science', 
                     'Machine Learning', 'Deep Learning', 'Алгоритмы', 'Java основы',
                     'JavaScript полное руководство', 'React', 'Docker'],
            'price_range': (19.99, 89.99)
        },
        'Home': {
            'items': ['Лампа', 'Стол', 'Стул', 'Шкаф', 'Кровать',
                     'Диван', 'Ковер', 'Зеркало', 'Шторы', 'Подушка'],
            'price_range': (49.99, 999.99)
        }
    }
    
    products = []
    product_id = 1
    for category, info in categories.items():
        for item_name in info['items']:
            price = round(random.uniform(*info['price_range']), 2)
            days_ago = random.randint(1, 365)
            created_at = datetime.now() - timedelta(days=days_ago)
            
            products.append({
                'product_id': product_id,
                'product_name': item_name,
                'category': category,
                'price': price,
                'created_at': created_at.strftime('%Y-%m-%d')
            })
            product_id += 1
    
    # Заказы
    statuses = ['completed', 'pending', 'cancelled', 'refunded', 'shipped']
    status_weights = [0.65, 0.15, 0.08, 0.05, 0.07]  # 65% completed
    
    orders = []
    for i in range(1, num_orders + 1):  # 2000 заказов
        customer_id = random.randint(1, len(customers))
        product_id = random.randint(1, len(products))
        
        # Сезонность: больше заказов в декабре и ноябре
        month = random.choices(range(1, 13), weights=[1,1,1,1,1,1,1,1,1,1,1.5,2])[0]
        day = random.randint(1, 28)
        order_date = datetime(2024, month, day) if random.random() < 0.7 else \
                     datetime(2023, random.randint(1, 12), random.randint(1, 28))
        
        quantity = np.random.poisson(1) + 1  # Чаще 1-2 товара
        status = random.choices(statuses, weights=status_weights)[0]
        
        product = products[product_id - 1]
        total_amount = round(product['price'] * quantity * random.uniform(0.8, 1.2), 2)
        
        orders.append({
            'order_id': i,
            'customer_id': customer_id,
            'product_id': product_id,
            'quantity': quantity,
            'order_date': order_date.strftime('%Y-%m-%d'),
            'status': status,
            'total_amount': total_amount
        })
        
        # Обновляем счетчик заказов клиента
        if status == 'completed':
            customers[customer_id - 1]['total_orders'] += 1
    
    # Сохраняем
    base_path = '/opt/airflow/data/source/'
    pd.DataFrame(customers).to_csv(f'{base_path}customers.csv', index=False)
    pd.DataFrame(products).to_csv(f'{base_path}products.csv', index=False)
    pd.DataFrame(orders).to_csv(f'{base_path}orders.csv', index=False)
    
    print(f"✅ Generated: {len(customers)} customers, {len(products)} products, {len(orders)} orders")

if __name__ == "__main__":
    num_clients = 5000
    num_orders = 10000
    generate_realistic_ecommerce_data(num_clients, num_orders)