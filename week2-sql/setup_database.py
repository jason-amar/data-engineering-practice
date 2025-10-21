"""
Create SQLite database with realistic sales data for SQL practice

Author: Jason Amar
Date: 2025-10-20
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

def create_database():
    """Create SQLite database with multiple related tables"""

    print("Creating sales_analytics.db...")
    conn = sqlite3.connect('sales_analytics.db')
    cursor = conn.cursor()

    # Drop tables if they exist
    cursor.execute("DROP TABLE IF EXISTS order_items")
    cursor.execute("DROP TABLE IF EXISTS orders")
    cursor.execute("DROP TABLE IF EXISTS customers")
    cursor.execute("DROP TABLE IF EXISTS products")
    cursor.execute("DROP TABLE IF EXISTS categories")
    
    print("✓ Cleaned existing tables")

    # 1. CATEGORIES TABLE
    cursor.execute("""
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL,
            description TEXT
        )
    """)
    
    categories_data = [
        (1, 'Electronics', 'Electronic devices and accessories'),
        (2, 'Clothing', 'Apparel and fashion items'),
        (3, 'Home & Garden', 'Home improvement and gardening'),
        (4, 'Sports', 'Sports equipment and gear'),
        (5, 'Books', 'Books and educational materials'),
        (6, 'Toys', 'Toys and games')
    ]
    
    cursor.executemany(
        "INSERT INTO categories VALUES (?, ?, ?)",
        categories_data
    )
    print("✓ Created categories table (6 categories)")
    
    # 2. PRODUCTS TABLE
    cursor.execute("""
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category_id INTEGER,
            unit_price REAL NOT NULL,
            stock_quantity INTEGER,
            is_active BOOLEAN DEFAULT 1,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
    """)
    
    products = []
    product_names = {
        1: ['Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Smartwatch', 'Camera'],
        2: ['T-Shirt', 'Jeans', 'Sneakers', 'Jacket', 'Dress', 'Hoodie'],
        3: ['Lamp', 'Chair', 'Plant Pot', 'Rug', 'Mirror', 'Vase'],
        4: ['Basketball', 'Yoga Mat', 'Dumbbells', 'Tennis Racket', 'Soccer Ball', 'Running Shoes'],
        5: ['Novel', 'Cookbook', 'Biography', 'Textbook', 'Magazine', 'Comic Book'],
        6: ['Action Figure', 'Board Game', 'Puzzle', 'Doll', 'LEGO Set', 'Video Game']
    }
    
    product_id = 1
    for cat_id, names in product_names.items():
        for name in names:
            price = round(random.uniform(9.99, 299.99), 2)
            stock = random.randint(0, 500)
            is_active = 1 if stock > 0 else random.choice([0, 1])
            products.append((product_id, name, cat_id, price, stock, is_active))
            product_id += 1
    
    cursor.executemany(
        "INSERT INTO products VALUES (?, ?, ?, ?, ?, ?)",
        products
    )
    print(f"✓ Created products table ({len(products)} products)")
    
    # 3. CUSTOMERS TABLE
    cursor.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT UNIQUE NOT NULL,
            phone TEXT,
            city TEXT,
            state TEXT,
            registration_date DATE,
            customer_tier TEXT DEFAULT 'Bronze'
        )
    """)
    
    customers = []
    customer_tiers = ['Bronze', 'Silver', 'Gold', 'Platinum']
    
    for i in range(1, 501):  # 500 customers
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"
        phone = fake.phone_number()[:15]
        city = fake.city()
        state = fake.state_abbr()
        reg_date = fake.date_between(start_date='-3y', end_date='today')
        tier = random.choices(customer_tiers, weights=[50, 30, 15, 5])[0]
        
        customers.append((i, first_name, last_name, email, phone, city, state, reg_date, tier))
    
    cursor.executemany(
        "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        customers
    )
    print(f"✓ Created customers table (500 customers)")
    
    # 4. ORDERS TABLE
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER NOT NULL,
            order_date DATE NOT NULL,
            ship_date DATE,
            order_status TEXT DEFAULT 'Pending',
            total_amount REAL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
    """)
    
    orders = []
    order_statuses = ['Completed', 'Pending', 'Shipped', 'Cancelled']
    
    for order_id in range(1, 2001):  # 2000 orders
        customer_id = random.randint(1, 500)
        order_date = fake.date_between(start_date='-1y', end_date='today')
        
        # Ship date is 1-7 days after order (if not cancelled)
        status = random.choices(order_statuses, weights=[70, 15, 10, 5])[0]
        
        if status in ['Shipped', 'Completed']:
            ship_date = order_date + timedelta(days=random.randint(1, 7))
        else:
            ship_date = None
        
        # Total amount will be calculated from order_items
        orders.append((order_id, customer_id, order_date, ship_date, status, 0))
    
    cursor.executemany(
        "INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?)",
        orders
    )
    print(f"✓ Created orders table (2000 orders)")
    
    # 5. ORDER ITEMS TABLE (junction table)
    cursor.execute("""
        CREATE TABLE order_items (
            order_item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """)
    
    order_items = []
    order_item_id = 1
    
    # Get product prices for reference
    cursor.execute("SELECT product_id, unit_price FROM products")
    product_prices = dict(cursor.fetchall())
    
    for order_id in range(1, 2001):
        # Each order has 1-5 items
        num_items = random.randint(1, 5)
        order_total = 0
        
        for _ in range(num_items):
            product_id = random.randint(1, len(products))
            quantity = random.randint(1, 5)
            unit_price = product_prices[product_id]
            line_total = round(quantity * unit_price, 2)
            order_total += line_total
            
            order_items.append((order_item_id, order_id, product_id, quantity, unit_price, line_total))
            order_item_id += 1
        
        # Update order total
        cursor.execute(
            "UPDATE orders SET total_amount = ? WHERE order_id = ?",
            (round(order_total, 2), order_id)
        )
    
    cursor.executemany(
        "INSERT INTO order_items VALUES (?, ?, ?, ?, ?, ?)",
        order_items
    )
    print(f"✓ Created order_items table ({len(order_items)} line items)")
    
    conn.commit()
    conn.close()
    
    print("\n" + "="*60)
    print("✅ DATABASE CREATED SUCCESSFULLY!")
    print("="*60)
    print(f"Database: sales_analytics.db")
    print(f"Tables: categories, products, customers, orders, order_items")
    print(f"Total records: {6 + len(products) + 500 + 2000 + len(order_items):,}")
    print("="*60)

def display_sample_data():
    """Display sample data from each table"""
    conn = sqlite3.connect('sales_analytics.db')
    
    tables = ['categories', 'products', 'customers', 'orders', 'order_items']
    
    print("\n" + "="*60)
    print("SAMPLE DATA PREVIEW")
    print("="*60)
    
    for table in tables:
        print(f"\n--- {table.upper()} (first 3 rows) ---")
        df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 3", conn)
        print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    create_database()
    display_sample_data()