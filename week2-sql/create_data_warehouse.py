"""
Data Warehouse ETL - Transform OLTP to OLAP Star Schema
Loads data from operational database into dimensional model

Author: Jason Amar
Date: 2025-10-30
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

class DataWarehouseETL:
    """
    ETL process to build star schema data warehouse
    """
    
    def __init__(self, source_db='sales_analytics.db', target_db='data_warehouse.db'):
        """Initialize connections to source and target databases"""

        self.source_conn = None
        self.target_conn = None

        if not Path(source_db).exists():
            print(f"❌ Source database not found: {source_db}")
            print("Please run setup_database.py first!")
            raise FileNotFoundError(f"Source database not found: {source_db}")

        self.source_conn = sqlite3.connect(source_db)
        self.target_conn = sqlite3.connect(target_db)

        print(f"✓ Connected to source: {source_db}")
        print(f"✓ Connected to target: {target_db}\n")
    
    def create_warehouse_schema(self):
        """Create dimensional model schema"""
        print("="*80)
        print("CREATING DATA WAREHOUSE SCHEMA")
        print("="*80 + "\n")
        
        # Read and execute the data modeling SQL
        with open('data_modeling.sql', 'r') as f:
            sql_script = f.read()
        
        # Execute all statements
        self.target_conn.executescript(sql_script)
        self.target_conn.commit()
        
        print("✓ Dimension tables created")
        print("✓ Fact table created")
        print("✓ Indexes created")
        print("✓ Views created\n")
    
    def load_dim_date(self, start_date='2023-01-01', end_date='2025-12-31'):
        """
        Load date dimension with all dates in range
        This is a type 0 dimension (never changes)
        """
        print("Loading dim_date...")
        
        # Check if already loaded
        cursor = self.target_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_date")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"  ⚠ dim_date already has {count} rows, skipping...")
            return
        
        # Generate date range
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start
        
        while current <= end:
            date_key = int(current.strftime('%Y%m%d'))
            
            dates.append({
                'date_key': date_key,
                'full_date': current.strftime('%Y-%m-%d'),
                'year': current.year,
                'quarter': (current.month - 1) // 3 + 1,
                'month': current.month,
                'month_name': current.strftime('%B'),
                'week_of_year': current.isocalendar()[1],
                'day_of_year': current.timetuple().tm_yday,
                'day_of_month': current.day,
                'day_of_week': current.weekday(),  # 0=Monday in Python
                'day_name': current.strftime('%A'),
                'is_weekend': 1 if current.weekday() >= 5 else 0,
                'is_holiday': 0,  # Could enhance with holiday logic
                'fiscal_year': current.year if current.month >= 7 else current.year - 1,
                'fiscal_quarter': ((current.month - 7) % 12) // 3 + 1
            })
            
            current += timedelta(days=1)
        
        # Bulk insert
        df = pd.DataFrame(dates)
        df.to_sql('dim_date', self.target_conn, if_exists='append', index=False)
        
        print(f"  ✓ Loaded {len(dates)} dates from {start_date} to {end_date}\n")
    
    def load_dim_customer(self):
        """
        Load customer dimension (Type 2 SCD)
        For simplicity, initial load creates one current record per customer
        """
        print("Loading dim_customer...")
        
        # Check if already loaded
        cursor = self.target_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_customer")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"  ⚠ dim_customer already has {count} rows, skipping...")
            return
        
        # Extract from source
        query = """
            SELECT 
                customer_id,
                first_name,
                last_name,
                email,
                phone,
                city,
                state,
                customer_tier,
                registration_date
            FROM customers
        """
        
        df = pd.read_sql_query(query, self.source_conn)
        
        # Add SCD Type 2 columns
        df['valid_from'] = df['registration_date']
        df['valid_to'] = '9999-12-31'
        df['is_current'] = 1
        df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Load to warehouse
        df.to_sql('dim_customer', self.target_conn, if_exists='append', index=False)
        
        print(f"  ✓ Loaded {len(df)} customers\n")
    
    def load_dim_product(self):
        """Load product dimension (denormalized with category)"""
        print("Loading dim_product...")
        
        # Check if already loaded
        cursor = self.target_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM dim_product")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"  ⚠ dim_product already has {count} rows, skipping...")
            return
        
        # Extract from source (join products and categories)
        query = """
            SELECT 
                p.product_id,
                p.product_name,
                p.category_id,
                c.category_name,
                c.description AS category_description,
                p.unit_price,
                p.is_active
            FROM products p
            JOIN categories c ON p.category_id = c.category_id
        """
        
        df = pd.read_sql_query(query, self.source_conn)
        
        # Add metadata
        df['created_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Load to warehouse
        df.to_sql('dim_product', self.target_conn, if_exists='append', index=False)
        
        print(f"  ✓ Loaded {len(df)} products\n")
    
    def load_dim_order_status(self):
        """Load order status dimension"""
        print("Loading dim_order_status...")
        
        statuses = [
            {'status_code': 'PENDING', 'status_name': 'Pending', 
             'status_description': 'Order placed, awaiting processing', 'is_final_state': 0},
            {'status_code': 'SHIPPED', 'status_name': 'Shipped', 
             'status_description': 'Order shipped to customer', 'is_final_state': 0},
            {'status_code': 'COMPLETED', 'status_name': 'Completed', 
             'status_description': 'Order delivered and completed', 'is_final_state': 1},
            {'status_code': 'CANCELLED', 'status_name': 'Cancelled', 
             'status_description': 'Order cancelled', 'is_final_state': 1},
        ]
        
        df = pd.DataFrame(statuses)
        df.to_sql('dim_order_status', self.target_conn, if_exists='replace', index=False)
        
        print(f"  ✓ Loaded {len(df)} order statuses\n")
    
    def load_dim_payment_method(self):
        """Load payment method dimension"""
        print("Loading dim_payment_method...")
        
        methods = [
            {'payment_method_code': 'CC', 'payment_method_name': 'Credit Card', 'processing_fee_pct': 2.9},
            {'payment_method_code': 'DC', 'payment_method_name': 'Debit Card', 'processing_fee_pct': 1.5},
            {'payment_method_code': 'PP', 'payment_method_name': 'PayPal', 'processing_fee_pct': 3.5},
            {'payment_method_code': 'AP', 'payment_method_name': 'Apple Pay', 'processing_fee_pct': 2.5},
        ]
        
        df = pd.DataFrame(methods)
        df.to_sql('dim_payment_method', self.target_conn, if_exists='replace', index=False)
        
        print(f"  ✓ Loaded {len(df)} payment methods\n")
    
    def load_fact_sales(self):
        """
        Load fact table from source
        This is the main ETL transformation
        """
        print("Loading fact_sales...")
        
        # Check if already loaded
        cursor = self.target_conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM fact_sales")
        count = cursor.fetchone()[0]
        
        if count > 0:
            print(f"  ⚠ fact_sales already has {count} rows, skipping...")
            return
        
        # Extract from source (denormalized query)
        query = """
            SELECT 
                o.order_id,
                oi.order_item_id,
                o.order_date,
                o.customer_id,
                oi.product_id,
                o.order_status,
                -- Infer payment method from patterns (simplified)
                CASE 
                    WHEN o.order_id % 4 = 0 THEN 'Credit Card'
                    WHEN o.order_id % 4 = 1 THEN 'Debit Card'
                    WHEN o.order_id % 4 = 2 THEN 'PayPal'
                    ELSE 'Apple Pay'
                END AS payment_method,
                oi.quantity,
                oi.unit_price,
                oi.line_total,
                0.0 AS discount_amount,  -- Could enhance with discount logic
                oi.line_total * 0.08 AS tax_amount  -- 8% tax
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            WHERE o.order_status = 'Completed'
        """
        
        df_source = pd.read_sql_query(query, self.source_conn)
        
        print(f"  Extracted {len(df_source)} records from source")
        
        # Transform: Get surrogate keys from dimensions
        
        # 1. Get date keys
        df_source['date_key'] = df_source['order_date'].apply(
            lambda x: int(x.replace('-', ''))
        )
        
        # 2. Get customer keys (join with dim_customer)
        dim_customer = pd.read_sql_query(
            "SELECT customer_key, customer_id FROM dim_customer WHERE is_current = 1",
            self.target_conn
        )
        df_source = df_source.merge(dim_customer, on='customer_id', how='left')
        
        # 3. Get product keys
        dim_product = pd.read_sql_query(
            "SELECT product_key, product_id FROM dim_product",
            self.target_conn
        )
        df_source = df_source.merge(dim_product, on='product_id', how='left')
        
        # 4. Get status keys
        status_mapping = {
            'Completed': 3,  # Based on our dim_order_status
            'Pending': 1,
            'Shipped': 2,
            'Cancelled': 4
        }
        df_source['status_key'] = df_source['order_status'].map(status_mapping)
        
        # 5. Get payment method keys
        payment_mapping = {
            'Credit Card': 1,
            'Debit Card': 2,
            'PayPal': 3,
            'Apple Pay': 4
        }
        df_source['payment_method_key'] = df_source['payment_method'].map(payment_mapping)
        
        # Calculate net revenue
        df_source['net_revenue'] = (
            df_source['line_total'] - 
            df_source['discount_amount'] + 
            df_source['tax_amount']
        )
        
        # Select columns for fact table
        fact_columns = [
            'date_key', 'customer_key', 'product_key', 'status_key', 'payment_method_key',
            'order_id', 'order_item_id',
            'quantity', 'unit_price', 'line_total', 
            'discount_amount', 'tax_amount', 'net_revenue'
        ]
        
        df_fact = df_source[fact_columns].copy()
        
        # Load to warehouse
        df_fact.to_sql('fact_sales', self.target_conn, if_exists='append', index=False)
        
        print(f"  ✓ Loaded {len(df_fact)} sales transactions\n")
    
    def run_full_etl(self):
        """Execute complete ETL process"""
        print("\n" + "🏗️"*40)
        print("DATA WAREHOUSE ETL - OLTP TO OLAP TRANSFORMATION")
        print("🏗️"*40 + "\n")
        
        try:
            # Step 1: Create schema
            self.create_warehouse_schema()
            
            # Step 2: Load dimensions (order matters!)
            self.load_dim_date()
            self.load_dim_customer()
            self.load_dim_product()
            self.load_dim_order_status()
            self.load_dim_payment_method()
            
            # Step 3: Load facts (must come after dimensions)
            self.load_fact_sales()
            
            # Step 4: Verify
            self.verify_warehouse()
            
            print("\n" + "="*80)
            print("✅ DATA WAREHOUSE ETL COMPLETED SUCCESSFULLY!")
            print("="*80)
            
        except Exception as e:
            print(f"\n❌ ETL Failed: {e}")
            import traceback
            traceback.print_exc()
    
    def verify_warehouse(self):
        """Verify warehouse was loaded correctly"""
        print("="*80)
        print("VERIFYING DATA WAREHOUSE")
        print("="*80 + "\n")
        
        tables = [
            'dim_date',
            'dim_customer',
            'dim_product',
            'dim_order_status',
            'dim_payment_method',
            'fact_sales'
        ]
        
        for table in tables:
            cursor = self.target_conn.cursor()
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table:25} {count:>10,} rows")
        
        print()
    
    def run_sample_queries(self):
        """Run sample analytical queries on the warehouse"""
        print("\n" + "="*80)
        print("SAMPLE ANALYTICAL QUERIES")
        print("="*80 + "\n")
        
        # Query 1: Monthly revenue by category
        print("Query 1: Monthly Revenue by Category (Last 6 months)")
        print("-"*80)
        query1 = """
            SELECT 
                d.year,
                d.month_name,
                p.category_name,
                SUM(f.net_revenue) AS revenue,
                SUM(f.quantity) AS units_sold,
                COUNT(DISTINCT f.customer_key) AS unique_customers
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            JOIN dim_product p ON f.product_key = p.product_key
            WHERE d.full_date >= date('now', '-6 months')
            GROUP BY d.year, d.month, d.month_name, p.category_name
            ORDER BY d.year, d.month, revenue DESC
            LIMIT 15
        """
        df1 = pd.read_sql_query(query1, self.target_conn)
        print(df1.to_string(index=False))
        
        # Query 2: Customer tier performance
        print("\n\nQuery 2: Revenue by Customer Tier")
        print("-"*80)
        query2 = """
            SELECT 
                c.customer_tier,
                COUNT(DISTINCT c.customer_key) AS customers,
                COUNT(DISTINCT f.order_id) AS orders,
                ROUND(SUM(f.net_revenue), 2) AS total_revenue,
                ROUND(AVG(f.net_revenue), 2) AS avg_transaction,
                SUM(f.quantity) AS total_units
            FROM fact_sales f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            WHERE c.is_current = 1
            GROUP BY c.customer_tier
            ORDER BY total_revenue DESC
        """
        df2 = pd.read_sql_query(query2, self.target_conn)
        print(df2.to_string(index=False))
        
        # Query 3: Weekend vs weekday sales
        print("\n\nQuery 3: Weekend vs Weekday Sales Pattern")
        print("-"*80)
        query3 = """
            SELECT 
                CASE WHEN d.is_weekend = 1 THEN 'Weekend' ELSE 'Weekday' END AS day_type,
                COUNT(*) AS transactions,
                ROUND(SUM(f.net_revenue), 2) AS revenue,
                ROUND(AVG(f.net_revenue), 2) AS avg_transaction,
                SUM(f.quantity) AS units_sold
            FROM fact_sales f
            JOIN dim_date d ON f.date_key = d.date_key
            GROUP BY d.is_weekend
        """
        df3 = pd.read_sql_query(query3, self.target_conn)
        print(df3.to_string(index=False))
        
        # Query 4: Top products by revenue
        print("\n\nQuery 4: Top 10 Products by Revenue")
        print("-"*80)
        query4 = """
            SELECT 
                p.product_name,
                p.category_name,
                SUM(f.quantity) AS units_sold,
                ROUND(SUM(f.net_revenue), 2) AS total_revenue,
                ROUND(AVG(f.unit_price), 2) AS avg_price
            FROM fact_sales f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.product_key, p.product_name, p.category_name
            ORDER BY total_revenue DESC
            LIMIT 10
        """
        df4 = pd.read_sql_query(query4, self.target_conn)
        print(df4.to_string(index=False))
        
        print("\n" + "="*80)
    
    def close(self):
        """Close database connections"""
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()
        print("\n✓ Database connections closed")

def main():
    """Main execution"""
    etl = DataWarehouseETL()
    
    print("="*80)
    print("DATA WAREHOUSE ETL OPTIONS")
    print("="*80)
    print("\nChoose an option:")
    print("  1. Run full ETL (create warehouse and load all data)")
    print("  2. Verify warehouse (check row counts)")
    print("  3. Run sample analytical queries")
    print("="*80)
    
    choice = input("\nEnter choice (1-3) or press Enter for option 1: ").strip()
    
    if not choice or choice == '1':
        etl.run_full_etl()
        input("\nPress Enter to run sample queries...")
        etl.run_sample_queries()
    elif choice == '2':
        etl.verify_warehouse()
    elif choice == '3':
        etl.run_sample_queries()
    else:
        print("Invalid choice!")
    
    etl.close()

if __name__ == "__main__":
    main()