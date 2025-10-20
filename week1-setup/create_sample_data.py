"""
Generate sample data for file format testing
Run this ONCE to create test files
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

def generate_sample_data(num_rows=10000):
    """
    Generate realistic e-commerce transaction data
    
    Args:
        num_rows: Number of rows to generate
    
    Returns:
        pandas DataFrame
    """
    print(f"Generating {num_rows:,} rows of sample data...")
    
    # Generate transaction IDs
    transaction_ids = [f"TXN{str(i).zfill(8)}" for i in range(1, num_rows + 1)]
    
    # Generate dates (last 365 days)
    start_date = datetime.now() - timedelta(days=365)
    dates = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(num_rows)]
    
    # Generate customer IDs (5000 unique customers)
    customer_ids = [f"CUST{random.randint(1, 5000):05d}" for _ in range(num_rows)]
    
    # Product categories
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys']
    product_categories = [random.choice(categories) for _ in range(num_rows)]
    
    # Generate prices (realistic distribution)
    prices = np.random.lognormal(mean=3.5, sigma=1.2, size=num_rows).round(2)
    
    # Generate quantities (mostly 1-3, occasionally higher)
    quantities = np.random.choice([1, 2, 3, 4, 5], size=num_rows, p=[0.5, 0.25, 0.15, 0.07, 0.03])
    
    # Calculate total amount
    total_amounts = (prices * quantities).round(2)
    
    # Payment methods
    payment_methods = np.random.choice(
        ['Credit Card', 'Debit Card', 'PayPal', 'Apple Pay'], 
        size=num_rows, 
        p=[0.45, 0.30, 0.15, 0.10]
    )
    
    # Order status
    statuses = np.random.choice(
        ['Completed', 'Pending', 'Cancelled', 'Returned'], 
        size=num_rows, 
        p=[0.80, 0.10, 0.05, 0.05]
    )
    
    # Create DataFrame
    df = pd.DataFrame({
        'transaction_id': transaction_ids,
        'transaction_date': dates,
        'customer_id': customer_ids,
        'product_category': product_categories,
        'unit_price': prices,
        'quantity': quantities,
        'total_amount': total_amounts,
        'payment_method': payment_methods,
        'order_status': statuses
    })
    
    print("✅ Sample data generated successfully!")
    return df

if __name__ == "__main__":
    # Generate data
    df = generate_sample_data(num_rows=10000)
    
    # Display preview
    print("\n" + "="*60)
    print("DATA PREVIEW")
    print("="*60)
    print(df.head(10))
    print(f"\nShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print(f"\nMemory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
    
    # Save in all three formats
    print("\n" + "="*60)
    print("SAVING FILES")
    print("="*60)
    
    # CSV
    print("Saving CSV...")
    df.to_csv('test_data/sample_data.csv', index=False)
    print("✅ CSV saved")
    
    # JSON
    print("Saving JSON...")
    df.to_json('test_data/sample_data.json', orient='records', date_format='iso')
    print("✅ JSON saved")
    
    # Parquet
    print("Saving Parquet...")
    df.to_parquet('test_data/sample_data.parquet', index=False, engine='pyarrow')
    print("✅ Parquet saved")
    
    print("\n🎉 All files created in test_data/ folder!")