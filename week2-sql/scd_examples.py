"""
Slowly Changing Dimension (SCD) Type 2 Implementation
Demonstrates how to handle dimension changes while preserving history

Author: Jason Amar
Date: 2025-10-30
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta

class SCDManager:
    """
    Manages Type 2 Slowly Changing Dimensions
    """
    
    def __init__(self, db_path='data_warehouse.db'):
        """Initialize connection to warehouse"""
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        print(f"✓ Connected to {db_path}\n")
    
    def show_current_customers(self, customer_id=None):
        """Display current customer records"""
        print("="*80)
        print("CURRENT CUSTOMER RECORDS (SCD Type 2)")
        print("="*80 + "\n")
        
        if customer_id:
            query = f"""
                SELECT 
                    customer_key,
                    customer_id,
                    first_name,
                    last_name,
                    customer_tier,
                    valid_from,
                    valid_to,
                    is_current
                FROM dim_customer
                WHERE customer_id = {customer_id}
                ORDER BY valid_from
            """
        else:
            query = """
                SELECT 
                    customer_key,
                    customer_id,
                    first_name,
                    last_name,
                    customer_tier,
                    valid_from,
                    valid_to,
                    is_current
                FROM dim_customer
                WHERE customer_id IN (1, 2, 3, 4, 5)
                ORDER BY customer_id, valid_from
            """
        
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
        print()
    
    def update_customer_tier(self, customer_id, new_tier, effective_date=None):
        """
        Update customer tier using SCD Type 2 logic
        Preserves history by:
        1. Closing out the old record (set valid_to, is_current=0)
        2. Inserting new record with new tier (valid_from = today, is_current=1)
        
        Args:
            customer_id: Natural key of customer
            new_tier: New tier value (Bronze, Silver, Gold, Platinum)
            effective_date: Date the change takes effect (default: today)
        """
        if effective_date is None:
            effective_date = datetime.now().strftime('%Y-%m-%d')
        
        print(f"\n{'='*80}")
        print(f"UPDATING CUSTOMER {customer_id} TIER TO {new_tier}")
        print(f"Effective Date: {effective_date}")
        print(f"{'='*80}\n")
        
        # Step 1: Get current record
        self.cursor.execute("""
            SELECT 
                customer_key,
                customer_id,
                first_name,
                last_name,
                email,
                phone,
                city,
                state,
                customer_tier,
                registration_date,
                valid_from
            FROM dim_customer
            WHERE customer_id = ? AND is_current = 1
        """, (customer_id,))
        
        current_record = self.cursor.fetchone()
        
        if not current_record:
            print(f"❌ Customer {customer_id} not found or has no current record!")
            return
        
        print(f"Current tier: {current_record[8]}")
        print(f"New tier: {new_tier}")
        
        if current_record[8] == new_tier:
            print("⚠️  Tier hasn't changed, no update needed")
            return
        
        # Step 2: Close out current record
        yesterday = (datetime.strptime(effective_date, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
        
        self.cursor.execute("""
            UPDATE dim_customer
            SET valid_to = ?,
                is_current = 0,
                updated_at = ?
            WHERE customer_key = ?
        """, (yesterday, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), current_record[0]))
        
        print(f"✓ Closed old record (customer_key={current_record[0]})")
        print(f"  - Set valid_to = {yesterday}")
        print(f"  - Set is_current = 0")
        
        # Step 3: Insert new record with updated tier
        self.cursor.execute("""
            INSERT INTO dim_customer (
                customer_id, first_name, last_name, email, phone,
                city, state, customer_tier, registration_date,
                valid_from, valid_to, is_current, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1, ?, ?)
        """, (
            current_record[1],  # customer_id
            current_record[2],  # first_name
            current_record[3],  # last_name
            current_record[4],  # email
            current_record[5],  # phone
            current_record[6],  # city
            current_record[7],  # state
            new_tier,           # NEW customer_tier
            current_record[9],  # registration_date
            effective_date,     # valid_from
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),  # created_at
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')   # updated_at
        ))
        
        new_key = self.cursor.lastrowid
        
        print(f"✓ Created new record (customer_key={new_key})")
        print(f"  - Set valid_from = {effective_date}")
        print(f"  - Set customer_tier = {new_tier}")
        print(f"  - Set is_current = 1")
        
        self.conn.commit()
        print("\n✅ SCD Type 2 update completed successfully!")
    
    def demonstrate_scd_type2(self):
        """
        Full demonstration of SCD Type 2 workflow
        """
        print("\n" + "📚"*40)
        print("SLOWLY CHANGING DIMENSION TYPE 2 DEMONSTRATION")
        print("📚"*40 + "\n")
        
        print("Scenario: Customer upgrades from Bronze to Silver tier")
        print("="*80 + "\n")
        
        # Show initial state
        print("BEFORE UPDATE:")
        self.show_current_customers(customer_id=1)
        
        input("Press Enter to upgrade customer 1 from Bronze to Silver...")
        
        # Perform SCD Type 2 update
        self.update_customer_tier(
            customer_id=1,
            new_tier='Silver',
            effective_date='2024-06-15'
        )
        
        # Show after state
        print("\n\nAFTER UPDATE:")
        self.show_current_customers(customer_id=1)
        
        # Show how to query historical data
        print("\n" + "="*80)
        print("QUERYING HISTORICAL DATA")
        print("="*80 + "\n")
        
        print("Example: What was customer 1's tier on June 1, 2024?")
        query = """
            SELECT 
                customer_id,
                first_name || ' ' || last_name AS name,
                customer_tier,
                valid_from,
                valid_to
            FROM dim_customer
            WHERE customer_id = 1
                AND '2024-06-01' BETWEEN valid_from AND valid_to
        """
        df = pd.read_sql_query(query, self.conn)
        print(df.to_string(index=False))
        
        print("\n\nExample: What is customer 1's current tier?")
        query2 = """
            SELECT 
                customer_id,
                first_name || ' ' || last_name AS name,
                customer_tier,
                valid_from,
                valid_to
            FROM dim_customer
            WHERE customer_id = 1
                AND is_current = 1
        """
        df2 = pd.read_sql_query(query2, self.conn)
        print(df2.to_string(index=False))
        
        print("\n\n" + "="*80)
        print("KEY BENEFITS OF SCD TYPE 2:")
        print("="*80)
        print("  ✓ Complete history preserved")
        print("  ✓ Can answer 'what was the tier on date X?'")
        print("  ✓ Accurate point-in-time reporting")
        print("  ✓ Audit trail for compliance")
        print("  ✓ Fact table references remain valid (surrogate keys don't change)")
        print("="*80)
    
    def compare_scd_types(self):
        """
        Show comparison of different SCD types
        """
        print("\n" + "="*80)
        print("COMPARISON OF SCD TYPES")
        print("="*80 + "\n")
        
        print("TYPE 1: OVERWRITE (No History)")
        print("-"*80)
        print("Before: | customer_id | name  | tier   |")
        print("        | 1           | Alice | Bronze |")
        print("\nAfter:  | customer_id | name  | tier   |")
        print("        | 1           | Alice | Silver |  ← Bronze overwritten!")
        print("\n✓ Pros: Simple, saves space")
        print("✗ Cons: History lost, can't do point-in-time analysis")
        
        print("\n\nTYPE 2: ADD ROW (Full History) ← WE USE THIS")
        print("-"*80)
        print("Before: | key | customer_id | name  | tier   | valid_from | valid_to   | current |")
        print("        | 1   | 1           | Alice | Bronze | 2024-01-01 | 9999-12-31 | True    |")
        print("\nAfter:  | key | customer_id | name  | tier   | valid_from | valid_to   | current |")
        print("        | 1   | 1           | Alice | Bronze | 2024-01-01 | 2024-06-14 | False   |")
        print("        | 2   | 1           | Alice | Silver | 2024-06-15 | 9999-12-31 | True    |")
        print("\n✓ Pros: Complete history, point-in-time queries, audit trail")
        print("✗ Cons: More complex, uses more space, queries need is_current filter")
        
        print("\n\nTYPE 3: ADD COLUMN (Limited History)")
        print("-"*80)
        print("Before: | customer_id | name  | current_tier | previous_tier |")
        print("        | 1           | Alice | Bronze       | NULL          |")
        print("\nAfter:  | customer_id | name  | current_tier | previous_tier |")
        print("        | 1           | Alice | Silver       | Bronze        |")
        print("\n✓ Pros: Simple, one row per entity, limited history tracking")
        print("✗ Cons: Only 1 previous value, can't answer 'what was tier 2 years ago?'")
        
        print("\n" + "="*80)
    
    def close(self):
        """Close connection"""
        self.conn.close()
        print("\n✓ Database connection closed")

def main():
    """Main execution"""
    scd = SCDManager()
    
    print("="*80)
    print("SLOWLY CHANGING DIMENSIONS (SCD) DEMONSTRATION")
    print("="*80)
    print("\nChoose an option:")
    print("  1. Full SCD Type 2 demonstration (recommended)")
    print("  2. Show current customer records")
    print("  3. Update a customer tier (manual)")
    print("  4. Compare SCD types (educational)")
    print("="*80)
    
    choice = input("\nEnter choice (1-4) or press Enter for option 1: ").strip()
    
    if not choice or choice == '1':
        scd.demonstrate_scd_type2()
    elif choice == '2':
        scd.show_current_customers()
    elif choice == '3':
        customer_id = int(input("Enter customer_id: "))
        new_tier = input("Enter new tier (Bronze/Silver/Gold/Platinum): ")
        scd.update_customer_tier(customer_id, new_tier)
        scd.show_current_customers(customer_id)
    elif choice == '4':
        scd.compare_scd_types()
    else:
        print("Invalid choice!")
    
    scd.close()

if __name__ == "__main__":
    main()