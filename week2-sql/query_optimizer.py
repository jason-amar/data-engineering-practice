"""
SQL Query Performance Optimizer and Tester
Benchmarks query performance, creates indexes, and analyzes execution plans

Author: Jason Amar
Date: 2025-10-27
"""

import sqlite3
import time
import pandas as pd
from pathlib import Path
from typing import Tuple, Dict, List

class QueryOptimizer:
    """
    Tools for analyzing and optimizing SQL query performance
    """
    
    def __init__(self, db_path='sales_analytics.db'):
        """Initialize with database connection"""
        if not Path(db_path).exists():
            print(f"❌ Database not found: {db_path}")
            print("Please run setup_database.py first!")
            return
        
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()
        print(f"✓ Connected to {db_path}\n")
    
    def benchmark_query(self, query: str, query_name: str, iterations: int = 5) -> Dict:
        """
        Benchmark a query's execution time
        
        Args:
            query: SQL query to benchmark
            query_name: Descriptive name
            iterations: Number of times to run query
        
        Returns:
            Dictionary with timing statistics
        """
        print(f"\n{'='*80}")
        print(f"BENCHMARKING: {query_name}")
        print(f"{'='*80}")
        
        times = []
        
        for i in range(iterations):
            start = time.time()
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            elapsed = time.time() - start
            times.append(elapsed)
            print(f"  Run {i+1}: {elapsed:.4f}s ({len(results)} rows)")
        
        avg_time = sum(times) / len(times)
        min_time = min(times)
        max_time = max(times)
        
        print(f"\n  Average: {avg_time:.4f}s")
        print(f"  Min: {min_time:.4f}s")
        print(f"  Max: {max_time:.4f}s")
        
        return {
            'query_name': query_name,
            'avg_time': avg_time,
            'min_time': min_time,
            'max_time': max_time,
            'iterations': iterations
        }
    
    def explain_query(self, query: str, query_name: str = "Query"):
        """
        Show query execution plan
        
        Args:
            query: SQL query to explain
            query_name: Descriptive name
        """
        print(f"\n{'='*80}")
        print(f"EXECUTION PLAN: {query_name}")
        print(f"{'='*80}\n")
        
        explain_query = f"EXPLAIN QUERY PLAN {query}"
        
        self.cursor.execute(explain_query)
        plan = self.cursor.fetchall()
        
        print("Query Execution Steps:")
        print("-" * 80)
        for row in plan:
            # SQLite EXPLAIN QUERY PLAN returns: (id, parent, notused, detail)
            print(f"  {row[3]}")
        
        print("\n" + "="*80)
    
    def list_indexes(self):
        """List all indexes in the database"""
        print(f"\n{'='*80}")
        print("CURRENT INDEXES")
        print(f"{'='*80}\n")
        
        query = """
            SELECT 
                name AS index_name,
                tbl_name AS table_name,
                sql AS create_statement
            FROM sqlite_master
            WHERE type = 'index' 
                AND name NOT LIKE 'sqlite_%'
            ORDER BY tbl_name, name
        """
        
        df = pd.read_sql_query(query, self.conn)
        
        if len(df) == 0:
            print("  No user-defined indexes found.")
        else:
            print(df.to_string(index=False))
        
        print()
    
    def create_performance_indexes(self):
        """Create indexes for optimal query performance"""
        print(f"\n{'='*80}")
        print("CREATING PERFORMANCE INDEXES")
        print(f"{'='*80}\n")
        
        indexes = [
            ("idx_customers_email", "customers(email)"),
            ("idx_customers_city_state", "customers(city, state)"),
            ("idx_customers_tier", "customers(customer_tier)"),
            ("idx_orders_customer_id", "orders(customer_id)"),
            ("idx_orders_order_date", "orders(order_date)"),
            ("idx_orders_status", "orders(order_status)"),
            ("idx_order_items_order_id", "order_items(order_id)"),
            ("idx_order_items_product_id", "order_items(product_id)"),
            ("idx_products_category_id", "products(category_id)"),
        ]
        
        created = 0
        skipped = 0
        
        for idx_name, idx_definition in indexes:
            try:
                query = f"CREATE INDEX IF NOT EXISTS {idx_name} ON {idx_definition}"
                self.cursor.execute(query)
                print(f"  ✓ Created: {idx_name} on {idx_definition}")
                created += 1
            except sqlite3.Error as e:
                print(f"  ⚠ Skipped: {idx_name} ({e})")
                skipped += 1
        
        self.conn.commit()
        
        print(f"\n  Total created: {created}")
        print(f"  Total skipped: {skipped}")
        print("="*80)
    
    def drop_all_indexes(self):
        """Drop all user-created indexes (for testing)"""
        print(f"\n{'='*80}")
        print("DROPPING ALL USER INDEXES")
        print(f"{'='*80}\n")
        
        # Get all user indexes
        self.cursor.execute("""
            SELECT name 
            FROM sqlite_master 
            WHERE type = 'index' 
                AND name NOT LIKE 'sqlite_%'
        """)
        
        indexes = self.cursor.fetchall()
        
        if not indexes:
            print("  No user indexes to drop.")
            return
        
        for (idx_name,) in indexes:
            try:
                self.cursor.execute(f"DROP INDEX IF EXISTS {idx_name}")
                print(f"  ✓ Dropped: {idx_name}")
            except sqlite3.Error as e:
                print(f"  ⚠ Error dropping {idx_name}: {e}")
        
        self.conn.commit()
        print("="*80)
    
    def run_performance_tests(self):
        """Run comprehensive performance tests"""
        print("\n" + "🚀"*40)
        print("SQL PERFORMANCE OPTIMIZATION SUITE")
        print("🚀"*40)
        
        # Test queries
        test_queries = {
            "Customer Email Lookup": """
                SELECT * FROM customers 
                WHERE email = 'emily.johnson100@email.com'
            """,
            
            "Customer Orders Join": """
                SELECT 
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    COUNT(o.order_id) AS order_count,
                    ROUND(SUM(o.total_amount), 2) AS total_spent
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                WHERE o.order_status = 'Completed'
                GROUP BY c.customer_id, c.first_name, c.last_name
                HAVING COUNT(o.order_id) >= 3
                ORDER BY total_spent DESC
                LIMIT 20
            """,
            
            "Date Range Query": """
                SELECT 
                    strftime('%Y-%m', order_date) AS month,
                    COUNT(*) AS orders,
                    ROUND(SUM(total_amount), 2) AS revenue
                FROM orders
                WHERE order_date >= date('now', '-6 months')
                    AND order_status = 'Completed'
                GROUP BY strftime('%Y-%m', order_date)
                ORDER BY month
            """,
            
            "Multi-Table Join": """
                SELECT 
                    cat.category_name,
                    p.product_name,
                    COUNT(DISTINCT o.customer_id) AS customers,
                    SUM(oi.quantity) AS units_sold,
                    ROUND(SUM(oi.line_total), 2) AS revenue
                FROM categories cat
                JOIN products p ON cat.category_id = p.category_id
                JOIN order_items oi ON p.product_id = oi.product_id
                JOIN orders o ON oi.order_id = o.order_id
                WHERE o.order_status = 'Completed'
                GROUP BY cat.category_name, p.product_name
                ORDER BY revenue DESC
                LIMIT 10
            """
        }
        
        print("\n" + "="*80)
        print("PHASE 1: PERFORMANCE WITHOUT INDEXES")
        print("="*80)
        
        # Drop indexes
        self.drop_all_indexes()
        
        # Run benchmarks without indexes
        results_no_index = []
        for name, query in test_queries.items():
            result = self.benchmark_query(query, name, iterations=3)
            results_no_index.append(result)
            self.explain_query(query, name)
        
        input("\nPress Enter to create indexes and re-test...")
        
        print("\n" + "="*80)
        print("PHASE 2: CREATING INDEXES")
        print("="*80)
        
        # Create indexes
        self.create_performance_indexes()
        
        # Run ANALYZE to update statistics
        print("\nRunning ANALYZE to update query optimizer statistics...")
        self.cursor.execute("ANALYZE")
        self.conn.commit()
        print("✓ Statistics updated")
        
        print("\n" + "="*80)
        print("PHASE 3: PERFORMANCE WITH INDEXES")
        print("="*80)
        
        # Run benchmarks with indexes
        results_with_index = []
        for name, query in test_queries.items():
            result = self.benchmark_query(query, name, iterations=3)
            results_with_index.append(result)
            self.explain_query(query, name)
        
        # Compare results
        print("\n" + "="*80)
        print("PERFORMANCE COMPARISON")
        print("="*80 + "\n")
        
        comparison_data = []
        for no_idx, with_idx in zip(results_no_index, results_with_index):
            speedup = no_idx['avg_time'] / with_idx['avg_time']
            improvement_pct = ((no_idx['avg_time'] - with_idx['avg_time']) / no_idx['avg_time']) * 100
            
            comparison_data.append({
                'Query': no_idx['query_name'],
                'Without Index': f"{no_idx['avg_time']:.4f}s",
                'With Index': f"{with_idx['avg_time']:.4f}s",
                'Speedup': f"{speedup:.2f}x",
                'Improvement': f"{improvement_pct:.1f}%"
            })
        
        df = pd.DataFrame(comparison_data)
        print(df.to_string(index=False))
        
        print("\n" + "="*80)
        print("✅ PERFORMANCE TESTING COMPLETE!")
        print("="*80)
        print("\nKey Takeaways:")
        print("  ✓ Indexes dramatically improve query performance")
        print("  ✓ Foreign key indexes speed up JOINs")
        print("  ✓ Date indexes help range queries")
        print("  ✓ Always run EXPLAIN QUERY PLAN to understand execution")
        print("  ✓ Use ANALYZE to update optimizer statistics")
        print("="*80)
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        print("\n✓ Database connection closed")

def main():
    """Main execution"""
    optimizer = QueryOptimizer('sales_analytics.db')
    
    print("="*80)
    print("SQL PERFORMANCE OPTIMIZATION TOOLKIT")
    print("="*80)
    print("\nChoose an option:")
    print("  1. Run full performance test suite (recommended)")
    print("  2. List current indexes")
    print("  3. Create performance indexes")
    print("  4. Drop all indexes")
    print("  5. Explain a specific query")
    print("="*80)
    
    choice = input("\nEnter choice (1-5) or press Enter for option 1: ").strip()
    
    if not choice or choice == '1':
        optimizer.run_performance_tests()
    elif choice == '2':
        optimizer.list_indexes()
    elif choice == '3':
        optimizer.create_performance_indexes()
        optimizer.list_indexes()
    elif choice == '4':
        confirm = input("Are you sure? This will drop all indexes (y/n): ")
        if confirm.lower() == 'y':
            optimizer.drop_all_indexes()
    elif choice == '5':
        query = input("Enter SQL query: ")
        optimizer.explain_query(query, "Custom Query")
    else:
        print("Invalid choice!")
    
    optimizer.close()

if __name__ == "__main__":
    main()