"""
SQL Query Executor and Results Display
Runs advanced SQL queries and displays results

Author: Jason Amar
Date: 2025-10-21
"""

import sqlite3
import pandas as pd
from pathlib import Path

class SQLExercises:
    """
    Execute and display SQL query results
    """
    
    def __init__(self, db_path='sales_analytics.db'):
        """Initialize with database connection"""
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        print(f"✓ Connected to {db_path}")
    
    def run_query(self, query: str, query_name: str = "Query"):
        """
        Execute SQL query and display results
        
        Args:
            query: SQL query string
            query_name: Descriptive name for the query
        """
        print("\n" + "="*80)
        print(f"{query_name}")
        print("="*80)
        print(f"\nSQL:\n{query}\n")
        print("-"*80)
        
        try:
            df = pd.read_sql_query(query, self.conn)
            print(f"Results ({len(df)} rows):\n")
            print(df.to_string(index=False))
            return df
        except Exception as e:
            print(f"❌ Error: {e}")
            return None

    def run_all_queries(self):
        """Run all advanced SQL queries"""
        
        # Query 1: Top customers by spending
        self.run_query("""
            SELECT 
                c.customer_id,
                c.first_name || ' ' || c.last_name AS customer_name,
                c.customer_tier,
                ROUND(SUM(o.total_amount), 2) AS total_spent,
                ROW_NUMBER() OVER (ORDER BY SUM(o.total_amount) DESC) AS spending_rank
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            WHERE o.order_status = 'Completed'
            GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_tier
            ORDER BY total_spent DESC
            LIMIT 10
        """, "Query 1: Top 10 Customers by Spending (Window Functions)")
        
        # Query 2: Category performance
        self.run_query("""
            SELECT 
                c.category_name,
                COUNT(DISTINCT o.order_id) AS orders,
                SUM(oi.quantity) AS units_sold,
                ROUND(SUM(oi.line_total), 2) AS revenue,
                ROUND(AVG(oi.unit_price), 2) AS avg_price
            FROM categories c
            JOIN products p ON c.category_id = p.category_id
            JOIN order_items oi ON p.product_id = oi.product_id
            JOIN orders o ON oi.order_id = o.order_id
            WHERE o.order_status = 'Completed'
            GROUP BY c.category_name
            ORDER BY revenue DESC
        """, "Query 2: Category Performance Summary")
        
        # Query 3: Monthly revenue trend with growth
        self.run_query("""
            SELECT 
                strftime('%Y-%m', order_date) AS month,
                COUNT(*) AS order_count,
                ROUND(SUM(total_amount), 2) AS monthly_revenue,
                LAG(ROUND(SUM(total_amount), 2)) OVER (ORDER BY strftime('%Y-%m', order_date)) AS prev_month,
                ROUND(
                    (SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY strftime('%Y-%m', order_date))) 
                    / LAG(SUM(total_amount)) OVER (ORDER BY strftime('%Y-%m', order_date)) * 100, 
                    2
                ) AS growth_pct
            FROM orders
            WHERE order_status = 'Completed'
            GROUP BY strftime('%Y-%m', order_date)
            ORDER BY month
            LIMIT 12
        """, "Query 3: Monthly Revenue Trend with MoM Growth (LAG function)")
        
        # Query 4: Customer lifetime value with CTE
        self.run_query("""
            WITH customer_stats AS (
                SELECT 
                    customer_id,
                    COUNT(*) AS order_count,
                    ROUND(SUM(total_amount), 2) AS lifetime_value,
                    ROUND(AVG(total_amount), 2) AS avg_order_value,
                    MIN(order_date) AS first_order,
                    MAX(order_date) AS last_order
                FROM orders
                WHERE order_status = 'Completed'
                GROUP BY customer_id
            )
            SELECT 
                c.first_name || ' ' || c.last_name AS customer_name,
                c.customer_tier,
                cs.order_count,
                cs.lifetime_value,
                cs.avg_order_value,
                JULIANDAY(cs.last_order) - JULIANDAY(cs.first_order) AS days_as_customer
            FROM customer_stats cs
            JOIN customers c ON cs.customer_id = c.customer_id
            WHERE cs.order_count >= 3
            ORDER BY cs.lifetime_value DESC
            LIMIT 15
        """, "Query 4: Customer Lifetime Value Analysis (CTE)")
        
        # Query 5: Product popularity vs category average
        self.run_query("""
            SELECT 
                p.product_name,
                c.category_name,
                SUM(oi.quantity) AS units_sold,
                ROUND(AVG(SUM(oi.quantity)) OVER (PARTITION BY c.category_id), 2) AS category_avg,
                ROUND(SUM(oi.quantity) - AVG(SUM(oi.quantity)) OVER (PARTITION BY c.category_id), 2) AS vs_avg
            FROM order_items oi
            JOIN products p ON oi.product_id = p.product_id
            JOIN categories c ON p.category_id = c.category_id
            GROUP BY p.product_id, p.product_name, c.category_id, c.category_name
            ORDER BY c.category_name, units_sold DESC
            LIMIT 20
        """, "Query 5: Product Performance vs Category Average (PARTITION BY)")
        
        # Query 6: Day of week analysis
        self.run_query("""
            SELECT
                CASE CAST(strftime('%w', order_date) AS INTEGER)
                    WHEN 0 THEN 'Sunday'
                    WHEN 1 THEN 'Monday'
                    WHEN 2 THEN 'Tuesday'
                    WHEN 3 THEN 'Wednesday'
                    WHEN 4 THEN 'Thursday'
                    WHEN 5 THEN 'Friday'
                    WHEN 6 THEN 'Saturday'
                END AS day_of_week,
                COUNT(*) AS orders,
                ROUND(SUM(total_amount), 2) AS revenue,
                ROUND(AVG(total_amount), 2) AS avg_order_value
            FROM orders
            WHERE order_status = 'Completed'
            GROUP BY strftime('%w', order_date)
            ORDER BY CAST(strftime('%w', order_date) AS INTEGER)
        """, "Query 6: Sales Pattern by Day of Week")

        # Query 7: Running total of daily sales
        self.run_query("""
            SELECT
                order_date,
                COUNT(*) AS orders_count,
                SUM(total_amount) AS daily_revenue,
                SUM(SUM(total_amount)) OVER (ORDER BY order_date) AS running_total
            FROM orders
            WHERE order_status = 'Completed'
            GROUP BY order_date
            ORDER BY order_date
            LIMIT 30
        """, "Query 7: Running Total of Daily Sales (Cumulative SUM)")

        # Query 8: Product performance with multiple CTEs
        self.run_query("""
            WITH product_sales AS (
                SELECT
                    p.product_id,
                    p.product_name,
                    c.category_name,
                    SUM(oi.quantity) AS units_sold,
                    SUM(oi.line_total) AS revenue
                FROM order_items oi
                JOIN products p ON oi.product_id = p.product_id
                JOIN categories c ON p.category_id = c.category_id
                GROUP BY p.product_id, p.product_name, c.category_name
            ),
            category_totals AS (
                SELECT
                    category_name,
                    SUM(revenue) AS category_revenue
                FROM product_sales
                GROUP BY category_name
            )
            SELECT
                ps.product_name,
                ps.category_name,
                ps.units_sold,
                ROUND(ps.revenue, 2) AS product_revenue,
                ROUND(ct.category_revenue, 2) AS category_revenue,
                ROUND((ps.revenue / ct.category_revenue) * 100, 2) AS pct_of_category
            FROM product_sales ps
            JOIN category_totals ct ON ps.category_name = ct.category_name
            ORDER BY ps.category_name, ps.revenue DESC
            LIMIT 20
        """, "Query 8: Product Performance with % of Category Revenue (Multiple CTEs)")

        # Query 9: Sales by customer tier and category
        self.run_query("""
            SELECT
                c.customer_tier,
                cat.category_name,
                COUNT(DISTINCT o.order_id) AS order_count,
                SUM(oi.quantity) AS units_sold,
                ROUND(SUM(oi.line_total), 2) AS total_revenue,
                ROUND(AVG(oi.line_total), 2) AS avg_line_value
            FROM customers c
            JOIN orders o ON c.customer_id = o.customer_id
            JOIN order_items oi ON o.order_id = oi.order_id
            JOIN products p ON oi.product_id = p.product_id
            JOIN categories cat ON p.category_id = cat.category_id
            WHERE o.order_status = 'Completed'
            GROUP BY c.customer_tier, cat.category_name
            ORDER BY c.customer_tier, total_revenue DESC
        """, "Query 9: Sales by Customer Tier and Category")

        # Query 10: Cohort analysis
        self.run_query("""
            SELECT
                strftime('%Y-%m', c.registration_date) AS cohort_month,
                COUNT(DISTINCT c.customer_id) AS customers_in_cohort,
                COUNT(DISTINCT o.order_id) AS total_orders,
                ROUND(AVG(o.total_amount), 2) AS avg_order_value,
                ROUND(SUM(o.total_amount), 2) AS cohort_revenue
            FROM customers c
            LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.order_status = 'Completed'
            GROUP BY cohort_month
            ORDER BY cohort_month
        """, "Query 10: Cohort Analysis - Customers by Registration Month")

        # Query 11: Customers who spent more than average (Subquery)
        self.run_query("""
            SELECT
                c.customer_id,
                c.first_name || ' ' || c.last_name as customer_name,
                ROUND(SUM(o.total_amount), 2) as total_spent,
                (SELECT
                    ROUND(AVG(customer_total), 2)
                 FROM (SELECT customer_id, SUM(total_amount) as customer_total
                        FROM orders
                        WHERE order_status = 'Completed'
                        GROUP BY customer_id)) as avg_customer_spend
            FROM customers c
            JOIN orders o on o.customer_id = c.customer_id
            WHERE o.order_status = 'Completed'
            GROUP BY c.customer_id, c.first_name, c.last_name
            HAVING SUM(o.total_amount) > (
                SELECT AVG(customer_total)
                FROM (SELECT customer_id, SUM(total_amount) AS customer_total
                      FROM orders
                      WHERE order_status = 'Completed'
                      GROUP BY customer_id)
            )
            ORDER BY total_spent DESC
            LIMIT 15
        """, "Query 11: Customers Spending Above Average (Subqueries)")

        # Query 12: Products never ordered
        self.run_query("""
            SELECT
                p.product_id,
                p.product_name,
                c.category_name,
                p.unit_price,
                p.stock_quantity
            FROM products p
            JOIN categories c ON p.category_id = c.category_id
            WHERE p.product_id NOT IN (
                SELECT DISTINCT product_id
                FROM order_items
            )
            ORDER BY c.category_name, p.product_name
        """, "Query 12: Products Never Ordered (NOT IN Subquery)")

        # Query 13: Shipping performance
        self.run_query("""
            SELECT
                strftime('%Y-%m', order_date) AS month,
                COUNT(*) AS orders_shipped,
                ROUND(AVG(JULIANDAY(ship_date) - JULIANDAY(order_date)), 2) AS avg_days_to_ship,
                MIN(JULIANDAY(ship_date) - JULIANDAY(order_date)) AS fastest_ship,
                MAX(JULIANDAY(ship_date) - JULIANDAY(order_date)) AS slowest_ship
            FROM orders
            WHERE ship_date IS NOT NULL
            GROUP BY strftime('%Y-%m', order_date)
            ORDER BY month
        """, "Query 13: Shipping Performance - Average Days to Ship")
    
    def close(self):
        """Close database connection"""
        self.conn.close()
        print("\n✓ Database connection closed")


def main():
    """
    Main execution function
    """
    print("="*80)
    print("ADVANCED SQL QUERIES - DATA ENGINEERING PRACTICE")
    print("="*80)

    # Initialize SQL exercises
    sql = SQLExercises('sales_analytics.db')

    # Run all queries
    sql.run_all_queries()

    # Close connection
    sql.close()

    print("\n" + "="*80)
    print("✅ ALL QUERIES EXECUTED SUCCESSFULLY!")
    print("="*80)
    print("\nKey SQL Concepts Demonstrated:")
    print("  ✓ Window Functions (ROW_NUMBER, LAG, AVG OVER PARTITION)")
    print("  ✓ Common Table Expressions (CTEs)")
    print("  ✓ Complex JOINs across multiple tables")
    print("  ✓ Aggregations and GROUP BY")
    print("  ✓ Subqueries")
    print("  ✓ Date/Time manipulation")
    print("="*80)

if __name__ == "__main__":
    main()