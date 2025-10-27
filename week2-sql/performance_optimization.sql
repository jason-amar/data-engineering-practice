-- ============================================================================
-- SQL PERFORMANCE & OPTIMIZATION
-- Learning to diagnose and fix slow queries
-- Author: Jason Amar
-- Date: 2025-10-20
-- ============================================================================

-- ============================================================================
-- SECTION 1: UNDERSTANDING QUERY EXECUTION PLANS
-- ============================================================================

-- Query execution plans show you HOW SQLite executes your query
-- This is critical for understanding performance

--Example 1: Simple query without index 
EXPLAIN QUERY PLAN
SELECT * FROM customers
WHERE email = 'john.doe1@email.com';
--RESULT: SCAN customers (full table scan = slow for large tables!)

--Example 2: Join query without indexes
EXPLAIN QUERY PLAN
SELECT 
    c.first_name,
    c.last_name,
    o.order_id,
    o.total_amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.city = 'Seattle';
--RESULT: shows how tables are scanned and joined 

-- ============================================================================
-- SECTION 2: CREATING INDEXES TO IMPROVE PERFORMANCE
-- ============================================================================

-- Indexes are like book indexes - they help find data quickly
-- WITHOUT index: Database scans EVERY row (like reading entire book)
-- WITH index: Database jumps directly to the row (like using book index)

-- Check current indexes
SELECT 
    name, 
    tbl_name, 
    sql 
FROM sqlite_master 
WHERE type = 'index' 
ORDER BY tbl_name, name;

-- Create index on email column in customers table
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);

-- Now run the same query - MUCH faster!
EXPLAIN QUERY PLAN
SELECT * FROM customers WHERE email = 'john.doe1@email.com';
-- Result: SEARCH using INDEX idx_customers_email (FAST!)

-- Create composite index (multiple columns)
CREATE INDEX IF NOT EXISTS idx_customers_city_state ON customers(city, state);

-- Create index on foreign keys (critical for joins!)
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON order_items(order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id);

--Partial index (index only rows matching condition)
CREATE INDEX IF NOT EXISTS idx_orders_completed ON orders(order_date)
WHERE order_status = 'completed';

-- ============================================================================
-- SECTION 3: BEFORE vs AFTER INDEX PERFORMANCE
-- ============================================================================

-- Test 1: Find customer by email (common lookup)
-- WITHOUT INDEX (slow):
-- DROP INDEX IF EXISTS idx_customers_email;
-- WITH INDEX (fast):
SELECT * FROM customers WHERE email = 'alice.smith1@email.com';

-- Test 2: Find orders by customer (JOIN performance)
SELECT 
    c.first_name || ' ' || c.last_name AS customer_name,
    COUNT(o.order_id) AS order_count,
    ROUND(SUM(o.total_amount), 2) AS total_spent
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE c.customer_tier = 'Gold'
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC;

-- Test 3: Date range query (benefits from index on order_date)
SELECT 
    order_date,
    COUNT(*) AS orders,
    ROUND(SUM(total_amount), 2) AS revenue
FROM orders
WHERE order_date BETWEEN '2024-01-01' AND '2024-12-31'
    AND order_status = 'Completed'
GROUP BY order_date
ORDER BY order_date;

-- ============================================================================
-- SECTION 4: QUERY OPTIMIZATION TECHNIQUES
-- ============================================================================

-- ANTI-PATTERN 1: SELECT * (retrieves unnecessary columns)
-- BAD:
SELECT * FROM orders WHERE order_status = 'Completed';

-- GOOD: Only select columns you need
SELECT order_id, customer_id, order_date, total_amount 
FROM orders 
WHERE order_status = 'Completed';

-- ANTI-PATTERN 2: Unnecessary DISTINCT
-- BAD: DISTINCT when not needed
SELECT DISTINCT customer_id FROM orders;

-- GOOD: Use GROUP BY if you need aggregation
SELECT customer_id, COUNT(*) AS order_count 
FROM orders 
GROUP BY customer_id;

-- ANTI-PATTERN 3: Functions in WHERE clause (prevents index usage)
-- BAD: Function on indexed column
SELECT * FROM customers 
WHERE LOWER(email) = 'john.doe@email.com';

-- GOOD: Direct comparison (uses index)
SELECT * FROM customers 
WHERE email = 'john.doe@email.com';

-- ANTI-PATTERN 4: Correlated subqueries (executes once per row)
-- BAD: Subquery executes for EACH customer
SELECT 
    c.customer_id,
    c.first_name,
    (SELECT COUNT(*) FROM orders o WHERE o.customer_id = c.customer_id) AS order_count
FROM customers c;

-- GOOD: Use JOIN instead
SELECT 
    c.customer_id,
    c.first_name,
    COUNT(o.order_id) AS order_count
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name;

-- ============================================================================
-- SECTION 5: ANALYZING SLOW QUERIES
-- ============================================================================

-- Complex query to analyze
-- This query finds top products by revenue in each category
EXPLAIN QUERY PLAN
WITH product_revenue AS (
    SELECT 
        p.product_id,
        p.product_name,
        c.category_name,
        SUM(oi.line_total) AS total_revenue,
        ROW_NUMBER() OVER (PARTITION BY c.category_id ORDER BY SUM(oi.line_total) DESC) AS rank_in_category
    FROM products p
    JOIN categories c ON p.category_id = c.category_id
    JOIN order_items oi ON p.product_id = oi.product_id
    JOIN orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'Completed'
    GROUP BY p.product_id, p.product_name, c.category_id, c.category_name
)
SELECT 
    category_name,
    product_name,
    ROUND(total_revenue, 2) AS revenue,
    rank_in_category
FROM product_revenue
WHERE rank_in_category <= 3
ORDER BY category_name, rank_in_category;

-- ============================================================================
-- SECTION 6: INDEX MAINTENANCE & BEST PRACTICES
-- ============================================================================

-- Check index usage and size
SELECT 
    name AS index_name,
    tbl_name AS table_name,
    sql AS create_statement
FROM sqlite_master
WHERE type = 'index' 
    AND name NOT LIKE 'sqlite_%'  -- Exclude internal indexes
ORDER BY tbl_name, name;

-- Analyze database statistics (helps query optimizer)
ANALYZE;

-- VACUUM - Reclaim space and defragment
-- (Run this periodically, but not in this script - it locks the DB)
-- VACUUM;

-- ============================================================================
-- SECTION 7: WHEN TO USE (AND NOT USE) INDEXES
-- ============================================================================

/*
WHEN TO CREATE INDEXES:
    Columns in WHERE clauses (especially equality comparisons)
    Columns in JOIN conditions (foreign keys!)
    Columns in ORDER BY clauses
    Columns used in GROUP BY
    Frequently queried columns

WHEN NOT TO CREATE INDEXES:
    Small tables (< 1000 rows) - full scan is faster
    Columns with low cardinality (few unique values like boolean)
    Columns that are frequently updated (index maintenance overhead)
    Wide columns (large text/blob fields)
    Tables with heavy INSERT/UPDATE workload (indexes write slowly)

GOLDEN RULE: Indexes speed up READS but slow down WRITES
*/

-- ============================================================================
-- SECTION 8: PERFORMANCE COMPARISON QUERIES
-- ============================================================================

--Query 1: Customer lookup performance test
--This should be FAST with idx_customers_email
SELECT * FROM customers
WHERE email = 'emily.johnson100@email.com'

-- Query 2: Join performance test
-- Should benefit from foreign key indexes
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    COUNT(o.order_id) AS total_orders,
    ROUND(SUM(o.total_amount), 2) AS lifetime_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
WHERE o.order_status = 'Completed'
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING COUNT(o.order_id) >= 5
ORDER BY lifetime_value DESC
LIMIT 20;

-- Query 3: Date range with index
SELECT 
    strftime('%Y-%m', order_date) AS month,
    COUNT(*) AS order_count,
    ROUND(SUM(total_amount), 2) AS monthly_revenue
FROM orders
WHERE order_date >= date('now', '-6 months')
    AND order_status = 'Completed'
GROUP BY strftime('%Y-%m', order_date)
ORDER BY month;

-- Query 4: Multi-table join (all foreign keys indexed)
SELECT
    cat.category_name,
    p.product_name,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    SUM(oi.quantity) AS units_sold,
    ROUND(SUM(oi.line_total), 2) AS revenue
FROM categories cat
JOIN products p ON cat.category_id = p.category_id
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'Completed'
    AND o.order_date >= date('now', '-3 months')
GROUP BY cat.category_name, p.product_name
ORDER BY revenue DESC
LIMIT 15;

-- ============================================================================
-- SECTION 9: QUERY OPTIMIZATION CHECKLIST
-- ============================================================================

/*
BEFORE RUNNING IN PRODUCTION:

1. ✅ Run EXPLAIN QUERY PLAN - understand execution
2. ✅ Ensure indexes exist on:
   - Foreign key columns
   - WHERE clause columns
   - JOIN conditions
   - ORDER BY columns
3. ✅ Select only needed columns (avoid SELECT *)
4. ✅ Use appropriate JOIN types
5. ✅ Filter as early as possible (WHERE before JOIN when possible)
6. ✅ Use CTEs for readability, but be aware they may not optimize like subqueries
7. ✅ Test with realistic data volumes
8. ✅ Run ANALYZE to update statistics
9. ✅ Monitor query execution time
10. ✅ Consider materialized views for very complex aggregations
*/