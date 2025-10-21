-- ============================================================================
-- ADVANCED SQL QUERIES FOR DATA ENGINEERING
-- Author: Jason Amar
-- Date: 2025-10-21
-- ============================================================================

-- These queries demonstrate production-level SQL skills:
-- 1. Window functions
-- 2. CTEs (Common Table Expressions)
-- 3. Aggregations and analytics
-- 4. Complex joins
-- 5. Subqueries

-- ============================================================================
-- SECTION 1: WINDOW FUNCTIONS
-- ============================================================================

-- Query 1: Rank customers by total spending
-- Window function: ROW_NUMBER() - assigns unique sequential number
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name as customer_name,
    c.customer_tier,
    sum(o.total_amount) as total_spent,
    ROW_NUMBER() OVER (order by sum(o.total_amount) desc) as spending_rank
FROM customers c 
JOIN orders o on c.customer_id = o.customer_id
WHERE o.order_status = 'Completed'
GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_tier
ORDER BY total_spent DESC
LIMIT 10;

-- Query 2: Running total of daily sales
-- Window function: SUM() OVER() - cumulative sum
SELECT 
    order_date,
    COUNT(*) AS orders_count,
    SUM(total_amount) AS daily_revenue,
    SUM(SUM(total_amount)) OVER (ORDER BY order_date) AS running_total
FROM orders
WHERE order_status = 'Completed'
GROUP BY order_date
ORDER BY order_date;

-- Query 3: Compare each product's sales to category average
-- Window function: AVG() OVER (PARTITION BY) - average within groups
SELECT 
    p.product_name,
    c.category_name,
    SUM(oi.quantity) AS units_sold,
    ROUND(AVG(SUM(oi.quantity)) OVER (PARTITION BY c.category_id), 2) AS category_avg_sales,
    ROUND(SUM(oi.quantity) - AVG(SUM(oi.quantity)) OVER (PARTITION BY c.category_id), 2) AS vs_category_avg
FROM order_items oi
JOIN products p ON oi.product_id = p.product_id
JOIN categories c ON p.category_id = c.category_id
GROUP BY p.product_id, p.product_name, c.category_id, c.category_name
ORDER BY c.category_name, units_sold DESC;

-- Query 4: Month-over-month growth
-- Window functions: LAG() - access previous row value
SELECT 
    strftime('%Y-%m', order_date) AS month,
    SUM(total_amount) AS monthly_revenue,
    LAG(SUM(total_amount)) OVER (ORDER BY strftime('%Y-%m', order_date)) AS prev_month_revenue,
    ROUND(
        (SUM(total_amount) - LAG(SUM(total_amount)) OVER (ORDER BY strftime('%Y-%m', order_date))) 
        / LAG(SUM(total_amount)) OVER (ORDER BY strftime('%Y-%m', order_date)) * 100, 
        2
    ) AS growth_percentage
FROM orders
WHERE order_status = 'Completed'
GROUP BY strftime('%Y-%m', order_date)
ORDER BY month;   

-- ============================================================================
-- SECTION 2: COMMON TABLE EXPRESSIONS (CTEs)
-- ============================================================================

-- Query 5: Multi-step analysis with CTEs - Customer lifetime value
WITH customer_orders as (
    SELECT
        customer_id,
        COUNT(*) AS order_count,
        SUM(total_amount) as total_spent,
        MIN(order_date) as first_order,
        MAX(order_date) as last_order,
    FROM orders
    WHERE order_status = 'Completed'
    Group by customer_id 
),
customer_segments as (
    SELECT
        *,
        CASE 
            WHEN total_spent >= 1000 THEN 'High Value'
            WHEN total_spent >= 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS value_segment,
        --JULIANDAY below is in place of DATEDIFF b/c this is in SQLite
        JULIANDAY(last_order) - JULIANDAY(first_order) AS customer_lifespan_days
    FROM customer_orders
)
SELECT
    c.first_name || ' ' || c.last_name as customer_name,
    c.customer_tier,
    cs.value_segment,
    cs.order_count,
    ROUND(cs.total_spent,2) as total_spent,
    ROUND(cs.total_spent / cs.order_count, 2) as avg_order_value,
    cs.customer_lifespan_days
FROM customer_segments cs 
JOIN customers c ON cs.customer_id = c.customer_id
ORDER BY cs.total_spent DESC
LIMIT 20;

-- Query 6: Product performance analysis with multiple CTEs
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
ORDER BY ps.category_name, ps.revenue DESC;

-- ============================================================================
-- SECTION 3: COMPLEX AGGREGATIONS
-- ============================================================================

-- Query 7: Sales by customer tier and category
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
ORDER BY c.customer_tier, total_revenue DESC;

-- Query 8: Cohort analysis - customers by registration month
SELECT 
    --strftime below is in place of month() b/c this is in SQLite
    strftime('%Y-%m', c.registration_date) AS cohort_month,
    COUNT(DISTINCT c.customer_id) AS customers_in_cohort,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(AVG(o.total_amount), 2) AS avg_order_value,
    ROUND(SUM(o.total_amount), 2) AS cohort_revenue
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id AND o.order_status = 'Completed'
GROUP BY cohort_month
ORDER BY cohort_month;

-- ============================================================================
-- SECTION 4: SUBQUERIES AND COMPLEX JOINS
-- ============================================================================

-- Query 9: Find customers who spent more than the average
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
ORDER BY total_spent desc 

-- Query 10: Products never ordered
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
ORDER BY c.category_name, p.product_name;

-- ============================================================================
-- SECTION 5: DATE/TIME ANALYSIS
-- ============================================================================

-- Query 11: Day of week sales pattern
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
    COUNT(*) AS order_count,
    ROUND(SUM(total_amount), 2) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value
FROM orders
WHERE order_status = 'Completed'
GROUP BY strftime('%w', order_date)
ORDER BY CAST(strftime('%w', order_date) AS INTEGER);

-- Query 12: Shipping performance - average days to ship
SELECT 
    strftime('%Y-%m', order_date) AS month,
    COUNT(*) AS orders_shipped,
    ROUND(AVG(JULIANDAY(ship_date) - JULIANDAY(order_date)), 2) AS avg_days_to_ship,
    MIN(JULIANDAY(ship_date) - JULIANDAY(order_date)) AS fastest_ship,
    MAX(JULIANDAY(ship_date) - JULIANDAY(order_date)) AS slowest_ship
FROM orders
WHERE ship_date IS NOT NULL
GROUP BY strftime('%Y-%m', order_date)
ORDER BY month;