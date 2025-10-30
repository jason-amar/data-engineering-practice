-- ============================================================================
-- DATA WAREHOUSE DIMENSIONAL MODEL
-- Star schema for sales analytics
-- Author: Jason Amar
-- Date: 2025-10-30
-- ============================================================================

-- This creates a star schema optimized for analytics (OLAP)
-- Different from our normalized OLTP database (sales_analytics.db)

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension 1: Date Dimension (every date in the range)
-- Pre-populated with all dates - crucial for time series analysis

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,           -- YYYYMMDD format (e.g., 20241020)
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,               -- 1, 2, 3, 4
    month INTEGER NOT NULL,                 -- 1-12
    month_name TEXT NOT NULL,               -- January, February, etc.
    week_of_year INTEGER NOT NULL,          -- 1-53
    day_of_year INTEGER NOT NULL,           -- 1-366
    day_of_month INTEGER NOT NULL,          -- 1-31
    day_of_week INTEGER NOT NULL,           -- 0=Sunday, 6=Saturday
    day_name TEXT NOT NULL,                 -- Monday, Tuesday, etc.
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT 0,
    fiscal_year INTEGER,                    -- For companies with fiscal years
    fiscal_quarter INTEGER
);

CREATE INDEX idx_dim_date_full_date ON dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month);

-- Dimension 2: Customer Dimension (Type 2 SCD - keeps history)
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER PRIMARY KEY AUTOINCREMENT,  -- Surrogate key
    customer_id INTEGER NOT NULL,                     -- Natural key (from source)
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    city TEXT,
    state TEXT,
    customer_tier TEXT NOT NULL,
    registration_date DATE,
      -- SCD Type 2 columns
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT 1,
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_natural_key ON dim_customer(customer_id, is_current);
CREATE INDEX idx_dim_customer_tier ON dim_customer(customer_tier);

-- Dimension 3: Product Dimension
CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER PRIMARY KEY AUTOINCREMENT,   -- Surrogate key
    product_id INTEGER NOT NULL UNIQUE,              -- Natural key
    product_name TEXT NOT NULL,
    category_id INTEGER NOT NULL,
    category_name TEXT NOT NULL,                     -- Denormalized from category table
    category_description TEXT,
    unit_price REAL NOT NULL,
    is_active BOOLEAN DEFAULT 1,
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_category ON dim_product(category_name);
CREATE INDEX idx_dim_product_active ON dim_product(is_active);

-- Dimension 4: Order Status Dimension (small lookup table)
CREATE TABLE IF NOT EXISTS dim_order_status (
    status_key INTEGER PRIMARY KEY AUTOINCREMENT,
    status_code TEXT NOT NULL UNIQUE,
    status_name TEXT NOT NULL,
    status_description TEXT,
    is_final_state BOOLEAN DEFAULT 0              -- Completed/Cancelled are final
);

-- Dimension 5: Payment Method Dimension
CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_method_key INTEGER PRIMARY KEY AUTOINCREMENT,
    payment_method_code TEXT NOT NULL UNIQUE,
    payment_method_name TEXT NOT NULL,
    processing_fee_pct REAL DEFAULT 0.0
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

-- Fact Table: Sales Transactions (grain: one row per order line item)
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key INTEGER PRIMARY KEY AUTOINCREMENT,
    -- Foreign keys to dimensions (using surrogate keys)
    date_key INTEGER NOT NULL,
    customer_key INTEGER NOT NULL,
    product_key INTEGER NOT NULL,
    status_key INTEGER NOT NULL,
    payment_method_key INTEGER,
    -- Degenerate dimensions (dimensions that don't warrant separate table)
    order_id INTEGER NOT NULL,
    order_item_id INTEGER NOT NULL,
    -- Measures (numeric facts)
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    line_total REAL NOT NULL,
    discount_amount REAL DEFAULT 0.0,
    tax_amount REAL DEFAULT 0.0,
    net_revenue REAL NOT NULL,                    -- line_total - discount + tax
    -- Derived measures (can be calculated, but stored for performance)
    gross_profit REAL,                            -- If you have cost data
    -- Metadata
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
    FOREIGN KEY (status_key) REFERENCES dim_order_status(status_key),
    FOREIGN KEY (payment_method_key) REFERENCES dim_payment_method(payment_method_key)
);

-- Critical indexes for fact table (will be queried heavily)
CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_order ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_composite ON fact_sales(date_key, customer_key, product_key);

-- ============================================================================
-- VIEWS FOR EASIER QUERYING
-- ============================================================================

-- View: Complete sales with all dimension attributes
CREATE VIEW IF NOT EXISTS vw_sales_complete AS
SELECT 
    -- Fact measures
    f.sales_key,
    f.quantity,
    f.unit_price,
    f.line_total,
    f.discount_amount,
    f.net_revenue,
    -- Date dimension
    d.full_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.day_name,
    d.is_weekend,
    -- Customer dimension
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    c.city,
    c.state,
    c.customer_tier,
    -- Product dimension
    p.product_id,
    p.product_name,
    p.category_name,
    -- Order status
    s.status_name,
    -- Payment method
    pm.payment_method_name,
    -- Degenerate dimensions
    f.order_id,
    f.order_item_id
FROM fact_sales f
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_order_status s ON f.status_key = s.status_key
LEFT JOIN dim_payment_method pm ON f.payment_method_key = pm.payment_method_key;
