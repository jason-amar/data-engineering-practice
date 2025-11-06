# Data Engineering Bootcamp

My learning journey to become a data engineer, focusing on Azure/Databricks stack.

## 📚 Curriculum

16-week intensive program covering:
- Python & SQL foundations
- PySpark & Databricks
- Azure data services
- Data modeling & warehousing
- Orchestration & CI/CD
- Production best practices

## 🗂️ Project Structure
```
data-engineering-bootcamp/
├── week1-setup/
│   ├── venv/
│   ├── logger_demo.py
│   ├── requirements.txt
│   ├── app.log
│   └── .gitignore
├── .gitignore
└── README.md
```

## 🚀 Setup Instructions

1. Clone this repository
2. Create virtual environment: `python -m venv venv`
3. Activate venv:
   - Windows: `venv\Scripts\activate`
   - Mac/Linux: `source venv/bin/activate`
4. Install dependencies: `pip install -r requirements.txt`

## 📅 Progress Tracker

### Week 1: Python & SQL Foundations
- [x] Day 1: Development Environment Setup
- [x] Day 2: Working with File Formats
- [x] Day 3: API Data Extraction (Basketball Reference)
- [x] Day 4: Data Validation & Quality Checks
- [x] Day 5: Mini ETL Pipeline

### Week 2: SQL Mastery
- [x] Day 1: Advanced SQL Queries (Window Functions, CTEs)
- [x] Day 2: Aggregations & Analytics (PIVOT, HAVING, RFM, Cohorts)
- [x] Day 3: SQL Performance & Optimization
- [x] Day 4: Data Modeling Basics (Star Schema, SCD Type 2)
- [x] Day 5: Python + SQL Integration (skipped - not relevant to Azure stack)

### Week 3: PySpark & Databricks
- [x] Day 1: PySpark Fundamentals (concepts)
- [ ] Day 2: Advanced DataFrames (Databricks)
- [ ] Day 3: Delta Lake & Performance (Databricks)
- [ ] Day 4: Databricks Workflows (Databricks)
- [ ] Day 5: Production Pipeline (Databricks)
## 💻 Technologies

- Python 3.11+
- Pandas, Requests, Pytest
- Git & GitHub
- VS Code

## 📝 Learning Notes

### Week 1: Python & SQL Foundations

**Day 1 - Development Environment Setup**
- Set up professional dev environment
- Learned Python logging best practices
- Configured Git with proper .gitignore
- Created reproducible environment with requirements.txt

**Day 2 - Working with File Formats**
- Compared CSV, JSON, and Parquet performance
- Learned why Parquet dominates big data (columnar storage, compression)
- Built reusable file format converter
- Benchmarked read/write speeds and file sizes

**Skipping API extraction and Data quality along with ETL pipeline building. see practice_archive\nba_pipeline for my work there.

**Week 2, Day 1 - Advanced SQL Queries**
- Mastered window functions (ROW_NUMBER, LAG, PARTITION BY)
- Built complex CTEs for multi-step analysis
- Joined 5 tables for comprehensive reporting
- Created realistic sales database with 500 customers, 2000 orders
- Analyzed customer lifetime value, monthly trends, category performance

**Week 2, Day 3 - SQL Performance & Optimization**
- Mastered query execution plans (EXPLAIN QUERY PLAN)
- Created indexes for optimal performance (10x+ speedups!)
- Learned indexing strategies (when to index, when not to)
- Identified and fixed common performance anti-patterns
- Built performance benchmarking tool to measure query speed
- Optimized foreign key joins with composite indexes
- Analyzed before/after performance with real metrics

### Week 2: SQL Mastery
- [x] Day 1: Advanced SQL Queries (Window Functions, CTEs)
- [x] Day 2: Aggregations & Analytics (PIVOT, HAVING, RFM, Cohorts)
- [x] Day 3: SQL Performance & Optimization
- [x] Day 4: Data Modeling Basics (Star Schema, SCD Type 2)
- [x] Day 5: Python + SQL Integration

# Week 3: PySpark & Databricks Fundamentals

Learning distributed data processing with PySpark and Azure Databricks.

## 📚 Overview

This week focuses on big data processing with Apache Spark and Azure Databricks:
- **Day 1:** PySpark fundamentals (completed conceptually)
- **Days 2-5:** Hands-on practice in Azure Databricks workspace

## 🎯 Day 1: PySpark Basics (Local Concepts)

**Topics Covered:**
- Spark architecture (driver, executors, partitions)
- Creating DataFrames (from lists, Pandas, CSV)
- Basic operations (select, filter, withColumn, groupBy)
- Lazy evaluation (transformations vs actions)
- Spark SQL syntax
- Schema definitions (explicit vs inferred)

**Key Concepts Learned:**
- ✅ Lazy evaluation - transformations build query plans, actions trigger execution
- ✅ Immutability - DataFrames are immutable, operations create new DataFrames
- ✅ Distributed processing - Spark splits data across executors for parallel processing
- ✅ SQL and DataFrame API - can use either syntax interchangeably

**Files:**
- `pyspark_basics.py` - Foundational PySpark concepts and operations
- `requirements.txt` - Python dependencies (pyspark, pandas, pyarrow)

## 🌐 Days 2-5: Azure Databricks

**Note:** Remaining lessons are completed in Azure Databricks workspace for hands-on practice with production tools.

**Topics to Cover:**
- Advanced DataFrame operations (joins, window functions)
- Reading/writing Parquet and Delta Lake
- Performance optimization (partitioning, caching, broadcast joins)
- Databricks notebooks and clusters
- Unity Catalog integration

### Week 3: PySpark & Databricks

**Week 3, Day 1 - PySpark Fundamentals**
- Learned Spark architecture (driver, executors, distributed processing)
- Understood lazy evaluation and query optimization
- Practiced DataFrame operations (select, filter, groupBy, aggregations)
- Learned transformations vs actions pattern
- Used both DataFrame API and Spark SQL syntax
- Defined explicit schemas for production data
- *Note: Remaining days completed in Azure Databricks workspace*
---
*Last Updated: November 6, 2025*