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
- [ ] Day 2: Aggregations & Analytics
- [ ] Day 3: SQL Performance & Optimization
- [ ] Day 4: Data Modeling Basics
- [ ] Day 5: Python + SQL Integration

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
---
*Last Updated: October 21, 2025*