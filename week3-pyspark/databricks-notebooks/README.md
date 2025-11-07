# Databricks Notebooks - Week 3 PySpark

This folder contains Databricks notebooks for hands-on PySpark practice with real-world data.

## 📓 Day 2: NYC Taxi Data Analysis

**File:** [Day2_NYC_Taxi_Analysis.html](Day2_NYC_Taxi_Analysis.html)

### Overview
Analyzed 10+ million NYC taxi trips using PySpark in Azure Databricks to uncover business insights about revenue patterns, demand forecasting, and operational efficiency.

### Business Questions Answered

**Revenue Optimization:**
- Identified peak revenue hours (7 PM generates highest revenue)
- Weekend fares average 15-20% higher than weekdays
- Calculated month-over-month growth trends

**Demand Patterns:**
- Analyzed hourly demand by day of week
- Identified busiest pickup locations
- Mapped high-traffic routes

**Operational Insights:**
- Average trip: X miles, X minutes, $X fare
- Tip percentage varies by payment type
- Distance-based fare analysis

### Technical Skills Demonstrated

**PySpark Operations:**
- Data cleaning and quality checks (removed X% invalid records)
- Feature engineering (date/time extraction, calculated metrics)
- Complex aggregations (GROUP BY, pivot tables)
- Window functions (rankings, running totals, MoM growth)
- PARTITION BY for group-wise operations
- Join operations across dimensions

**Performance Optimization:**
- Cached DataFrames for repeated queries
- Efficient use of window functions
- Appropriate partition strategies

### Key Metrics

| Metric | Value |
|--------|-------|
| Total Trips Analyzed | X million |
| Date Range | YYYY-MM to YYYY-MM |
| Peak Hour | X PM (X trips) |
| Average Fare | $X.XX |
| Average Distance | X.X miles |
| Weekend Premium | +X% |

### Sample Insights

**Peak Revenue Hours:**
1. 7 PM - $XXX,XXX
2. 8 PM - $XXX,XXX
3. 6 PM - $XXX,XXX

**Top Routes:**
1. Zone A → Zone B: X,XXX trips
2. Zone C → Zone D: X,XXX trips

---

## 🔗 Related Work

This analysis builds on:
- [Week 1: Python Foundations](../../week1-setup)
- [Week 2: SQL Mastery](../../week2-sql)
- [Week 3 Day 1: PySpark Basics](../pyspark_basics.py)

---

## 📊 Business Value

**For Operations:**
- Optimize driver allocation based on demand patterns
- Focus on high-revenue hours and locations
- Improve route efficiency

**For Finance:**
- Revenue forecasting using historical trends
- Pricing optimization by time/location
- Identify growth opportunities

**For Strategy:**
- Market expansion opportunities (underserved areas)
- Partnership opportunities (high-traffic routes)
- Competitive positioning

---

*Completed in Azure Databricks using PySpark DataFrames and SQL*