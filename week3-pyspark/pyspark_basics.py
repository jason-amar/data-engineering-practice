"""
PySpark Fundamentals - Day 1
Understanding Spark architecture and basic operations

Author: Jason Amar
Date: 2025-11-06
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, avg, sum, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """
    Create SparkSession (entry point for PySpark)
    
    In Databricks, this is already created as 'spark'
    Locally, we create it ourselves
    """
    spark = SparkSession.builder \
        .appName("PySpark Basics") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info(f"✓ Spark {spark.version} started")
    logger.info(f"  Master: {spark.sparkContext.master}")
    logger.info(f"  App Name: {spark.sparkContext.appName}")
    
    return spark


def demo_create_dataframe():
    """
    Different ways to create Spark DataFrames
    """
    print("\n" + "="*80)
    print("CREATING SPARK DATAFRAMES")
    print("="*80)
    
    spark = create_spark_session()
    
    # Method 1: From Python list of tuples
    print("\n1. From Python list:")
    data = [
        ("Alice", 28, "Engineering"),
        ("Bob", 35, "Sales"),
        ("Charlie", 32, "Engineering"),
        ("Diana", 29, "Marketing")
    ]
    columns = ["name", "age", "department"]
    df = spark.createDataFrame(data, columns)
    
    df.show()
    print(f"  Rows: {df.count()}, Columns: {len(df.columns)}")
    
    # Method 2: From Pandas DataFrame
    print("\n2. From Pandas DataFrame:")
    import pandas as pd
    pandas_df = pd.DataFrame({
        'product': ['Laptop', 'Mouse', 'Keyboard'],
        'price': [999.99, 29.99, 79.99],
        'stock': [50, 200, 150]
    })
    
    spark_df = spark.createDataFrame(pandas_df)
    spark_df.show()
    
    # Method 3: From CSV file (we'll create one)
    print("\n3. From CSV file:")
    # Create sample CSV first
    import os
    os.makedirs('data', exist_ok=True)
    
    sample_csv = """name,age,salary
John,30,75000
Jane,28,82000
Mike,35,95000
Sarah,32,88000"""
    
    with open('data/employees.csv', 'w') as f:
        f.write(sample_csv)
    
    df_csv = spark.read.csv('data/employees.csv', header=True, inferSchema=True)
    df_csv.show()
    
    spark.stop()


def demo_dataframe_operations():
    """
    Basic DataFrame operations
    """
    print("\n" + "="*80)
    print("DATAFRAME OPERATIONS")
    print("="*80)
    
    spark = create_spark_session()
    
    # Create sample data
    data = [
        ("Alice", 28, "Engineering", 95000),
        ("Bob", 35, "Sales", 75000),
        ("Charlie", 32, "Engineering", 88000),
        ("Diana", 29, "Marketing", 72000),
        ("Eve", 31, "Sales", 82000),
        ("Frank", 38, "Engineering", 105000)
    ]
    columns = ["name", "age", "department", "salary"]
    df = spark.createDataFrame(data, columns)
    
    print("\nOriginal DataFrame:")
    df.show()
    
    # SELECT columns
    print("\n1. SELECT (specific columns):")
    df.select("name", "salary").show()
    
    # FILTER rows
    print("\n2. FILTER (WHERE clause):")
    df.filter(col("age") > 30).show()
    
    # Multiple conditions
    print("\n3. Multiple conditions (AND):")
    df.filter((col("age") > 30) & (col("salary") > 80000)).show()
    
    # WITH COLUMN (add new column)
    print("\n4. WITH COLUMN (add calculated column):")
    df_with_bonus = df.withColumn("bonus", col("salary") * 0.10)
    df_with_bonus.show()
    
    # WHEN (conditional logic - like CASE WHEN in SQL)
    print("\n5. WHEN (conditional logic):")
    df_with_level = df.withColumn(
        "level",
        when(col("salary") > 90000, "Senior")
        .when(col("salary") > 75000, "Mid")
        .otherwise("Junior")
    )
    df_with_level.show()
    
    spark.stop()


def demo_aggregations():
    """
    Aggregations and GROUP BY operations
    """
    print("\n" + "="*80)
    print("AGGREGATIONS & GROUP BY")
    print("="*80)
    
    spark = create_spark_session()
    
    # Create sales data
    data = [
        ("Electronics", "Laptop", 999.99, 5),
        ("Electronics", "Mouse", 29.99, 20),
        ("Electronics", "Keyboard", 79.99, 15),
        ("Clothing", "T-Shirt", 19.99, 50),
        ("Clothing", "Jeans", 59.99, 30),
        ("Home", "Lamp", 39.99, 25),
        ("Home", "Chair", 199.99, 10)
    ]
    columns = ["category", "product", "price", "quantity"]
    df = spark.createDataFrame(data, columns)
    
    print("\nSales Data:")
    df.show()
    
    # Simple aggregation
    print("\n1. Simple aggregation (total revenue):")
    df.withColumn("revenue", col("price") * col("quantity")) \
        .agg({"revenue": "sum"}) \
        .show()
    
    # GROUP BY
    print("\n2. GROUP BY category:")
    df.groupBy("category") \
        .agg(
            count("product").alias("products"),
            sum(col("price") * col("quantity")).alias("total_revenue"),
            avg("price").alias("avg_price")
        ) \
        .orderBy(col("total_revenue").desc()) \
        .show()
    
    # Multiple aggregations
    print("\n3. Multiple aggregations:")
    from pyspark.sql.functions import min, max, stddev
    
    df.groupBy("category") \
        .agg(
            count("*").alias("count"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            avg("price").alias("avg_price")
        ) \
        .show()
    
    spark.stop()


def demo_transformations_vs_actions():
    """
    Understand lazy evaluation
    """
    print("\n" + "="*80)
    print("TRANSFORMATIONS VS ACTIONS (Lazy Evaluation)")
    print("="*80)
    
    spark = create_spark_session()
    
    data = [(i, i*2, i*3) for i in range(1, 1001)]  # 1000 rows
    df = spark.createDataFrame(data, ["id", "value1", "value2"])
    
    print("\n1. TRANSFORMATIONS (lazy - don't execute immediately):")
    print("   - select(), filter(), groupBy(), join(), withColumn()")
    print("   - These build up a 'query plan' but don't process data yet")
    
    # These are all transformations - nothing runs yet!
    filtered = df.filter(col("value1") > 100)
    selected = filtered.select("id", "value1")
    with_new_col = selected.withColumn("doubled", col("value1") * 2)
    
    print("\n   Created 3 transformations, but NO data processed yet!")
    print("   Spark is just building a query plan...")
    
    print("\n2. ACTIONS (trigger execution):")
    print("   - show(), count(), collect(), write(), take()")
    print("   - These actually execute the query plan")
    
    print("\n   Now calling show() - THIS triggers execution:")
    with_new_col.show(5)
    
    print("\n   Calling count() - Another action:")
    count = with_new_col.count()
    print(f"   Count: {count}")
    
    print("\n✓ Key Insight: Spark optimizes the ENTIRE query plan before running")
    print("  This is much more efficient than running each step separately!")
    
    spark.stop()


def demo_spark_sql():
    """
    Use SQL syntax instead of DataFrame API
    """
    print("\n" + "="*80)
    print("SPARK SQL")
    print("="*80)
    
    spark = create_spark_session()
    
    # Create DataFrame
    data = [
        ("Alice", 28, "Engineering", 95000),
        ("Bob", 35, "Sales", 75000),
        ("Charlie", 32, "Engineering", 88000),
        ("Diana", 29, "Marketing", 72000)
    ]
    columns = ["name", "age", "department", "salary"]
    df = spark.createDataFrame(data, columns)
    
    # Register as temporary view (table)
    df.createOrReplaceTempView("employees")
    
    print("\n1. Using SQL syntax:")
    result = spark.sql("""
        SELECT 
            name,
            department,
            salary,
            CASE 
                WHEN salary > 90000 THEN 'Senior'
                WHEN salary > 75000 THEN 'Mid'
                ELSE 'Junior'
            END as level
        FROM employees
        WHERE age > 28
        ORDER BY salary DESC
    """)
    
    result.show()
    
    print("\n2. Aggregation with SQL:")
    result2 = spark.sql("""
        SELECT 
            department,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            MAX(salary) as max_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    """)
    
    result2.show()
    
    print("\n✓ You can mix DataFrame API and SQL - use what's comfortable!")
    
    spark.stop()


def demo_schema_and_types():
    """
    Understanding schemas and data types
    """
    print("\n" + "="*80)
    print("SCHEMAS & DATA TYPES")
    print("="*80)
    
    spark = create_spark_session()
    
    # Inferred schema
    print("\n1. Inferred Schema:")
    data = [("Alice", 28, 95000.50), ("Bob", 35, 75000.00)]
    df = spark.createDataFrame(data, ["name", "age", "salary"])
    
    df.printSchema()
    
    # Explicit schema (better for production)
    print("\n2. Explicit Schema (RECOMMENDED for production):")
    schema = StructType([
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=False),
        StructField("salary", FloatType(), nullable=False)
    ])
    
    df_explicit = spark.createDataFrame(data, schema)
    df_explicit.printSchema()
    
    print("\n✓ Explicit schemas:")
    print("  - Prevent type inference errors")
    print("  - Fail fast on bad data")
    print("  - Better performance (no type detection)")
    
    spark.stop()


def main():
    """
    Run all demonstrations
    """
    print("\n" + "⚡"*40)
    print("PYSPARK FUNDAMENTALS - DAY 1")
    print("⚡"*40)
    
    demo_create_dataframe()
    input("\nPress Enter to continue...")
    
    demo_dataframe_operations()
    input("\nPress Enter to continue...")
    
    demo_aggregations()
    input("\nPress Enter to continue...")
    
    demo_transformations_vs_actions()
    input("\nPress Enter to continue...")
    
    demo_spark_sql()
    input("\nPress Enter to continue...")
    
    demo_schema_and_types()
    
    print("\n" + "="*80)
    print("✅ PYSPARK BASICS COMPLETE - DAY 1")
    print("="*80)
    print("\nKey Takeaways:")
    print("  ✓ PySpark DataFrames work like Pandas but distributed")
    print("  ✓ Transformations are lazy (build query plan)")
    print("  ✓ Actions trigger execution")
    print("  ✓ Can use DataFrame API OR SQL syntax")
    print("  ✓ Always define explicit schemas for production")
    print("  ✓ Spark optimizes the entire query before running")
    print("="*80)
    print("\nNext: Day 2 - Advanced DataFrame operations, joins, window functions")


if __name__ == "__main__":
    main()