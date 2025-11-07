# Databricks notebook source
# MAGIC %md
# MAGIC Advanced DataFrames with NYC Taxi Sample Data

# COMMAND ----------

# MAGIC %md
# MAGIC Part 1: Load Data

# COMMAND ----------

# DBTITLE 1,Load NYC Taxi data from Databricks samples
df_taxi = spark.read.format("delta") \
    .load("/databricks-datasets/nyctaxi/tables/nyctaxi_yellow")

print(f"Total trips: {df_taxi.count():,}")
df_taxi.printSchema()
display(df_taxi.limit(10))

# COMMAND ----------

# DBTITLE 1,Data Quality Assessment
from pyspark.sql.functions import *

# Date Range in dataset
date_range = df_taxi.select(
    min("pickup_datetime").alias("earliest_trip"),
    max("pickup_datetime").alias("latest_trip")
).collect()[0]

print(f"Date range in dataset: {date_range.earliest_trip} to {date_range.latest_trip}")

# Check for nulls
null_counts = df_taxi.select([
    count(when(col(c).isNull(), c)).alias(c)
    for c in df_taxi.columns
])
print("\nNull counts by column:")
display(null_counts)

# Basic statistics
print("\nBasic statistics:")
display(df_taxi.describe())


# COMMAND ----------

# MAGIC %md
# MAGIC Part 2: Data Cleaning & Feature Engineering

# COMMAND ----------

# DBTITLE 1,Data cleaning and enhancement
# Original count
original_count = df_taxi.count()
print(f"Original record count: {original_count:,}")

# Remove invalid/suspicious trips
df_clean = df_taxi.filter(
    (col("fare_amount") >= 0) &
    (col("fare_amount") <= 500) &
    (col("trip_distance") >= 0) &
    (col("trip_distance") <= 100) &
    (col("passenger_count") >= 0) &
    (col("passenger_count") <= 7) &
    (col("total_amount") > 0) &
    (col("dropoff_datetime").isNotNull()) &
    (col("pickup_datetime").isNotNull())
)

clean_count = df_clean.count()
removed = original_count - clean_count
print(f"After cleaning: {clean_count:,}")
print(f"Removed: {removed:,} ({removed/original_count*100:.2f}%)")

# COMMAND ----------

# DBTITLE 1,Add calculated columns
from pyspark.sql.functions import try_divide

df_enhanced = df_clean \
    .withColumn("pickup_date", to_date("pickup_datetime")) \
    .withColumn("pickup_hour", hour("pickup_datetime")) \
    .withColumn("pickup_day_of_week", dayofweek("pickup_datetime")) \
    .withColumn("pickup_month", month("pickup_datetime")) \
    .withColumn("pickup_year", year("pickup_datetime")) \
    .withColumn("day_name", 
        when(col("pickup_day_of_week") == 1, "Sunday")
        .when(col("pickup_day_of_week") == 2, "Monday")
        .when(col("pickup_day_of_week") == 3, "Tuesday")
        .when(col("pickup_day_of_week") == 4, "Wednesday")
        .when(col("pickup_day_of_week") == 5, "Thursday")
        .when(col("pickup_day_of_week") == 6, "Friday")
        .otherwise("Saturday")
    ) \
    .withColumn("is_weekend",
        when(col("pickup_day_of_week").isin([1,7]), True).otherwise(False)
    ) \
    .withColumn("trip_duration_minutes",
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60
    ) \
    .withColumn("speed_mph",
        try_divide(col("trip_distance"), col("trip_duration_minutes") / 60)
    ) \
    .withColumn("tip_percentage",
        try_divide(col("tip_amount"), col("fare_amount")) * 100
    ) \
    .withColumn("revenue_per_mile",
        try_divide(col("total_amount"), col("trip_distance"))
    )
print("\n✓ Added calculated columns:")
print("  - Date/time features (hour, day, month, year)")
print("  - Weekend flag")
print("  - Trip duration in minutes")
print("  - Speed (mph)")
print("  - Tip percentage")
print("  - Revenue per mile")

display(df_enhanced.select(
     "pickup_date", "pickup_hour", "day_name", "is_weekend",
    "trip_distance", "trip_duration_minutes", "speed_mph",
    "fare_amount", "tip_percentage", "revenue_per_mile"
).limit(10))

print("\n✓ Skipped DataFrame caching (not supported on serverless compute)")

# COMMAND ----------

# MAGIC %md
# MAGIC Part 3: Business Questions - Revenue Analysis

# COMMAND ----------

# DBTITLE 1,Question 1 - What are the busiest hours?
print("\nQUESTION 1: BUSIEST HOURS BY TRIP COUNT")
print("="*80)

df_hourly = df_enhanced.groupBy("pickup_hour").agg(
    count("*").alias("numtrips"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_fare"),
    avg("trip_distance").alias("avg_distance"),
).orderBy("pickup_hour")

display(df_hourly)

# Find Peak Hour
peak_hour = df_hourly.orderBy(col("numtrips").desc()).first()
print(f"\n Peak Hour: {peak_hour.pickup_hour}:00 with {peak_hour.numtrips:,} trips")

# COMMAND ----------

# DBTITLE 1,Question 2 - Weekend vs Weekday Performance
print("\nQUESTION 2: WEEKEND VS WEEKDAY PATTERNS")
print("="*80)

df_weekend_analysis = df_enhanced.groupBy("is_weekend", "pickup_hour").agg(
    count("*").alias("numtrips"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_fare"),
    avg("trip_distance").alias("avg_distance"),
).orderBy("is_weekend","pickup_hour")

# Pivot for easier comparison
df_weekend_pivot = df_weekend_analysis.groupBy("pickup_hour") \
    .pivot("is_weekend", [False, True]) \
    .agg(first("numtrips"))

display(df_weekend_pivot)

# Summary stats
df_weekend_summary = df_enhanced.groupBy("is_weekend").agg(
    count("*").alias("total_trips"),
    sum("total_amount").alias("total_revenue"),
    avg("total_amount").alias("avg_fare"),
    avg("trip_distance").alias("avg_distance"),
    avg("trip_duration_minutes").alias("avg_duration"),
    try_avg("tip_percentage").alias("avg_tip_pct")
)

display(df_weekend_summary)

# COMMAND ----------

# DBTITLE 1,Question 3 - Monthly Revenue Trends
print("\nQUESTION 3: MONTHLY REVENUE TRENDS")
print("="*80)

df_monthly = df_enhanced.groupBy("pickup_year", "pickup_month").agg(
    count("*").alias("trips"),
    sum("total_amount").alias("revenue"),
    avg("total_amount").alias("avg_fare")
).orderBy("pickup_year", "pickup_month")

display(df_monthly)

# Calculate total revenue
total_revenue = df_enhanced.agg(sum("total_amount")).collect()[0][0]
print(f"\n Total revenue: ${total_revenue:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC Part 4: Window Functions - Rankings & Trends

# COMMAND ----------

# DBTITLE 1,Rank Hours by Revenue
from pyspark.sql.window import Window

# Note: This will generate a warning about no partition defined
# This is EXPECTED and OK because:
# 1. We're ranking only 24 hours (small dataset)
# 2. We need a GLOBAL ranking (which hour is #1 overall)
# 3. No partition makes sense here - we want one ordered list

#Create window spec for ranking
window_revenue_rank = Window.orderBy(col("total_revenue").desc())

df_hour_ranked = df_hourly.withColumn(
  "revenue_rank", row_number().over(window_revenue_rank)
).withColumn(
  "revenue_percentile", percent_rank().over(window_revenue_rank)
)

print("Top 5 revenue hours: ")
display(df_hour_ranked.limit(5))

# COMMAND ----------

# DBTITLE 1,Month-over-Month Growth Analysis
window_time = Window.orderBy("pickup_year", "pickup_month")

df_mom_growth = df_monthly \
    .withColumn("prev_month_revenue", lag("revenue", 1).over(window_time)) \
    .withColumn("prev_month_trips", lag("trips", 1).over(window_time)) \
    .withColumn("revenue_growth",
        col("revenue") - col("prev_month_revenue")
    ) \
    .withColumn("revenue_growth_pct",
        ((col("revenue") - col("prev_month_revenue")) / col("prev_month_revenue")) * 100
    ) \
    .withColumn("trip_growth_pct",
        ((col("trips") - col("prev_month_trips")) / col("prev_month_trips")) * 100
    )

display(df_mom_growth)

# COMMAND ----------

# DBTITLE 1,Running Total (cumulative revenue)
window_cumulative = Window.orderBy("pickup_year", "pickup_month") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_cumulative = df_monthly.withColumn(
    "cumulative_revenue",
    sum("revenue").over(window_cumulative)
).withColumn(
    "cumulative_trips",
    sum("trips").over(window_cumulative)
)

display(df_cumulative)

# COMMAND ----------

# DBTITLE 1,Day of Week Ranking with PARTITION BY
df_day_hour = df_enhanced.groupBy("day_name", "pickup_day_of_week", "pickup_hour").agg(
    count("*").alias("numtrips"),
    sum("total_amount").alias("total_revenue"),
).orderBy("pickup_day_of_week", "pickup_hour")

# Rank hours within each day
window_by_day = Window.partitionBy("day_name").orderBy(col("total_revenue").desc())

df_day_hour_ranked = df_day_hour.withColumn(
    "rank_within_day",row_number().over(window_by_day)
).filter(col("rank_within_day") <= 3)

display(df_day_hour_ranked.orderBy("pickup_day_of_week", "rank_within_day"))

# COMMAND ----------

# MAGIC %md
# MAGIC Part 5: Advanced Aggregations

# COMMAND ----------

# DBTITLE 1,Tip Analysis by Payment Type
df_tips = df_enhanced.groupBy("payment_type").agg(
    count("*").alias("numtrips"),
    sum("tip_amount").alias("total_tips"),
    avg("tip_amount").alias("avg_tip"),
    avg("tip_percentage").alias("avg_tip_pct")
).orderBy(col("avg_tip_pct").desc())

display(df_tips)

# Pivot tips by hour and payment type
df_tips_pivot = df_enhanced.groupBy("pickup_hour") \
    .pivot("payment_type") \
    .agg(avg("tip_percentage")) \
    .orderBy("pickup_hour")

display(df_tips_pivot)

# COMMAND ----------

# DBTITLE 1,Passenger Count Analysis
df_passengers = df_enhanced.groupBy("passenger_count").agg(
    count("*").alias("num_trips"),
    sum("total_amount").alias("revenue"),
    avg("total_amount").alias("avg_fare"),
    avg("trip_distance").alias("avg_distance"),
    avg("tip_percentage").alias("avg_tip_pct")
).orderBy("passenger_count")

display(df_passengers)

# COMMAND ----------

# DBTITLE 1,Distance Buckets Analysis
df_distance_buckets = df_enhanced \
    .withColumn("distance_bucket",
        when(col("trip_distance") < 1, "< 1 mile")
        .when(col("trip_distance") < 3, "1-3 miles")
        .when(col("trip_distance") < 5, "3-5 miles")
        .when(col("trip_distance") < 10, "5-10 miles")
        .otherwise("10+ miles")
    ) \
    .groupBy("distance_bucket").agg(
        count("*").alias("trips"),
        avg("total_amount").alias("avg_fare"),
        avg("trip_duration_minutes").alias("avg_duration"),
        avg("speed_mph").alias("avg_speed")
    )

# Order by custom sort
from pyspark.sql.functions import when

df_distance_sorted = df_distance_buckets.withColumn("sort_order",
    when(col("distance_bucket") == "< 1 mile", 1)
    .when(col("distance_bucket") == "1-3 miles", 2)
    .when(col("distance_bucket") == "3-5 miles", 3)
    .when(col("distance_bucket") == "5-10 miles", 4)
    .otherwise(5)
).orderBy("sort_order").drop("sort_order")

display(df_distance_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC Part 6: Location Analysis

# COMMAND ----------

# DBTITLE 1,Top Pickup Locations
# Round coordinates to create zones
df_pickup_zones = df_enhanced \
  .withColumn("pickup_zone",
              concat(
                round("pickup_latitude",2).cast("string"),
                round("pickup_longitude",2).cast("string")
              )
  ) \
  .groupBy("pickup_zone").agg(
    count("*").alias("pickups"),
    avg("total_amount").alias("avg_fare"),
    sum("total_amount").alias("total_fare")
  ).orderBy(col("pickups").desc())

print("Top 10 Pickup Zones by Volume: ")
display(df_pickup_zones.limit(10))

# COMMAND ----------

# DBTITLE 1,Top Dropoff Locations
df_dropoff_zones = df_enhanced \
    .withColumn("dropoff_zone",
        concat(
            round("dropoff_latitude", 2).cast("string"),
            lit(","),
            round("dropoff_longitude", 2).cast("string")
        )
    ) \
    .groupBy("dropoff_zone").agg(
        count("*").alias("dropoffs"),
        sum("total_amount").alias("revenue"),
        avg("total_amount").alias("avg_fare")
    ).orderBy(col("dropoffs").desc())

print("Top 10 dropoff zones by Volume:")
display(df_dropoff_zones.limit(10))

# COMMAND ----------

# DBTITLE 1,Most Popular Routes
df_routes = df_enhanced \
    .withColumn("pickup_zone",
        concat(
            round("pickup_latitude", 2).cast("string"),
            lit(","),
            round("pickup_longitude", 2).cast("string")
        )
    ) \
    .withColumn("dropoff_zone",
        concat(
            round("dropoff_latitude", 2).cast("string"),
            lit(","),
            round("dropoff_longitude", 2).cast("string")
        )
    ) \
    .groupBy("pickup_zone", "dropoff_zone").agg(
        count("*").alias("trips"),
        sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_fare"),
        avg("trip_distance").alias("avg_distance")
    ).filter(col("trips") > 100)

print("Top 20 routes by trip count:")
display(df_routes.orderBy(col("trips").desc()).limit(20))