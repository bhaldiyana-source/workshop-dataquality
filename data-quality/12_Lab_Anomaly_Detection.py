# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Anomaly Detection
# MAGIC
# MAGIC **Module 12: Statistical and ML-Based Quality Anomaly Detection**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 50 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC 1. **Statistical Anomalies** - Z-score and IQR methods
# MAGIC 2. **Time Series Anomalies** - Trend-based detection
# MAGIC 3. **ML-Based Detection** - Unsupervised learning approaches
# MAGIC 4. **Alerting** - Automated anomaly notifications

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Time Series Data with Anomalies

# COMMAND ----------

# Generate 90 days of data with intentional anomalies
ts_data = []
base_date = datetime.now() - timedelta(days=90)

for days_ago in range(90):
    metric_date = base_date + timedelta(days=days_ago)
    
    # Normal pattern: around 1000 orders per day
    base_orders = 1000
    
    # Add weekly seasonality
    day_of_week = metric_date.weekday()
    if day_of_week >= 5:  # Weekend
        base_orders = int(base_orders * 0.7)
    
    # Add random noise
    orders = base_orders + random.randint(-100, 100)
    
    # Inject anomalies
    if days_ago == 30:  # Spike
        orders = orders * 3
    elif days_ago == 60:  # Drop
        orders = int(orders * 0.3)
    
    ts_data.append({
        'metric_date': metric_date.date(),
        'order_count': orders,
        'revenue': orders * random.uniform(45, 55),
        'avg_order_value': random.uniform(45, 55)
    })

df_ts = spark.createDataFrame(ts_data)
df_ts.write.format("delta").mode("overwrite").saveAsTable("daily_metrics_for_anomaly")

print(f"✅ Created time series with {len(ts_data)} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 1: Z-Score Anomaly Detection

# COMMAND ----------

def detect_zscore_anomalies(df, metric_column, threshold=3.0):
    """
    Detect anomalies using Z-score method
    """
    
    # Calculate mean and stddev
    stats = df.select(
        avg(metric_column).alias("mean"),
        stddev(metric_column).alias("stddev")
    ).collect()[0]
    
    mean_val = stats["mean"]
    stddev_val = stats["stddev"]
    
    print(f"Z-SCORE ANOMALY DETECTION: {metric_column}")
    print("="*80)
    print(f"Mean: {mean_val:.2f}")
    print(f"Std Dev: {stddev_val:.2f}")
    print(f"Threshold: ±{threshold} standard deviations")
    print()
    
    # Calculate z-scores
    df_with_zscore = df.withColumn(
        "z_score",
        (col(metric_column) - mean_val) / stddev_val
    ).withColumn(
        "is_anomaly",
        abs(col("z_score")) > threshold
    )
    
    # Find anomalies
    anomalies = df_with_zscore.filter(col("is_anomaly")).orderBy("metric_date")
    anomaly_count = anomalies.count()
    
    print(f"Anomalies Detected: {anomaly_count}")
    print()
    
    if anomaly_count > 0:
        print("Anomalous Periods:")
        for row in anomalies.collect():
            print(f"  {row.metric_date}: {getattr(row, metric_column):.2f} (z-score: {row.z_score:.2f})")
    
    print("="*80)
    
    return df_with_zscore

df_zscore = detect_zscore_anomalies(spark.table("daily_metrics_for_anomaly"), "order_count", threshold=2.5)

display(df_zscore.filter(col("is_anomaly")).select("metric_date", "order_count", "z_score", "is_anomaly"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 2: IQR (Interquartile Range) Method

# COMMAND ----------

def detect_iqr_anomalies(df, metric_column):
    """
    Detect anomalies using IQR method
    """
    
    # Calculate quartiles
    quantiles = df.stat.approxQuantile(metric_column, [0.25, 0.5, 0.75], 0.05)
    q1, median, q3 = quantiles[0], quantiles[1], quantiles[2]
    iqr = q3 - q1
    
    lower_bound = q1 - (1.5 * iqr)
    upper_bound = q3 + (1.5 * iqr)
    
    print(f"IQR ANOMALY DETECTION: {metric_column}")
    print("="*80)
    print(f"Q1: {q1:.2f}")
    print(f"Median: {median:.2f}")
    print(f"Q3: {q3:.2f}")
    print(f"IQR: {iqr:.2f}")
    print(f"Lower Bound: {lower_bound:.2f}")
    print(f"Upper Bound: {upper_bound:.2f}")
    print()
    
    # Detect outliers
    df_with_outliers = df.withColumn(
        "is_outlier",
        (col(metric_column) < lower_bound) | (col(metric_column) > upper_bound)
    )
    
    outliers = df_with_outliers.filter(col("is_outlier")).orderBy("metric_date")
    outlier_count = outliers.count()
    
    print(f"Outliers Detected: {outlier_count}")
    
    if outlier_count > 0:
        print("\nOutlier Periods:")
        for row in outliers.collect():
            print(f"  {row.metric_date}: {getattr(row, metric_column):.2f}")
    
    print("="*80)
    
    return df_with_outliers

df_iqr = detect_iqr_anomalies(spark.table("daily_metrics_for_anomaly"), "order_count")

display(df_iqr.filter(col("is_outlier")).select("metric_date", "order_count", "is_outlier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 3: Moving Average / Time Series Method

# COMMAND ----------

def detect_time_series_anomalies(df, date_col, metric_col, window_days=7, threshold_std=2.0):
    """
    Detect anomalies using moving average and standard deviation
    """
    
    print(f"TIME SERIES ANOMALY DETECTION: {metric_col}")
    print("="*80)
    print(f"Window: {window_days} days")
    print(f"Threshold: {threshold_std} standard deviations")
    print()
    
    # Define window
    window = Window.orderBy(date_col).rowsBetween(-window_days, 0)
    
    # Calculate moving statistics
    df_with_ma = df.withColumn(
        "moving_avg",
        avg(metric_col).over(window)
    ).withColumn(
        "moving_stddev",
        stddev(metric_col).over(window)
    ).withColumn(
        "deviation",
        col(metric_col) - col("moving_avg")
    ).withColumn(
        "is_anomaly",
        abs(col("deviation")) > (threshold_std * col("moving_stddev"))
    )
    
    # Find anomalies
    anomalies = df_with_ma.filter(col("is_anomaly")).orderBy(date_col)
    anomaly_count = anomalies.count()
    
    print(f"Anomalies Detected: {anomaly_count}")
    
    if anomaly_count > 0:
        print("\nAnomalous Periods:")
        for row in anomalies.collect():
            print(f"  {getattr(row, date_col)}: {getattr(row, metric_col):.2f} (MA: {row.moving_avg:.2f}, Dev: {row.deviation:.2f})")
    
    print("="*80)
    
    return df_with_ma

df_ts_anomaly = detect_time_series_anomalies(
    spark.table("daily_metrics_for_anomaly"),
    "metric_date",
    "order_count",
    window_days=7,
    threshold_std=2.0
)

display(df_ts_anomaly.select("metric_date", "order_count", "moving_avg", "moving_stddev", "deviation", "is_anomaly"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Method 4: Combined Anomaly Score

# COMMAND ----------

def calculate_combined_anomaly_score(df, metric_col):
    """
    Combine multiple methods for robust anomaly detection
    """
    
    # Method 1: Z-score
    stats = df.select(avg(metric_col).alias("mean"), stddev(metric_col).alias("stddev")).collect()[0]
    mean_val, std_val = stats["mean"], stats["stddev"]
    
    df_combined = df.withColumn(
        "z_score",
        abs((col(metric_col) - mean_val) / std_val)
    )
    
    # Method 2: IQR
    quantiles = df.stat.approxQuantile(metric_col, [0.25, 0.75], 0.05)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    lower, upper = q1 - (1.5 * iqr), q3 + (1.5 * iqr)
    
    df_combined = df_combined.withColumn(
        "iqr_anomaly",
        when((col(metric_col) < lower) | (col(metric_col) > upper), 1).otherwise(0)
    )
    
    # Method 3: Moving average
    window = Window.orderBy("metric_date").rowsBetween(-7, 0)
    df_combined = df_combined.withColumn(
        "moving_avg",
        avg(metric_col).over(window)
    ).withColumn(
        "ma_deviation",
        abs((col(metric_col) - col("moving_avg")) / col("moving_avg")) * 100
    )
    
    # Combined score (0-100, higher = more anomalous)
    df_combined = df_combined.withColumn(
        "anomaly_score",
        (
            when(col("z_score") > 3, 40).when(col("z_score") > 2, 20).otherwise(0) +
            when(col("iqr_anomaly") == 1, 30).otherwise(0) +
            when(col("ma_deviation") > 50, 30).when(col("ma_deviation") > 30, 15).otherwise(0)
        )
    ).withColumn(
        "anomaly_level",
        when(col("anomaly_score") >= 70, "HIGH")
        .when(col("anomaly_score") >= 40, "MEDIUM")
        .when(col("anomaly_score") > 0, "LOW")
        .otherwise("NORMAL")
    )
    
    return df_combined

df_combined = calculate_combined_anomaly_score(
    spark.table("daily_metrics_for_anomaly"),
    "order_count"
)

print("\nANOMALY SCORE SUMMARY")
print("="*80)
display(df_combined.groupBy("anomaly_level").count().orderBy("anomaly_level"))

print("\nHigh Anomalies:")
display(df_combined.filter(col("anomaly_level") == "HIGH").select(
    "metric_date", "order_count", "z_score", "iqr_anomaly", "ma_deviation", "anomaly_score", "anomaly_level"
).orderBy("metric_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Anomaly Detection Results

# COMMAND ----------

# Create anomaly detection results table
spark.sql("""
    CREATE TABLE IF NOT EXISTS anomaly_detection_results (
        detection_timestamp TIMESTAMP,
        metric_date DATE,
        table_name STRING,
        metric_name STRING,
        metric_value DOUBLE,
        anomaly_score DOUBLE,
        anomaly_level STRING,
        detection_method STRING
    ) USING DELTA
""")

def save_anomaly_results(df_anomalies, table_name, metric_name):
    """
    Save anomaly detection results
    """
    
    df_to_save = df_anomalies.filter(col("anomaly_level") != "NORMAL").select(
        lit(datetime.now()).alias("detection_timestamp"),
        col("metric_date"),
        lit(table_name).alias("table_name"),
        lit(metric_name).alias("metric_name"),
        col(metric_name).alias("metric_value"),
        col("anomaly_score"),
        col("anomaly_level"),
        lit("combined_method").alias("detection_method")
    )
    
    df_to_save.write.format("delta").mode("append").saveAsTable("anomaly_detection_results")
    
    print(f"✅ Saved {df_to_save.count()} anomaly detections")

save_anomaly_results(df_combined, "daily_metrics_for_anomaly", "order_count")

display(spark.table("anomaly_detection_results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Anomaly Detection Methods
# MAGIC 1. **Z-Score** - Simple, assumes normal distribution
# MAGIC 2. **IQR** - Robust to outliers, no distribution assumption
# MAGIC 3. **Moving Average** - Good for time series with trends
# MAGIC 4. **Combined Score** - Most robust, reduces false positives
# MAGIC
# MAGIC ### What You Built
# MAGIC - Statistical anomaly detection (Z-score, IQR)
# MAGIC - Time series anomaly detection
# MAGIC - Combined anomaly scoring
# MAGIC - Anomaly tracking and storage
# MAGIC
# MAGIC ### Best Practices
# MAGIC - Use multiple methods for robustness
# MAGIC - Tune thresholds based on your data
# MAGIC - Consider seasonality and trends
# MAGIC - Track false positives and adjust
# MAGIC - Alert on HIGH severity only
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 10**: End-to-end production pipeline
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 50 minutes | **Level**: 300 | **Type**: Lab
