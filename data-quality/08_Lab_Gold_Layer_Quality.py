# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Gold Layer Quality
# MAGIC
# MAGIC **Module 8: Aggregation Validation and Consistency Checks**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC 1. **Reconciliation** - Validate aggregations match source data
# MAGIC 2. **Metric consistency** - Ensure derived metrics are correct
# MAGIC 3. **Completeness** - Check for gaps in time series
# MAGIC 4. **Trend analysis** - Detect anomalies in aggregated data
# MAGIC 5. **Business validation** - Apply gold-layer business rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import json

catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print("✅ Setup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver Test Data

# COMMAND ----------

# Create sample Silver data
silver_data = []
for i in range(1, 31):  # 30 days of data
    day = i
    num_orders = 10 + (i % 5) * 2  # Varying number of orders per day
    
    for j in range(num_orders):
        order_id = i * 100 + j
        silver_data.append((
            order_id,
            100 + (j % 10),  # customer_id
            f"2024-01-{day:02d}",
            f"customer{j}@example.com",
            100.0 + (j * 10.5),
            "COMPLETED",
            95 + (j % 5)
        ))

silver_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_date", StringType()),
    StructField("email", StringType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("quality_score", IntegerType())
])

df_silver = spark.createDataFrame(silver_data, silver_schema)
df_silver = df_silver.withColumn("order_date", col("order_date").cast("date"))
df_silver.write.format("delta").mode("overwrite").saveAsTable("orders_silver_for_gold")

print(f"✅ Created Silver test data with {df_silver.count()} records")
print(f"   Date range: {df_silver.agg(min('order_date')).collect()[0][0]} to {df_silver.agg(max('order_date')).collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Create Gold Aggregations

# COMMAND ----------

def create_gold_daily_summary(silver_table, gold_table):
    """
    Create Gold layer daily summary with key metrics
    """
    
    df_silver = spark.table(silver_table)
    
    # Daily aggregations
    df_gold = df_silver \
        .filter(col("status") == "COMPLETED") \
        .groupBy("order_date") \
        .agg(
            count("order_id").alias("order_count"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            min("amount").alias("min_order_value"),
            max("amount").alias("max_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("quality_score").alias("avg_quality_score")
        ) \
        .withColumn("revenue_per_customer", col("total_revenue") / col("unique_customers")) \
        .withColumn("created_timestamp", current_timestamp()) \
        .withColumn("created_date", current_date())
    
    # Write to Gold
    df_gold.write.format("delta").mode("overwrite").saveAsTable(gold_table)
    
    print(f"✅ Created Gold table: {gold_table}")
    print(f"   Records: {df_gold.count()}")
    
    return df_gold

df_gold = create_gold_daily_summary("orders_silver_for_gold", "orders_gold_daily")

display(df_gold.orderBy("order_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Reconciliation Validation
# MAGIC
# MAGIC Ensure Gold totals match Silver source

# COMMAND ----------

def validate_reconciliation(gold_table, silver_table):
    """
    Validate that Gold aggregations reconcile with Silver source
    """
    
    df_gold = spark.table(gold_table)
    df_silver = spark.table(silver_table).filter(col("status") == "COMPLETED")
    
    print("RECONCILIATION VALIDATION")
    print("="*80)
    
    validations = {}
    
    # Validation 1: Total revenue matches
    gold_total_revenue = df_gold.agg(sum("total_revenue")).collect()[0][0] or 0
    silver_total_revenue = df_silver.agg(sum("amount")).collect()[0][0] or 0
    
    diff_amount = abs(gold_total_revenue - silver_total_revenue)
    diff_pct = (diff_amount / silver_total_revenue * 100) if silver_total_revenue > 0 else 0
    
    validations['total_revenue_reconciliation'] = {
        'passed': diff_pct < 0.01,  # Less than 0.01% difference
        'gold_total': float(gold_total_revenue),
        'silver_total': float(silver_total_revenue),
        'difference': float(diff_amount),
        'difference_pct': float(diff_pct)
    }
    
    print(f"\n1. Total Revenue Reconciliation:")
    print(f"   Gold Total:   ${gold_total_revenue:,.2f}")
    print(f"   Silver Total: ${silver_total_revenue:,.2f}")
    print(f"   Difference:   ${diff_amount:,.2f} ({diff_pct:.4f}%)")
    print(f"   Status: {'✅ PASSED' if validations['total_revenue_reconciliation']['passed'] else '❌ FAILED'}")
    
    # Validation 2: Order count matches
    gold_total_orders = df_gold.agg(sum("order_count")).collect()[0][0] or 0
    silver_total_orders = df_silver.count()
    
    validations['order_count_reconciliation'] = {
        'passed': gold_total_orders == silver_total_orders,
        'gold_count': int(gold_total_orders),
        'silver_count': int(silver_total_orders),
        'difference': int(abs(gold_total_orders - silver_total_orders))
    }
    
    print(f"\n2. Order Count Reconciliation:")
    print(f"   Gold Count:   {gold_total_orders:,}")
    print(f"   Silver Count: {silver_total_orders:,}")
    print(f"   Status: {'✅ PASSED' if validations['order_count_reconciliation']['passed'] else '❌ FAILED'}")
    
    print("\n" + "="*80)
    
    overall_passed = all(v['passed'] for v in validations.values())
    print(f"Overall Reconciliation: {'✅ PASSED' if overall_passed else '❌ FAILED'}")
    
    return validations

reconciliation_results = validate_reconciliation("orders_gold_daily", "orders_silver_for_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Metric Consistency Validation
# MAGIC
# MAGIC Validate that derived metrics are mathematically correct

# COMMAND ----------

def validate_metric_consistency(gold_table):
    """
    Validate that derived metrics match their calculations
    """
    
    df_gold = spark.table(gold_table)
    
    print("METRIC CONSISTENCY VALIDATION")
    print("="*80)
    
    validations = {}
    
    # Check 1: avg_order_value = total_revenue / order_count
    df_check = df_gold.withColumn(
        "calculated_avg",
        col("total_revenue") / col("order_count")
    ).withColumn(
        "avg_difference",
        abs(col("calculated_avg") - col("avg_order_value"))
    )
    
    inconsistent_avg = df_check.filter(col("avg_difference") > 0.01).count()
    
    validations['avg_order_value_consistency'] = {
        'passed': inconsistent_avg == 0,
        'inconsistent_records': inconsistent_avg
    }
    
    print(f"\n1. Average Order Value Consistency:")
    print(f"   Formula: total_revenue / order_count")
    print(f"   Inconsistent records: {inconsistent_avg}")
    print(f"   Status: {'✅ PASSED' if validations['avg_order_value_consistency']['passed'] else '❌ FAILED'}")
    
    # Check 2: revenue_per_customer = total_revenue / unique_customers
    df_check2 = df_gold.withColumn(
        "calculated_rpc",
        col("total_revenue") / col("unique_customers")
    ).withColumn(
        "rpc_difference",
        abs(col("calculated_rpc") - col("revenue_per_customer"))
    )
    
    inconsistent_rpc = df_check2.filter(col("rpc_difference") > 0.01).count()
    
    validations['revenue_per_customer_consistency'] = {
        'passed': inconsistent_rpc == 0,
        'inconsistent_records': inconsistent_rpc
    }
    
    print(f"\n2. Revenue Per Customer Consistency:")
    print(f"   Formula: total_revenue / unique_customers")
    print(f"   Inconsistent records: {inconsistent_rpc}")
    print(f"   Status: {'✅ PASSED' if validations['revenue_per_customer_consistency']['passed'] else '❌ FAILED'}")
    
    # Check 3: Logical bounds
    illogical = df_gold.filter(
        (col("min_order_value") > col("avg_order_value")) |
        (col("max_order_value") < col("avg_order_value")) |
        (col("avg_order_value") < 0) |
        (col("order_count") < 0)
    ).count()
    
    validations['logical_bounds'] = {
        'passed': illogical == 0,
        'illogical_records': illogical
    }
    
    print(f"\n3. Logical Bounds:")
    print(f"   Checking: min <= avg <= max, counts >= 0")
    print(f"   Illogical records: {illogical}")
    print(f"   Status: {'✅ PASSED' if validations['logical_bounds']['passed'] else '❌ FAILED'}")
    
    print("\n" + "="*80)
    
    overall_passed = all(v['passed'] for v in validations.values())
    print(f"Overall Consistency: {'✅ PASSED' if overall_passed else '❌ FAILED'}")
    
    return validations

consistency_results = validate_metric_consistency("orders_gold_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Time Series Completeness
# MAGIC
# MAGIC Check for missing dates in the time series

# COMMAND ----------

def validate_time_series_completeness(gold_table, date_column="order_date"):
    """
    Validate that all expected dates are present in time series
    """
    
    df_gold = spark.table(gold_table)
    
    print("TIME SERIES COMPLETENESS VALIDATION")
    print("="*80)
    
    # Get actual date range
    date_range = df_gold.agg(
        min(date_column).alias("min_date"),
        max(date_column).alias("max_date")
    ).collect()[0]
    
    min_date = date_range.min_date
    max_date = date_range.max_date
    
    if not min_date or not max_date:
        print("❌ No dates found in data")
        return {'passed': False, 'missing_dates': None}
    
    print(f"\nDate Range: {min_date} to {max_date}")
    
    # Create expected date range
    expected_dates = spark.sql(f"""
        SELECT sequence(
            date'{min_date}',
            date'{max_date}',
            interval 1 day
        ) as date_range
    """).selectExpr("explode(date_range) as expected_date")
    
    # Find missing dates
    actual_dates = df_gold.select(col(date_column).alias("expected_date"))
    missing_dates = expected_dates.join(actual_dates, "expected_date", "left_anti")
    
    missing_count = missing_dates.count()
    expected_count = expected_dates.count()
    
    print(f"\nExpected dates: {expected_count}")
    print(f"Actual dates: {df_gold.count()}")
    print(f"Missing dates: {missing_count}")
    
    if missing_count > 0:
        print(f"\n⚠️  Missing dates:")
        for row in missing_dates.orderBy("expected_date").collect()[:10]:
            print(f"   - {row.expected_date}")
        if missing_count > 10:
            print(f"   ... and {missing_count - 10} more")
    
    validation = {
        'passed': missing_count == 0,
        'expected_count': expected_count,
        'actual_count': df_gold.count(),
        'missing_count': missing_count,
        'completeness_rate': ((expected_count - missing_count) / expected_count * 100) if expected_count > 0 else 0
    }
    
    print(f"\nCompleteness Rate: {validation['completeness_rate']:.2f}%")
    print(f"Status: {'✅ PASSED' if validation['passed'] else '❌ FAILED'}")
    print("="*80)
    
    return validation

completeness_results = validate_time_series_completeness("orders_gold_daily")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Trend Anomaly Detection
# MAGIC
# MAGIC Detect unusual changes in metrics

# COMMAND ----------

def detect_trend_anomalies(gold_table, metric_column, threshold_std=3):
    """
    Detect anomalies in time series using statistical methods
    """
    
    df_gold = spark.table(gold_table).orderBy("order_date")
    
    print(f"TREND ANOMALY DETECTION: {metric_column}")
    print("="*80)
    
    # Calculate statistics
    stats = df_gold.select(
        avg(metric_column).alias("mean"),
        stddev(metric_column).alias("stddev")
    ).collect()[0]
    
    mean_val = stats['mean'] or 0
    stddev_val = stats['stddev'] or 0
    
    print(f"\nMetric Statistics:")
    print(f"  Mean: {mean_val:.2f}")
    print(f"  Std Dev: {stddev_val:.2f}")
    print(f"  Threshold: ±{threshold_std} standard deviations")
    
    # Calculate z-scores
    df_with_zscore = df_gold.withColumn(
        "z_score",
        (col(metric_column) - mean_val) / stddev_val if stddev_val > 0 else lit(0)
    ).withColumn(
        "is_anomaly",
        abs(col("z_score")) > threshold_std
    )
    
    # Find anomalies
    anomalies = df_with_zscore.filter(col("is_anomaly")).orderBy("order_date")
    anomaly_count = anomalies.count()
    
    print(f"\nAnomalies Detected: {anomaly_count}")
    
    if anomaly_count > 0:
        print(f"\n⚠️  Anomalous periods:")
        for row in anomalies.collect():
            print(f"   {row.order_date}: {getattr(row, metric_column):.2f} (z-score: {row.z_score:.2f})")
    
    validation = {
        'passed': anomaly_count == 0,
        'anomaly_count': anomaly_count,
        'mean': float(mean_val),
        'stddev': float(stddev_val),
        'threshold_std': threshold_std
    }
    
    print(f"\nStatus: {'✅ NO ANOMALIES' if validation['passed'] else '⚠️  ANOMALIES DETECTED'}")
    print("="*80)
    
    return validation

# Detect anomalies in total revenue
revenue_anomalies = detect_trend_anomalies("orders_gold_daily", "total_revenue", threshold_std=2.5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Period-over-Period Comparison

# COMMAND ----------

def validate_period_over_period(gold_table, metric_column, max_change_pct=50):
    """
    Validate that day-over-day changes are within reasonable bounds
    """
    
    df_gold = spark.table(gold_table).orderBy("order_date")
    
    print(f"PERIOD-OVER-PERIOD VALIDATION: {metric_column}")
    print("="*80)
    
    # Calculate previous day's value
    window = Window.orderBy("order_date")
    
    df_with_prev = df_gold.withColumn(
        "prev_value",
        lag(metric_column).over(window)
    ).withColumn(
        "pct_change",
        when(col("prev_value").isNotNull() & (col("prev_value") != 0),
             ((col(metric_column) - col("prev_value")) / col("prev_value") * 100)
        ).otherwise(lit(None))
    ).withColumn(
        "is_large_change",
        abs(col("pct_change")) > max_change_pct
    )
    
    # Find large changes
    large_changes = df_with_prev.filter(col("is_large_change")).orderBy("order_date")
    large_change_count = large_changes.count()
    
    print(f"\nThreshold: {max_change_pct}% change")
    print(f"Large changes detected: {large_change_count}")
    
    if large_change_count > 0:
        print(f"\n⚠️  Periods with large changes:")
        for row in large_changes.collect():
            print(f"   {row.order_date}: {row.pct_change:.1f}% change ({row.prev_value:.2f} → {getattr(row, metric_column):.2f})")
    
    validation = {
        'passed': large_change_count == 0,
        'large_change_count': large_change_count,
        'threshold_pct': max_change_pct
    }
    
    print(f"\nStatus: {'✅ PASSED' if validation['passed'] else '⚠️  UNUSUAL CHANGES'}")
    print("="*80)
    
    return validation

pop_validation = validate_period_over_period("orders_gold_daily", "total_revenue", max_change_pct=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Complete Gold Validation Function

# COMMAND ----------

def validate_gold_quality(gold_table, silver_table):
    """
    Comprehensive Gold layer quality validation
    """
    
    print("\n" + "="*80)
    print("GOLD LAYER QUALITY VALIDATION")
    print("="*80)
    print()
    
    all_validations = {}
    
    # 1. Reconciliation
    print("1. RECONCILIATION")
    print("-"*80)
    all_validations['reconciliation'] = validate_reconciliation(gold_table, silver_table)
    print()
    
    # 2. Metric Consistency
    print("2. METRIC CONSISTENCY")
    print("-"*80)
    all_validations['consistency'] = validate_metric_consistency(gold_table)
    print()
    
    # 3. Time Series Completeness
    print("3. TIME SERIES COMPLETENESS")
    print("-"*80)
    all_validations['completeness'] = validate_time_series_completeness(gold_table)
    print()
    
    # 4. Trend Anomalies
    print("4. TREND ANOMALIES")
    print("-"*80)
    all_validations['anomalies'] = detect_trend_anomalies(gold_table, "total_revenue")
    print()
    
    # Overall status
    critical_checks = [
        all_validations['reconciliation']['total_revenue_reconciliation']['passed'],
        all_validations['reconciliation']['order_count_reconciliation']['passed'],
        all_validations['consistency']['avg_order_value_consistency']['passed'],
        all_validations['consistency']['logical_bounds']['passed']
    ]
    
    overall_passed = all(critical_checks)
    
    print("="*80)
    print(f"OVERALL GOLD QUALITY: {'✅ PASSED' if overall_passed else '❌ FAILED'}")
    print("="*80)
    
    return {
        'overall_passed': overall_passed,
        'validations': all_validations
    }

# Run complete Gold validation
gold_validation_results = validate_gold_quality("orders_gold_daily", "orders_silver_for_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Save Gold Quality Metrics

# COMMAND ----------

# Create Gold quality metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS gold_quality_metrics (
        validation_timestamp TIMESTAMP,
        gold_table STRING,
        silver_table STRING,
        validation_type STRING,
        passed BOOLEAN,
        metric_name STRING,
        metric_value DOUBLE,
        details STRING
    ) USING DELTA
""")

def save_gold_quality_metrics(gold_table, silver_table, validation_results):
    """
    Save Gold quality validation results
    """
    
    timestamp = datetime.now()
    records = []
    
    # Flatten validation results
    for val_type, val_data in validation_results['validations'].items():
        if isinstance(val_data, dict):
            for metric_name, metric_data in val_data.items():
                if isinstance(metric_data, dict) and 'passed' in metric_data:
                    record = {
                        'validation_timestamp': timestamp,
                        'gold_table': gold_table,
                        'silver_table': silver_table,
                        'validation_type': val_type,
                        'passed': metric_data['passed'],
                        'metric_name': metric_name,
                        'metric_value': None,
                        'details': json.dumps(metric_data)
                    }
                    records.append(record)
    
    if records:
        df_metrics = spark.createDataFrame(records)
        df_metrics.write.format("delta").mode("append").saveAsTable("gold_quality_metrics")
        print(f"✅ Saved {len(records)} Gold quality metrics")

save_gold_quality_metrics("orders_gold_daily", "orders_silver_for_gold", gold_validation_results)

display(spark.table("gold_quality_metrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Gold Layer Quality Principles
# MAGIC 1. **Reconciliation** - Always validate against source (Silver)
# MAGIC 2. **Metric Math** - Ensure derived metrics are calculated correctly
# MAGIC 3. **Completeness** - Check for gaps in time series/dimensions
# MAGIC 4. **Reasonableness** - Values should be within expected ranges
# MAGIC 5. **Trend Validation** - Detect anomalies and unusual changes
# MAGIC
# MAGIC ### What You Built
# MAGIC - Reconciliation validation (Gold vs Silver)
# MAGIC - Metric consistency checks
# MAGIC - Time series completeness validation
# MAGIC - Trend anomaly detection
# MAGIC - Period-over-period comparison
# MAGIC - Complete Gold validation pipeline
# MAGIC
# MAGIC ### Gold Quality Checks
# MAGIC ✅ **Critical**:
# MAGIC - Reconciliation with Silver
# MAGIC - Metric mathematical correctness
# MAGIC - No illogical values (negatives where impossible, min > max, etc.)
# MAGIC
# MAGIC ⚠️  **Important**:
# MAGIC - Time series completeness
# MAGIC - Trend anomalies
# MAGIC - Large period-over-period changes
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 6**: Great Expectations integration
# MAGIC - **Lab 7**: Quarantine & remediation workflows
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 45 minutes | **Level**: 300 | **Type**: Lab
