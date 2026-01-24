# Databricks notebook source
# MAGIC %md
# MAGIC # Validation Summary and Quality Dashboard
# MAGIC
# MAGIC **Module 9: Monitoring and Reporting Data Quality**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ### ⚠️ Implementation Note
# MAGIC
# MAGIC This notebook has been adapted to use **PySpark-based validation** instead of the conceptual `dqx` API shown in the original demo.
# MAGIC
# MAGIC **Key Changes:**
# MAGIC * Uses PySpark DataFrame operations for data quality checks
# MAGIC * Implements custom validation functions that replicate DQX concepts
# MAGIC * Stores metrics in Delta tables for analysis
# MAGIC * All quality monitoring and dashboard functionality remains the same
# MAGIC
# MAGIC **Why?** The `databricks-labs-dqx` package uses a different API structure (`DQEngine`, `DQRule`, `Criticality`) than the simplified demo API (`Validator`, `MetricsCollector`, `CheckType`). This implementation demonstrates the same data quality concepts using working PySpark code.
# MAGIC
# MAGIC **For production use**, refer to the [databricks-labs-dqx documentation](https://databrickslabs.github.io/dqx/) for the actual API.

# COMMAND ----------

# DBTITLE 1,Install DQX Framework
# MAGIC %pip install databricks-labs-dqx

# COMMAND ----------

# DBTITLE 1,Restart Python Kernel
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Collect quality metrics** from validations
# MAGIC 2. **Build summary views** for quality monitoring
# MAGIC 3. **Create quality dashboards** with SQL
# MAGIC 4. **Analyze quality trends** over time
# MAGIC 5. **Set up quality alerts** for degradation

# COMMAND ----------

# DBTITLE 1,Cell 3
# Import required libraries
# Note: This demo uses PySpark for data quality validation
# The simplified 'dqx' API shown in the original notebook is conceptual
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import random
from datetime import datetime, timedelta

print("✅ Libraries imported successfully!")
print("Using PySpark for data quality validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Collecting Quality Metrics
# MAGIC
# MAGIC ### 1.1 Configure MetricsCollector

# COMMAND ----------

# DBTITLE 1,Cell 5
# Create metrics table for storing validation results
# This replaces the MetricsCollector concept

metrics_table = "dqx_demo.validation_metrics"

# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS dqx_demo")

# Create metrics table schema
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {metrics_table} (
  timestamp TIMESTAMP,
  table_name STRING,
  check_name STRING,
  check_type STRING,
  level STRING,
  records_processed BIGINT,
  records_failed BIGINT,
  pass_rate DOUBLE
)
USING DELTA
""")

print("✅ Metrics table configured")
print(f"   Target table: {metrics_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create Validator with Metrics Collection

# COMMAND ----------

# DBTITLE 1,Cell 7
# Define quality check functions
# This replaces the Validator concept

def validate_data(df, table_name="unknown"):
    """
    Apply quality checks to a DataFrame and collect metrics
    Returns: (validated_df, summary_dict)
    """
    from pyspark.sql import DataFrame
    
    total_records = df.count()
    
    # Add quality check columns
    validated_df = df
    
    # Check 1: customer_id NOT NULL
    validated_df = validated_df.withColumn(
        "check_customer_id_not_null",
        F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
    )
    
    # Check 2: amount > 0
    validated_df = validated_df.withColumn(
        "check_amount_positive",
        F.when(F.col("amount") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
    )
    
    # Check 3: status in valid set
    validated_df = validated_df.withColumn(
        "check_valid_status",
        F.when(
            F.col("status").isin(["pending", "completed", "cancelled"]),
            F.lit("PASSED")
        ).otherwise(F.lit("FAILED"))
    )
    
    # Collect metrics for each check
    checks = [
        ("customer_id_not_null", "NOT_NULL", "error"),
        ("amount_positive", "GREATER_THAN", "error"),
        ("valid_status", "IN_SET", "warning")
    ]
    
    metrics_data = []
    failed_count = 0
    
    for check_name, check_type, level in checks:
        check_col = f"check_{check_name}"
        failures = validated_df.filter(F.col(check_col) == "FAILED").count()
        failed_count += failures
        pass_rate = (total_records - failures) / total_records if total_records > 0 else 1.0
        
        metrics_data.append((
            datetime.now(),
            table_name,
            check_name,
            check_type,
            level,
            total_records,
            failures,
            pass_rate
        ))
    
    # Save metrics to table
    metrics_df = spark.createDataFrame(
        metrics_data,
        ["timestamp", "table_name", "check_name", "check_type", "level",
         "records_processed", "records_failed", "pass_rate"]
    )
    metrics_df.write.mode("append").saveAsTable(metrics_table)
    
    # Create summary
    summary = {
        "total_records": total_records,
        "failed_records": failed_count,
        "pass_rate": (total_records - failed_count) / (total_records * len(checks)) if total_records > 0 else 1.0
    }
    
    return validated_df, summary

print("✅ Validation function configured")
print("   Checks: customer_id_not_null, amount_positive, valid_status")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Generate Sample Data and Run Validations

# COMMAND ----------

# DBTITLE 1,Cell 9
# Function to generate sample transaction data
def generate_transactions(batch_id, num_records=100, error_rate=0.1):
    """Generate sample transactions with controlled error rate"""
    transactions = []
    base_date = datetime.now() - timedelta(days=30-batch_id)
    
    for i in range(num_records):
        # Introduce errors based on error_rate
        if random.random() < error_rate:
            # Create record with issue
            issue_type = random.choice(['null_customer', 'negative_amount', 'invalid_status'])
            
            if issue_type == 'null_customer':
                record = (i+1, f"TXN{batch_id}{i:04d}", None, random.uniform(100, 5000), 
                         random.choice(['pending', 'completed']), base_date + timedelta(hours=i))
            elif issue_type == 'negative_amount':
                record = (i+1, f"TXN{batch_id}{i:04d}", f"CUST{random.randint(1,100):04d}", 
                         random.uniform(-500, -10), random.choice(['pending', 'completed']), 
                         base_date + timedelta(hours=i))
            else:  # invalid_status
                record = (i+1, f"TXN{batch_id}{i:04d}", f"CUST{random.randint(1,100):04d}", 
                         random.uniform(100, 5000), 'invalid', base_date + timedelta(hours=i))
        else:
            # Create valid record
            record = (i+1, f"TXN{batch_id}{i:04d}", f"CUST{random.randint(1,100):04d}", 
                     random.uniform(100, 5000), random.choice(['pending', 'completed', 'cancelled']), 
                     base_date + timedelta(hours=i))
        
        transactions.append(record)
    
    return spark.createDataFrame(
        transactions,
        ["id", "transaction_id", "customer_id", "amount", "status", "timestamp"]
    )

# Generate and validate multiple batches
print("Generating and validating transaction batches...\n")

for batch_id in range(1, 11):
    # Generate batch with varying error rates
    error_rate = 0.05 + (batch_id * 0.01)  # Increasing error rate
    batch_df = generate_transactions(batch_id, num_records=100, error_rate=error_rate)
    
    # Run validation (metrics automatically collected)
    result_df, summary = validate_data(batch_df, table_name="transactions")
    
    print(f"Batch {batch_id}:")
    print(f"  Records: {summary.get('total_records', 0)}")
    print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")
    print(f"  Failed: {summary.get('failed_records', 0)}")

print("\n✅ Generated and validated 10 batches")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Query and Analyze Metrics
# MAGIC
# MAGIC ### 2.1 View Raw Metrics Data

# COMMAND ----------

# Query validation metrics
metrics_df = spark.table("dqx_demo.validation_metrics")

print(f"Total validation records: {metrics_df.count()}")
display(metrics_df.orderBy(F.desc("timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Summary Statistics

# COMMAND ----------

# Calculate overall summary
summary_stats = (
    metrics_df
    .agg(
        F.sum("records_processed").alias("total_processed"),
        F.sum("records_failed").alias("total_failed"),
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.min("pass_rate").alias("min_pass_rate"),
        F.max("pass_rate").alias("max_pass_rate"),
        F.count("*").alias("validation_runs")
    )
)

print("Overall Quality Summary:")
display(summary_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Quality Dashboard Views
# MAGIC
# MAGIC ### 3.1 Overall Quality Score View

# COMMAND ----------

# DBTITLE 1,Overall Quality Score
# Overall Quality Score Analysis
overall_quality = spark.sql("""
SELECT 
  date_trunc('day', timestamp) as date,
  table_name,
  AVG(pass_rate) as daily_pass_rate,
  SUM(records_processed) as daily_records,
  SUM(records_failed) as daily_failures,
  COUNT(*) as validation_runs
FROM dqx_demo.validation_metrics
GROUP BY date_trunc('day', timestamp), table_name
ORDER BY date DESC
""")

print("✅ Overall Quality Score:")
display(overall_quality)

# COMMAND ----------

# DBTITLE 1,Table Quality Health
# Table Quality Health Analysis
table_health = spark.sql("""
SELECT 
  table_name,
  AVG(pass_rate) * 100 as avg_pass_rate_pct,
  MIN(pass_rate) * 100 as min_pass_rate_pct,
  MAX(pass_rate) * 100 as max_pass_rate_pct,
  SUM(records_processed) as total_records,
  SUM(records_failed) as total_failures,
  COUNT(DISTINCT check_name) as total_checks,
  MAX(timestamp) as last_validated,
  COUNT(*) as validation_count
FROM dqx_demo.validation_metrics
WHERE timestamp >= current_date() - INTERVAL 7 DAYS
GROUP BY table_name
""")

print("✅ Table Quality Health:")
display(table_health)

# COMMAND ----------

# DBTITLE 1,Top Failing Checks
# Top Failing Checks Analysis
top_failures = spark.sql("""
SELECT 
  table_name,
  check_name,
  check_type,
  level,
  SUM(records_failed) as total_failures,
  COUNT(*) as failure_occurrences,
  AVG(pass_rate) * 100 as avg_pass_rate_pct,
  MAX(timestamp) as last_failure,
  MIN(timestamp) as first_failure
FROM dqx_demo.validation_metrics
WHERE records_failed > 0
  AND timestamp >= current_date() - INTERVAL 30 DAYS
GROUP BY table_name, check_name, check_type, level
ORDER BY total_failures DESC
LIMIT 20
""")

print("✅ Top Failing Checks:")
display(top_failures)

# COMMAND ----------

# DBTITLE 1,Quality Trend Analysis
# Quality Trend Analysis
quality_trends = spark.sql("""
SELECT 
  date_trunc('day', timestamp) as date,
  table_name,
  check_name,
  AVG(pass_rate) * 100 as pass_rate_pct,
  SUM(records_processed) as records_processed,
  SUM(records_failed) as records_failed,
  COUNT(*) as validation_runs
FROM dqx_demo.validation_metrics
WHERE timestamp >= current_date() - INTERVAL 30 DAYS
GROUP BY date_trunc('day', timestamp), table_name, check_name
ORDER BY date DESC, table_name, check_name
LIMIT 50
""")

print("✅ Quality Trend Analysis:")
display(quality_trends)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Table Quality Health View

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Top Failing Checks View

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Quality Trend Analysis View

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Advanced Analytics
# MAGIC
# MAGIC ### 4.1 Quality Score Over Time

# COMMAND ----------

# Calculate quality score trend
quality_trend = (
    metrics_df
    .withColumn("date", F.to_date("timestamp"))
    .groupBy("date", "table_name")
    .agg(
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.sum("records_processed").alias("total_records"),
        F.sum("records_failed").alias("total_failed")
    )
    .orderBy("date", "table_name")
)

print("Quality Trend Over Time:")
display(quality_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Check-Level Performance

# COMMAND ----------

# Analyze performance by check
check_performance = (
    metrics_df
    .groupBy("table_name", "check_name", "check_type", "level")
    .agg(
        F.sum("records_processed").alias("total_processed"),
        F.sum("records_failed").alias("total_failed"),
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.min("pass_rate").alias("min_pass_rate"),
        F.count("*").alias("run_count")
    )
    .orderBy(F.desc("total_failed"))
)

print("Check-Level Performance:")
display(check_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Quality Degradation Detection

# COMMAND ----------

# Detect quality degradation using window functions
degradation_analysis = (
    metrics_df
    .withColumn("date", F.to_date("timestamp"))
    .groupBy("date", "table_name", "check_name")
    .agg(F.avg("pass_rate").alias("daily_pass_rate"))
    .withColumn(
        "prev_pass_rate",
        F.lag("daily_pass_rate", 1).over(
            Window.partitionBy("table_name", "check_name").orderBy("date")
        )
    )
    .withColumn(
        "pass_rate_change",
        F.col("daily_pass_rate") - F.col("prev_pass_rate")
    )
    .filter("pass_rate_change < -0.05")  # Alert if > 5% degradation
    .orderBy(F.desc("pass_rate_change"))
)

print("Quality Degradation Alerts (>5% drop):")
display(degradation_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Validation Run Statistics

# COMMAND ----------

# Analyze validation execution patterns
run_stats = (
    metrics_df
    .withColumn("hour", F.hour("timestamp"))
    .groupBy("table_name", "hour")
    .agg(
        F.count("*").alias("run_count"),
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.sum("records_processed").alias("total_records")
    )
    .orderBy("table_name", "hour")
)

print("Validation Run Patterns by Hour:")
display(run_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Quality KPIs and Metrics
# MAGIC
# MAGIC ### 5.1 Key Performance Indicators

# COMMAND ----------

# Calculate KPIs
kpis = metrics_df.agg(
    # Overall metrics
    F.sum("records_processed").alias("total_records_processed"),
    F.sum("records_failed").alias("total_records_failed"),
    
    # Quality rates
    (F.sum("records_processed") - F.sum("records_failed")).alias("total_records_passed"),
    (1 - F.sum("records_failed") / F.sum("records_processed")).alias("overall_pass_rate"),
    
    # Validation coverage
    F.countDistinct("table_name").alias("tables_validated"),
    F.countDistinct("check_name").alias("unique_checks"),
    F.count("*").alias("total_validations"),
    
    # Time ranges
    F.min("timestamp").alias("first_validation"),
    F.max("timestamp").alias("last_validation")
).collect()[0]

print("=" * 60)
print("DATA QUALITY KEY PERFORMANCE INDICATORS")
print("=" * 60)
print(f"\n📊 Volume Metrics:")
print(f"   Total Records Processed: {kpis['total_records_processed']:,}")
print(f"   Total Records Passed: {kpis['total_records_passed']:,}")
print(f"   Total Records Failed: {kpis['total_records_failed']:,}")
print(f"\n✅ Quality Metrics:")
print(f"   Overall Pass Rate: {kpis['overall_pass_rate']:.2%}")
print(f"\n🔍 Coverage Metrics:")
print(f"   Tables Validated: {kpis['tables_validated']}")
print(f"   Unique Checks: {kpis['unique_checks']}")
print(f"   Total Validations: {kpis['total_validations']}")
print(f"\n📅 Time Range:")
print(f"   First Validation: {kpis['first_validation']}")
print(f"   Last Validation: {kpis['last_validation']}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Quality Scorecard

# COMMAND ----------

# Create quality scorecard
scorecard = (
    metrics_df
    .groupBy("table_name")
    .agg(
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.min("pass_rate").alias("worst_pass_rate"),
        F.max("pass_rate").alias("best_pass_rate"),
        F.sum("records_processed").alias("total_records"),
        F.sum("records_failed").alias("total_failures")
    )
    .withColumn("quality_grade",
        F.when(F.col("avg_pass_rate") >= 0.99, "A+")
        .when(F.col("avg_pass_rate") >= 0.95, "A")
        .when(F.col("avg_pass_rate") >= 0.90, "B")
        .when(F.col("avg_pass_rate") >= 0.80, "C")
        .otherwise("F")
    )
    .select(
        "table_name",
        (F.col("avg_pass_rate") * 100).alias("avg_pass_rate_pct"),
        (F.col("worst_pass_rate") * 100).alias("worst_pass_rate_pct"),
        (F.col("best_pass_rate") * 100).alias("best_pass_rate_pct"),
        "total_records",
        "total_failures",
        "quality_grade"
    )
    .orderBy(F.desc("avg_pass_rate_pct"))
)

print("Quality Scorecard:")
display(scorecard)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Alerting and Notifications
# MAGIC
# MAGIC ### 6.1 Quality Alert Rules

# COMMAND ----------

# Define alert thresholds
ALERT_THRESHOLDS = {
    "critical_pass_rate": 0.90,  # Alert if pass rate < 90%
    "warning_pass_rate": 0.95,   # Warn if pass rate < 95%
    "failure_spike": 50,         # Alert if failures > 50 in single run
    "degradation_pct": 0.05      # Alert if 5%+ degradation
}

# Check for alerts
recent_metrics = metrics_df.filter("timestamp >= current_timestamp() - INTERVAL 1 HOUR")

alerts = (
    recent_metrics
    .filter(
        (F.col("pass_rate") < ALERT_THRESHOLDS["critical_pass_rate"]) |
        (F.col("records_failed") > ALERT_THRESHOLDS["failure_spike"])
    )
    .select(
        "timestamp",
        "table_name",
        "check_name",
        "pass_rate",
        "records_failed",
        F.when(F.col("pass_rate") < ALERT_THRESHOLDS["critical_pass_rate"], "LOW_PASS_RATE")
        .when(F.col("records_failed") > ALERT_THRESHOLDS["failure_spike"], "HIGH_FAILURE_COUNT")
        .alias("alert_type")
    )
    .orderBy(F.desc("timestamp"))
)

print(f"Quality Alerts (last hour):")
display(alerts)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Alert Summary

# COMMAND ----------

# Summarize alerts by type
alert_summary = (
    alerts
    .groupBy("alert_type", "table_name")
    .agg(
        F.count("*").alias("alert_count"),
        F.min("pass_rate").alias("lowest_pass_rate"),
        F.max("records_failed").alias("max_failures")
    )
    .orderBy(F.desc("alert_count"))
)

print("Alert Summary:")
display(alert_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Dashboard Visualizations
# MAGIC
# MAGIC ### 7.1 Time Series Visualization

# COMMAND ----------

# Prepare data for time series visualization
timeseries_data = (
    metrics_df
    .withColumn("date_hour", F.date_trunc("hour", "timestamp"))
    .groupBy("date_hour", "table_name")
    .agg(
        F.avg("pass_rate").alias("pass_rate"),
        F.sum("records_processed").alias("records_processed"),
        F.sum("records_failed").alias("records_failed")
    )
    .orderBy("date_hour")
)

print("Time Series Data for Visualization:")
display(timeseries_data)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Check Distribution

# COMMAND ----------

# Analyze check type distribution
check_distribution = (
    metrics_df
    .groupBy("check_type", "level")
    .agg(
        F.count("*").alias("check_count"),
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.sum("records_failed").alias("total_failures")
    )
    .orderBy(F.desc("check_count"))
)

print("Check Type Distribution:")
display(check_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **MetricsCollector** automatically tracks validation results
# MAGIC
# MAGIC ✅ **SQL views** provide easy access to quality metrics
# MAGIC
# MAGIC ✅ **Trend analysis** identifies quality degradation
# MAGIC
# MAGIC ✅ **KPIs and scorecards** communicate quality status
# MAGIC
# MAGIC ✅ **Alerting rules** enable proactive quality management
# MAGIC
# MAGIC ✅ **Dashboards visualize** quality trends and patterns
# MAGIC
# MAGIC ✅ **Historical tracking** supports continuous improvement

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Collect metrics consistently** - Enable for all validations
# MAGIC 2. **Create reusable views** - Standard KPIs across teams
# MAGIC 3. **Set appropriate thresholds** - Balance sensitivity and noise
# MAGIC 4. **Monitor trends** - Don't just look at point-in-time
# MAGIC 5. **Automate alerts** - Integrate with notification systems
# MAGIC 6. **Review regularly** - Weekly quality reviews with stakeholders
# MAGIC 7. **Act on insights** - Use metrics to improve data quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Advanced topics** - Custom checks, multi-table validation
# MAGIC 2. **Performance optimization** strategies
# MAGIC 3. **CI/CD integration** for quality checks
# MAGIC 4. **Quality rule versioning** and management
# MAGIC
# MAGIC **Continue to**: `09_Advanced_Topics_and_Best_Practices`
