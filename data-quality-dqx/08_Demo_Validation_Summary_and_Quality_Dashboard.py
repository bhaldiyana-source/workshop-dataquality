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

# Import required libraries
from dqx import Validator, MetricsCollector, CheckType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import random
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Collecting Quality Metrics
# MAGIC
# MAGIC ### 1.1 Configure MetricsCollector

# COMMAND ----------

# Create metrics collector
metrics_collector = MetricsCollector(
    target_table="dqx_demo.validation_metrics"
)

print("✅ MetricsCollector configured")
print(f"   Target table: dqx_demo.validation_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Create Validator with Metrics Collection

# COMMAND ----------

# Create validator
validator = Validator()
validator.set_metrics_collector(metrics_collector)

# Add quality checks
validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error"
)

validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error"
)

validator.add_check(
    name="valid_status",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "completed", "cancelled"],
    level="warning"
)

print("✅ Validator with metrics collection configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Generate Sample Data and Run Validations

# COMMAND ----------

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
    result_df, summary = validator.validate(batch_df, table_name="transactions")
    
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

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dqx_demo.overall_quality_score AS
# MAGIC SELECT 
# MAGIC   date_trunc('day', timestamp) as date,
# MAGIC   table_name,
# MAGIC   AVG(pass_rate) as daily_pass_rate,
# MAGIC   SUM(records_processed) as daily_records,
# MAGIC   SUM(records_failed) as daily_failures,
# MAGIC   COUNT(*) as validation_runs
# MAGIC FROM dqx_demo.validation_metrics
# MAGIC GROUP BY date_trunc('day', timestamp), table_name
# MAGIC ORDER BY date DESC;
# MAGIC
# MAGIC SELECT * FROM dqx_demo.overall_quality_score;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Table Quality Health View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dqx_demo.table_quality_health AS
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   AVG(pass_rate) * 100 as avg_pass_rate_pct,
# MAGIC   MIN(pass_rate) * 100 as min_pass_rate_pct,
# MAGIC   MAX(pass_rate) * 100 as max_pass_rate_pct,
# MAGIC   SUM(records_processed) as total_records,
# MAGIC   SUM(records_failed) as total_failures,
# MAGIC   COUNT(DISTINCT check_name) as total_checks,
# MAGIC   MAX(timestamp) as last_validated,
# MAGIC   COUNT(*) as validation_count
# MAGIC FROM dqx_demo.validation_metrics
# MAGIC WHERE timestamp >= current_date() - INTERVAL 7 DAYS
# MAGIC GROUP BY table_name;
# MAGIC
# MAGIC SELECT * FROM dqx_demo.table_quality_health;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Top Failing Checks View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dqx_demo.top_failing_checks AS
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   check_name,
# MAGIC   check_type,
# MAGIC   level,
# MAGIC   SUM(records_failed) as total_failures,
# MAGIC   COUNT(*) as failure_occurrences,
# MAGIC   AVG(pass_rate) * 100 as avg_pass_rate_pct,
# MAGIC   MAX(timestamp) as last_failure,
# MAGIC   MIN(timestamp) as first_failure
# MAGIC FROM dqx_demo.validation_metrics
# MAGIC WHERE records_failed > 0
# MAGIC   AND timestamp >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY table_name, check_name, check_type, level
# MAGIC ORDER BY total_failures DESC
# MAGIC LIMIT 20;
# MAGIC
# MAGIC SELECT * FROM dqx_demo.top_failing_checks;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Quality Trend Analysis View

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW dqx_demo.quality_trend_analysis AS
# MAGIC SELECT 
# MAGIC   date_trunc('day', timestamp) as date,
# MAGIC   table_name,
# MAGIC   check_name,
# MAGIC   AVG(pass_rate) * 100 as pass_rate_pct,
# MAGIC   SUM(records_processed) as records_processed,
# MAGIC   SUM(records_failed) as records_failed,
# MAGIC   COUNT(*) as validation_runs
# MAGIC FROM dqx_demo.validation_metrics
# MAGIC WHERE timestamp >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY date_trunc('day', timestamp), table_name, check_name
# MAGIC ORDER BY date DESC, table_name, check_name;
# MAGIC
# MAGIC SELECT * FROM dqx_demo.quality_trend_analysis LIMIT 50;

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
# MAGIC **Continue to**: `09_Demo_Advanced_Topics_and_Best_Practices`
