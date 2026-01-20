# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Quality Dashboard
# MAGIC
# MAGIC **Hands-On Lab Exercise 7**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 75 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Objectives
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Collect comprehensive quality metrics
# MAGIC 2. Create dashboard views with SQL
# MAGIC 3. Build trend analysis queries
# MAGIC 4. Configure quality alerts
# MAGIC 5. Generate executive quality reports

# COMMAND ----------

# Import required libraries
from dqx import Validator, MetricsCollector, CheckType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import random
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Generate Historical Quality Metrics

# COMMAND ----------

# TODO: Generate 30 days of quality metrics for various tables
# Simulate realistic quality trends

def generate_historical_metrics(num_days=30):
    """
    Generate historical quality metrics for dashboard
    """
    metrics = []
    base_date = datetime.now() - timedelta(days=num_days)
    
    tables = ["transactions", "customers", "products", "orders"]
    check_types = ["not_null", "unique", "range", "in_set", "regex"]
    
    for day in range(num_days):
        date = base_date + timedelta(days=day)
        
        for table in tables:
            # Simulate degrading quality over time for some tables
            base_pass_rate = 0.95 if table in ["transactions", "customers"] else 0.98
            # Add some degradation trend
            pass_rate = base_pass_rate - (day * 0.001) + random.uniform(-0.02, 0.02)
            pass_rate = max(0.85, min(0.99, pass_rate))  # Keep in reasonable range
            
            records_processed = random.randint(5000, 15000)
            records_failed = int(records_processed * (1 - pass_rate))
            
            for check_type in random.sample(check_types, k=random.randint(2, 4)):
                metrics.append({
                    "timestamp": date,
                    "table_name": table,
                    "check_name": f"{table}_{check_type}_check",
                    "check_type": check_type,
                    "level": "error" if check_type in ["not_null", "unique"] else "warning",
                    "records_processed": records_processed,
                    "records_failed": random.randint(0, records_failed),
                    "pass_rate": pass_rate
                })
    
    return spark.createDataFrame(metrics)

# Generate metrics
historical_metrics = generate_historical_metrics(30)

# Save to table
historical_metrics.write.mode("overwrite").saveAsTable("dqx_lab.historical_quality_metrics")

print(f"✅ Generated {historical_metrics.count()} historical metric records")
display(historical_metrics.orderBy(F.desc("timestamp")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Basic Dashboard Views (20 minutes)
# MAGIC
# MAGIC ### Task 1.1: Overall Quality Score View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create a view showing overall quality score by day
# MAGIC -- Include: date, daily_pass_rate, total_records, total_failures
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dqx_lab.overall_quality_score AS
# MAGIC -- Your SQL here
# MAGIC SELECT 
# MAGIC   date_trunc('day', timestamp) as date,
# MAGIC   AVG(pass_rate) as daily_pass_rate,
# MAGIC   SUM(records_processed) as total_records,
# MAGIC   SUM(records_failed) as total_failures
# MAGIC FROM dqx_lab.historical_quality_metrics
# MAGIC GROUP BY date_trunc('day', timestamp)
# MAGIC ORDER BY date DESC;
# MAGIC
# MAGIC -- Display the view
# MAGIC SELECT * FROM dqx_lab.overall_quality_score;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Table Health View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create a view showing health of each table
# MAGIC -- Include: table_name, avg_pass_rate, total_checks, last_validated, quality_grade
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dqx_lab.table_health AS
# MAGIC -- Your SQL here
# MAGIC
# MAGIC SELECT * FROM dqx_lab.table_health;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Top Failing Checks View

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create a view identifying most problematic checks
# MAGIC -- Include: table, check_name, total_failures, failure_rate, last_failure
# MAGIC
# MAGIC CREATE OR REPLACE VIEW dqx_lab.top_failing_checks AS
# MAGIC -- Your SQL here
# MAGIC
# MAGIC SELECT * FROM dqx_lab.top_failing_checks LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Trend Analysis (25 minutes)
# MAGIC
# MAGIC ### Task 2.1: Quality Trend Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Analyze quality trends
# MAGIC -- Show daily pass rate trend with 7-day moving average
# MAGIC
# MAGIC SELECT 
# MAGIC   date_trunc('day', timestamp) as date,
# MAGIC   table_name,
# MAGIC   AVG(pass_rate) as daily_pass_rate,
# MAGIC   -- Add 7-day moving average
# MAGIC   -- Your SQL here
# MAGIC FROM dqx_lab.historical_quality_metrics
# MAGIC WHERE timestamp >= current_date() - INTERVAL 30 DAYS
# MAGIC GROUP BY date_trunc('day', timestamp), table_name
# MAGIC ORDER BY date DESC, table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Detect Quality Degradation

# COMMAND ----------

# TODO: Use PySpark to detect quality degradation
# Identify tables/checks where quality has degraded > 5% in last 7 days

metrics_df = spark.table("dqx_lab.historical_quality_metrics")

# Your code here:
# 1. Calculate pass rate for last 7 days vs previous 7 days
# 2. Compare and identify degradation
# 3. Generate alert list


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Identify Anomalies

# COMMAND ----------

# TODO: Find anomalous quality metrics
# Use statistical methods (e.g., Z-score) to identify outliers

from pyspark.sql.window import Window

# Your code here:
# 1. Calculate mean and stddev for each table/check
# 2. Identify records more than 2 std deviations from mean
# 3. Flag as anomalies


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Advanced Analytics (25 minutes)
# MAGIC
# MAGIC ### Task 3.1: Quality Correlation Analysis

# COMMAND ----------

# TODO: Analyze correlations between different quality metrics
# Questions to answer:
# - Do failures in one table correlate with failures in another?
# - Do specific check types fail together?
# - Are there time-of-day patterns?

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Quality Prediction

# COMMAND ----------

# TODO: Build simple quality prediction model
# Predict tomorrow's pass rate based on historical trends

# Your code here:
# 1. Aggregate daily metrics
# 2. Calculate trends
# 3. Extrapolate to predict next day


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Root Cause Analysis

# COMMAND ----------

# TODO: Perform root cause analysis for quality issues
# Identify patterns in failed records:
# - Common characteristics
# - Data sources
# - Time patterns
# - Upstream dependencies

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Quality KPIs and Scorecards (20 minutes)
# MAGIC
# MAGIC ### Task 4.1: Calculate Comprehensive KPIs

# COMMAND ----------

# TODO: Calculate and display key quality KPIs:
# - Overall data quality score (0-100)
# - Table-level quality scores
# - Quality improvement/degradation rate
# - Mean time to detect (MTTD) quality issues
# - Mean time to resolve (MTTR) quality issues

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Create Quality Scorecard

# COMMAND ----------

# TODO: Create a quality scorecard with grades (A+, A, B, C, D, F)
# - A+: 99%+ pass rate
# - A: 95-99% pass rate
# - B: 90-95% pass rate
# - C: 80-90% pass rate
# - F: <80% pass rate

scorecard_query = """
-- Your SQL here
"""

display(spark.sql(scorecard_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3: Executive Summary Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create executive summary dashboard query
# MAGIC -- Include high-level metrics suitable for executive review
# MAGIC -- Your SQL here

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Alerting and Notifications (20 minutes)
# MAGIC
# MAGIC ### Task 5.1: Define Alert Rules

# COMMAND ----------

# TODO: Define alert rules as a configuration
alert_rules = {
    "critical_pass_rate": 0.90,  # Alert if below 90%
    "warning_pass_rate": 0.95,   # Warn if below 95%
    "failure_spike": 100,        # Alert if >100 failures in single run
    "degradation_pct": 0.05,     # Alert if 5%+ degradation
    "consecutive_failures": 3    # Alert if 3 consecutive failures
}

print("Alert rules configured:")
for rule, threshold in alert_rules.items():
    print(f"  {rule}: {threshold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Implement Alert Detection

# COMMAND ----------

# TODO: Implement alert detection logic
def detect_alerts(metrics_df, alert_rules):
    """
    Detect quality alerts based on defined rules
    Returns DataFrame of active alerts
    """
    # Your code here:
    pass

# Run alert detection
alerts = detect_alerts(metrics_df, alert_rules)

# Display alerts
if alerts and alerts.count() > 0:
    print("🚨 Active Alerts:")
    display(alerts)
else:
    print("✅ No active alerts")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.3: Generate Alert Notifications

# COMMAND ----------

# TODO: Create alert notification messages
# Format alerts for different channels (Slack, email, PagerDuty)

def format_alert_message(alert_row, channel="slack"):
    """
    Format alert message for specific channel
    """
    # Your code here:
    pass

# Test with sample alert
# format_alert_message(sample_alert, "slack")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Quality Reports (Bonus - 30 minutes)
# MAGIC
# MAGIC ### Task 6.1: Daily Quality Report

# COMMAND ----------

# TODO: Generate daily quality report
# Include:
# - Summary statistics
# - Top issues
# - Quality trends
# - Recommendations

def generate_daily_report(report_date):
    """
    Generate comprehensive daily quality report
    """
    # Your code here:
    pass

# Generate report for yesterday
yesterday = datetime.now() - timedelta(days=1)
daily_report = generate_daily_report(yesterday.date())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2: Weekly Quality Report

# COMMAND ----------

# TODO: Generate weekly quality report
# Include:
# - Week-over-week comparison
# - Quality trend analysis
# - Root cause analysis of major issues
# - Action items and recommendations

def generate_weekly_report(week_ending_date):
    """
    Generate comprehensive weekly quality report
    """
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.3: Export Reports

# COMMAND ----------

# TODO: Export quality reports to various formats
# - PDF for executive review
# - Excel for detailed analysis
# - JSON for API consumption
# - HTML for email distribution

def export_report(report_df, format="html", destination_path=None):
    """
    Export quality report to specified format
    """
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC ✅ Dashboard views provide quick insights into data quality
# MAGIC
# MAGIC ✅ Trend analysis identifies quality degradation early
# MAGIC
# MAGIC ✅ KPIs and scorecards communicate quality to stakeholders
# MAGIC
# MAGIC ✅ Automated alerts enable proactive quality management
# MAGIC
# MAGIC ✅ Regular reports maintain quality awareness
# MAGIC
# MAGIC ✅ Root cause analysis drives continuous improvement
# MAGIC
# MAGIC ### Dashboard Best Practices
# MAGIC 1. **Keep it simple** - Focus on actionable metrics
# MAGIC 2. **Update frequently** - Real-time or near real-time
# MAGIC 3. **Set thresholds** - Clear indicators of health vs problems
# MAGIC 4. **Show trends** - Context matters more than point-in-time
# MAGIC 5. **Enable drill-down** - High-level to detailed analysis
# MAGIC 6. **Automate alerts** - Don't rely on manual monitoring
# MAGIC
# MAGIC ### Challenge Questions
# MAGIC 1. What metrics best indicate data quality health?
# MAGIC 2. How do you balance sensitivity and alert fatigue?
# MAGIC 3. What quality SLAs are appropriate for your organization?
# MAGIC 4. How do you measure ROI of quality initiatives?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Lab 8: Production Pipeline** to learn about:
# MAGIC - Building production-grade quality pipelines
# MAGIC - CI/CD integration
# MAGIC - Operational procedures
# MAGIC - Enterprise deployment patterns
