# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Quality Monitoring Dashboard
# MAGIC
# MAGIC **Module 11: Real-Time Quality Tracking**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 200/300       |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## This lab creates SQL queries and visualizations for monitoring data quality over time.
# MAGIC
# MAGIC Use the queries below in Databricks SQL to create dashboards.

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Quality Metrics Table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS quality_metrics_dashboard (
# MAGIC   metric_id STRING,
# MAGIC   table_name STRING,
# MAGIC   layer STRING,
# MAGIC   metric_timestamp TIMESTAMP,
# MAGIC   metric_date DATE,
# MAGIC   total_records BIGINT,
# MAGIC   valid_records BIGINT,
# MAGIC   invalid_records BIGINT,
# MAGIC   quality_score DOUBLE,
# MAGIC   completeness_rate DOUBLE,
# MAGIC   accuracy_rate DOUBLE,
# MAGIC   critical_failures INT,
# MAGIC   warning_failures INT
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (metric_date, layer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Metrics Data

# COMMAND ----------

import uuid

# Generate 30 days of sample metrics
sample_metrics = []
base_date = datetime.now() - timedelta(days=30)

tables = [("orders_bronze", "bronze", 95), ("orders_silver", "silver", 92), ("orders_gold", "gold", 98)]

for days_ago in range(30):
    metric_date = base_date + timedelta(days=days_ago)
    
    for table_name, layer, base_quality in tables:
        # Add some randomness
        quality_score = base_quality + random.uniform(-5, 5)
        total_records = random.randint(1000, 5000)
        valid_records = int(total_records * (quality_score / 100))
        
        sample_metrics.append({
            'metric_id': str(uuid.uuid4()),
            'table_name': table_name,
            'layer': layer,
            'metric_timestamp': metric_date,
            'metric_date': metric_date.date(),
            'total_records': total_records,
            'valid_records': valid_records,
            'invalid_records': total_records - valid_records,
            'quality_score': quality_score,
            'completeness_rate': random.uniform(90, 99.5),
            'accuracy_rate': random.uniform(88, 99),
            'critical_failures': random.randint(0, 5) if quality_score < 90 else 0,
            'warning_failures': random.randint(5, 20)
        })

df_metrics = spark.createDataFrame(sample_metrics)
df_metrics.write.format("delta").mode("overwrite").saveAsTable("quality_metrics_dashboard")

print(f"✅ Generated {len(sample_metrics)} sample metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard SQL Queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 1: Quality Trend Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Score Trend (Last 30 Days)
# MAGIC SELECT 
# MAGIC   metric_date,
# MAGIC   layer,
# MAGIC   table_name,
# MAGIC   AVG(quality_score) as avg_quality_score,
# MAGIC   MIN(quality_score) as min_quality_score,
# MAGIC   MAX(quality_score) as max_quality_score
# MAGIC FROM quality_metrics_dashboard
# MAGIC WHERE metric_date >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY metric_date, layer, table_name
# MAGIC ORDER BY metric_date DESC, layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 2: Current Quality Status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current Quality Status (Today)
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   layer,
# MAGIC   ROUND(AVG(quality_score), 2) as quality_score,
# MAGIC   ROUND(AVG(completeness_rate), 2) as completeness,
# MAGIC   ROUND(AVG(accuracy_rate), 2) as accuracy,
# MAGIC   SUM(critical_failures) as critical_failures,
# MAGIC   SUM(total_records) as total_records
# MAGIC FROM quality_metrics_dashboard
# MAGIC WHERE metric_date = CURRENT_DATE
# MAGIC GROUP BY table_name, layer
# MAGIC ORDER BY quality_score ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 3: Quality Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality Alerts (Issues Requiring Attention)
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   layer,
# MAGIC   metric_date,
# MAGIC   quality_score,
# MAGIC   critical_failures,
# MAGIC   CASE 
# MAGIC     WHEN quality_score < 85 OR critical_failures > 10 THEN 'CRITICAL'
# MAGIC     WHEN quality_score < 90 OR critical_failures > 5 THEN 'HIGH'
# MAGIC     WHEN quality_score < 95 THEN 'MEDIUM'
# MAGIC     ELSE 'LOW'
# MAGIC   END as alert_level
# MAGIC FROM quality_metrics_dashboard
# MAGIC WHERE metric_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC   AND (quality_score < 95 OR critical_failures > 0)
# MAGIC ORDER BY 
# MAGIC   CASE 
# MAGIC     WHEN quality_score < 85 OR critical_failures > 10 THEN 1
# MAGIC     WHEN quality_score < 90 OR critical_failures > 5 THEN 2
# MAGIC     WHEN quality_score < 95 THEN 3
# MAGIC     ELSE 4
# MAGIC   END,
# MAGIC   metric_date DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 4: Layer Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality by Layer (Last 7 Days)
# MAGIC SELECT 
# MAGIC   layer,
# MAGIC   ROUND(AVG(quality_score), 2) as avg_quality_score,
# MAGIC   ROUND(MIN(quality_score), 2) as min_quality_score,
# MAGIC   ROUND(MAX(quality_score), 2) as max_quality_score,
# MAGIC   ROUND(AVG(completeness_rate), 2) as avg_completeness,
# MAGIC   ROUND(AVG(accuracy_rate), 2) as avg_accuracy,
# MAGIC   SUM(critical_failures) as total_critical_failures
# MAGIC FROM quality_metrics_dashboard
# MAGIC WHERE metric_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY layer
# MAGIC ORDER BY layer

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query 5: Week-over-Week Comparison

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Week-over-Week Quality Change
# MAGIC WITH current_week AS (
# MAGIC   SELECT 
# MAGIC     table_name,
# MAGIC     layer,
# MAGIC     AVG(quality_score) as current_quality
# MAGIC   FROM quality_metrics_dashboard
# MAGIC   WHERE metric_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC   GROUP BY table_name, layer
# MAGIC ),
# MAGIC previous_week AS (
# MAGIC   SELECT 
# MAGIC     table_name,
# MAGIC     layer,
# MAGIC     AVG(quality_score) as previous_quality
# MAGIC   FROM quality_metrics_dashboard
# MAGIC   WHERE metric_date >= CURRENT_DATE - INTERVAL 14 DAYS
# MAGIC     AND metric_date < CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC   GROUP BY table_name, layer
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.table_name,
# MAGIC   c.layer,
# MAGIC   ROUND(c.current_quality, 2) as current_week_quality,
# MAGIC   ROUND(p.previous_quality, 2) as previous_week_quality,
# MAGIC   ROUND(c.current_quality - p.previous_quality, 2) as change,
# MAGIC   CASE 
# MAGIC     WHEN c.current_quality > p.previous_quality THEN '📈 Improving'
# MAGIC     WHEN c.current_quality < p.previous_quality THEN '📉 Declining'
# MAGIC     ELSE '➡️ Stable'
# MAGIC   END as trend
# MAGIC FROM current_week c
# MAGIC JOIN previous_week p ON c.table_name = p.table_name AND c.layer = p.layer
# MAGIC ORDER BY change ASC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Dashboard Components
# MAGIC 1. **Trend Charts** - Track quality over time
# MAGIC 2. **Status Widgets** - Current quality snapshot
# MAGIC 3. **Alert Lists** - Issues requiring attention
# MAGIC 4. **Comparisons** - Layer and period comparisons
# MAGIC 5. **Drill-downs** - Detailed issue analysis
# MAGIC
# MAGIC ### Best Practices
# MAGIC - Update metrics frequently (hourly/daily)
# MAGIC - Set appropriate alert thresholds
# MAGIC - Review dashboards regularly
# MAGIC - Act on alerts promptly
# MAGIC - Track trends, not just points
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 45 minutes | **Level**: 200/300 | **Type**: Lab
