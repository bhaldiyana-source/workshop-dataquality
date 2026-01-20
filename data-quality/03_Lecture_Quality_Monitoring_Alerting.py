# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Monitoring and Alerting
# MAGIC
# MAGIC **Module 4: Dashboards, SLAs, and Alerting Strategies**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 35 minutes    |
# MAGIC | Level           | 200/300       |
# MAGIC | Type            | Lecture       |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will understand:
# MAGIC
# MAGIC 1. **Quality Metrics** - What to measure and track
# MAGIC 2. **Monitoring Dashboards** - Visualizing quality over time
# MAGIC 3. **Quality SLAs** - Setting and enforcing service level agreements
# MAGIC 4. **Alerting Strategies** - When and how to notify stakeholders

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Monitor Data Quality?
# MAGIC
# MAGIC ### Business Impact
# MAGIC - **Trust**: Stakeholders need confidence in data
# MAGIC - **Compliance**: Regulatory requirements for data accuracy
# MAGIC - **Operations**: Prevent downstream failures
# MAGIC - **Cost**: Early detection reduces remediation costs
# MAGIC
# MAGIC ### Key Questions to Answer
# MAGIC 1. Is our data quality improving or degrading?
# MAGIC 2. Are we meeting our quality SLAs?
# MAGIC 3. Which tables have the most quality issues?
# MAGIC 4. What types of quality issues are most common?
# MAGIC 5. How quickly are quality issues resolved?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics Framework
# MAGIC
# MAGIC ```
# MAGIC ┌────────────────────────────────────────────────────────────┐
# MAGIC │                  QUALITY METRICS PYRAMID                    │
# MAGIC ├────────────────────────────────────────────────────────────┤
# MAGIC │                                                             │
# MAGIC │                    BUSINESS METRICS                         │
# MAGIC │              ┌─────────────────────────┐                   │
# MAGIC │              │ Revenue Impact          │                   │
# MAGIC │              │ Customer Complaints     │                   │
# MAGIC │              │ Compliance Violations   │                   │
# MAGIC │              └─────────────────────────┘                   │
# MAGIC │                        ▲                                    │
# MAGIC │                        │                                    │
# MAGIC │              ┌─────────────────────────┐                   │
# MAGIC │              │ OPERATIONAL METRICS     │                   │
# MAGIC │              │ • Pipeline Success Rate │                   │
# MAGIC │              │ • Remediation Time      │                   │
# MAGIC │              │ • Data Freshness        │                   │
# MAGIC │              └─────────────────────────┘                   │
# MAGIC │                        ▲                                    │
# MAGIC │                        │                                    │
# MAGIC │      ┌─────────────────────────────────────────┐           │
# MAGIC │      │ TECHNICAL METRICS                       │           │
# MAGIC │      │ • Quality Score (%)                     │           │
# MAGIC │      │ • Completeness Rate                     │           │
# MAGIC │      │ • Accuracy Rate                         │           │
# MAGIC │      │ • Duplication Rate                      │           │
# MAGIC │      │ • Schema Drift Count                    │           │
# MAGIC │      └─────────────────────────────────────────┘           │
# MAGIC │                                                             │
# MAGIC └────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Quality Metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Quality Score
# MAGIC Overall percentage of records passing all validations

# COMMAND ----------

# Example: Calculate quality score
from pyspark.sql.functions import col, count, when, lit

def calculate_quality_score(df, validation_rules):
    """
    Calculate overall quality score for a dataset
    
    Args:
        df: DataFrame to evaluate
        validation_rules: Dict of validation rules {name: condition}
    
    Returns:
        Quality score (0-100)
    """
    
    total_records = df.count()
    if total_records == 0:
        return 0
    
    # Apply each validation rule
    df_validated = df
    for rule_name, condition in validation_rules.items():
        df_validated = df_validated.withColumn(
            f"passes_{rule_name}",
            when(condition, 1).otherwise(0)
        )
    
    # Calculate pass rate for each rule
    rule_columns = [f"passes_{rule_name}" for rule_name in validation_rules.keys()]
    
    # Record passes all rules if sum equals number of rules
    df_validated = df_validated.withColumn(
        "passes_all_rules",
        sum([col(rule_col) for rule_col in rule_columns]) == len(validation_rules)
    )
    
    # Calculate quality score
    records_passing = df_validated.filter(col("passes_all_rules")).count()
    quality_score = (records_passing / total_records) * 100
    
    return quality_score

# Example usage
sample_data = [
    (1, "john@example.com", 100, "completed"),
    (2, "invalid-email", 200, "pending"),
    (3, "jane@test.com", -50, "completed"),
    (None, "bob@test.com", 150, "invalid_status")
]

df_sample = spark.createDataFrame(sample_data, ["id", "email", "amount", "status"])

validation_rules = {
    "id_not_null": col("id").isNotNull(),
    "email_valid": col("email").rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'),
    "amount_positive": col("amount") > 0,
    "status_valid": col("status").isin("pending", "completed", "cancelled")
}

score = calculate_quality_score(df_sample, validation_rules)
print(f"Quality Score: {score:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Completeness Rate
# MAGIC Percentage of required fields that are populated

# COMMAND ----------

def calculate_completeness_rate(df, required_columns):
    """
    Calculate completeness rate across required columns
    """
    
    total_records = df.count()
    if total_records == 0:
        return {}
    
    completeness = {}
    
    for col_name in required_columns:
        non_null_count = df.filter(col(col_name).isNotNull()).count()
        completeness[col_name] = (non_null_count / total_records) * 100
    
    # Overall completeness
    completeness['overall'] = sum(completeness.values()) / len(required_columns)
    
    return completeness

# Example
completeness = calculate_completeness_rate(df_sample, ["id", "email", "amount", "status"])
print("\nCompleteness Rates:")
for col_name, rate in completeness.items():
    print(f"  {col_name}: {rate:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Timeliness Metric
# MAGIC How fresh is the data?

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, unix_timestamp, expr

def calculate_data_freshness(df, timestamp_column):
    """
    Calculate average data age in hours
    """
    
    freshness = df.select(
        expr(f"(unix_timestamp(current_timestamp()) - unix_timestamp({timestamp_column})) / 3600").alias("age_hours")
    ).agg({"age_hours": "avg"}).collect()[0][0]
    
    return freshness

# Example with timestamp data
from datetime import datetime, timedelta

now = datetime.now()
data_with_timestamps = [
    (1, now - timedelta(hours=2)),
    (2, now - timedelta(hours=5)),
    (3, now - timedelta(hours=24)),
    (4, now - timedelta(hours=48))
]

df_timestamps = spark.createDataFrame(data_with_timestamps, ["id", "updated_at"])
avg_age = calculate_data_freshness(df_timestamps, "updated_at")
print(f"\nAverage data age: {avg_age:.2f} hours")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics Storage
# MAGIC
# MAGIC Store metrics in a Delta table for trend analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create metrics tracking table
# MAGIC CREATE TABLE IF NOT EXISTS quality_metrics (
# MAGIC   metric_id STRING,
# MAGIC   table_name STRING,
# MAGIC   layer STRING,
# MAGIC   metric_timestamp TIMESTAMP,
# MAGIC   metric_date DATE,
# MAGIC   
# MAGIC   -- Core metrics
# MAGIC   total_records BIGINT,
# MAGIC   valid_records BIGINT,
# MAGIC   invalid_records BIGINT,
# MAGIC   quality_score DOUBLE,
# MAGIC   
# MAGIC   -- Dimension metrics
# MAGIC   completeness_rate DOUBLE,
# MAGIC   accuracy_rate DOUBLE,
# MAGIC   uniqueness_rate DOUBLE,
# MAGIC   timeliness_score DOUBLE,
# MAGIC   
# MAGIC   -- Issue tracking
# MAGIC   critical_failures INT,
# MAGIC   warning_failures INT,
# MAGIC   info_failures INT,
# MAGIC   
# MAGIC   -- Metadata
# MAGIC   validation_details STRING,
# MAGIC   pipeline_run_id STRING
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (metric_date, layer);

# COMMAND ----------

# Example: Log quality metrics
from datetime import date
import json
import uuid

def log_quality_metrics(table_name, layer, metrics_dict):
    """
    Log quality metrics to tracking table
    """
    
    metric_record = {
        'metric_id': str(uuid.uuid4()),
        'table_name': table_name,
        'layer': layer,
        'metric_timestamp': datetime.now(),
        'metric_date': date.today(),
        'total_records': metrics_dict.get('total_records', 0),
        'valid_records': metrics_dict.get('valid_records', 0),
        'invalid_records': metrics_dict.get('invalid_records', 0),
        'quality_score': metrics_dict.get('quality_score', 0),
        'completeness_rate': metrics_dict.get('completeness_rate', 0),
        'accuracy_rate': metrics_dict.get('accuracy_rate', 0),
        'uniqueness_rate': metrics_dict.get('uniqueness_rate', 0),
        'timeliness_score': metrics_dict.get('timeliness_score', 0),
        'critical_failures': metrics_dict.get('critical_failures', 0),
        'warning_failures': metrics_dict.get('warning_failures', 0),
        'info_failures': metrics_dict.get('info_failures', 0),
        'validation_details': json.dumps(metrics_dict.get('details', {})),
        'pipeline_run_id': metrics_dict.get('pipeline_run_id', 'manual')
    }
    
    df_metric = spark.createDataFrame([metric_record])
    df_metric.write.format("delta").mode("append").saveAsTable("quality_metrics")
    
    print(f"✅ Logged quality metrics for {table_name}")

# Example usage
sample_metrics = {
    'total_records': 1000,
    'valid_records': 950,
    'invalid_records': 50,
    'quality_score': 95.0,
    'completeness_rate': 98.0,
    'accuracy_rate': 96.0,
    'critical_failures': 2,
    'warning_failures': 48
}

log_quality_metrics("orders_silver", "silver", sample_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Dashboards

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 1: Quality Trend Over Time

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality score trend (last 30 days)
# MAGIC SELECT 
# MAGIC   metric_date,
# MAGIC   layer,
# MAGIC   AVG(quality_score) as avg_quality_score,
# MAGIC   AVG(completeness_rate) as avg_completeness,
# MAGIC   AVG(accuracy_rate) as avg_accuracy,
# MAGIC   SUM(critical_failures) as total_critical_failures
# MAGIC FROM quality_metrics
# MAGIC WHERE metric_date >= CURRENT_DATE - INTERVAL 30 DAYS
# MAGIC GROUP BY metric_date, layer
# MAGIC ORDER BY metric_date DESC, layer;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 2: Table Quality Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current quality status by table
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   layer,
# MAGIC   MAX(metric_timestamp) as last_checked,
# MAGIC   AVG(quality_score) as avg_quality_score,
# MAGIC   MIN(quality_score) as min_quality_score,
# MAGIC   SUM(critical_failures) as total_critical_failures,
# MAGIC   SUM(total_records) as total_records_checked
# MAGIC FROM quality_metrics
# MAGIC WHERE metric_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY table_name, layer
# MAGIC ORDER BY avg_quality_score ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 3: Quality Issues Breakdown

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Quality issues by type and severity
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   layer,
# MAGIC   SUM(critical_failures) as critical_issues,
# MAGIC   SUM(warning_failures) as warning_issues,
# MAGIC   SUM(info_failures) as info_issues,
# MAGIC   SUM(critical_failures + warning_failures + info_failures) as total_issues
# MAGIC FROM quality_metrics
# MAGIC WHERE metric_date >= CURRENT_DATE - INTERVAL 7 DAYS
# MAGIC GROUP BY table_name, layer
# MAGIC HAVING total_issues > 0
# MAGIC ORDER BY critical_issues DESC, total_issues DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard 4: Quality Dimensions Scorecard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Six dimensions of quality (current state)
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   layer,
# MAGIC   ROUND(AVG(completeness_rate), 2) as completeness,
# MAGIC   ROUND(AVG(accuracy_rate), 2) as accuracy,
# MAGIC   ROUND(AVG(uniqueness_rate), 2) as uniqueness,
# MAGIC   ROUND(AVG(timeliness_score), 2) as timeliness,
# MAGIC   ROUND(AVG(quality_score), 2) as overall_quality
# MAGIC FROM quality_metrics
# MAGIC WHERE metric_date = CURRENT_DATE
# MAGIC GROUP BY table_name, layer
# MAGIC ORDER BY overall_quality DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality SLAs (Service Level Agreements)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining Quality SLAs
# MAGIC
# MAGIC | SLA Tier | Quality Score | Completeness | Max Critical Issues | Data Freshness |
# MAGIC |----------|---------------|--------------|---------------------|----------------|
# MAGIC | **Gold** | ≥ 99% | ≥ 99.5% | 0 | < 1 hour |
# MAGIC | **Silver** | ≥ 95% | ≥ 98% | ≤ 5 | < 4 hours |
# MAGIC | **Bronze** | ≥ 90% | ≥ 95% | ≤ 20 | < 24 hours |
# MAGIC | **Acceptable** | ≥ 85% | ≥ 90% | ≤ 50 | < 48 hours |
# MAGIC | **Critical** | < 85% | < 90% | > 50 | > 48 hours |

# COMMAND ----------

# Example: SLA checking function
def check_quality_sla(metrics, sla_tier="silver"):
    """
    Check if quality metrics meet SLA requirements
    """
    
    sla_definitions = {
        'gold': {
            'quality_score': 99.0,
            'completeness_rate': 99.5,
            'max_critical_failures': 0,
            'max_freshness_hours': 1
        },
        'silver': {
            'quality_score': 95.0,
            'completeness_rate': 98.0,
            'max_critical_failures': 5,
            'max_freshness_hours': 4
        },
        'bronze': {
            'quality_score': 90.0,
            'completeness_rate': 95.0,
            'max_critical_failures': 20,
            'max_freshness_hours': 24
        },
        'acceptable': {
            'quality_score': 85.0,
            'completeness_rate': 90.0,
            'max_critical_failures': 50,
            'max_freshness_hours': 48
        }
    }
    
    sla = sla_definitions.get(sla_tier.lower())
    if not sla:
        raise ValueError(f"Invalid SLA tier: {sla_tier}")
    
    violations = []
    
    if metrics.get('quality_score', 0) < sla['quality_score']:
        violations.append(f"Quality score {metrics['quality_score']:.2f}% below threshold {sla['quality_score']}%")
    
    if metrics.get('completeness_rate', 0) < sla['completeness_rate']:
        violations.append(f"Completeness {metrics['completeness_rate']:.2f}% below threshold {sla['completeness_rate']}%")
    
    if metrics.get('critical_failures', 0) > sla['max_critical_failures']:
        violations.append(f"Critical failures {metrics['critical_failures']} exceeds threshold {sla['max_critical_failures']}")
    
    return {
        'sla_met': len(violations) == 0,
        'sla_tier': sla_tier,
        'violations': violations
    }

# Test SLA check
test_metrics = {
    'quality_score': 96.5,
    'completeness_rate': 98.5,
    'critical_failures': 2
}

sla_result = check_quality_sla(test_metrics, "silver")
print(f"SLA Met: {sla_result['sla_met']}")
if sla_result['violations']:
    print("Violations:")
    for v in sla_result['violations']:
        print(f"  - {v}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alerting Strategies

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alert Severity Levels
# MAGIC
# MAGIC | Severity | Condition | Action | Response Time |
# MAGIC |----------|-----------|--------|---------------|
# MAGIC | **Critical** | SLA violation, data loss | Page on-call, auto-escalate | Immediate |
# MAGIC | **High** | Quality degradation > 10% | Notify team, create ticket | 1 hour |
# MAGIC | **Medium** | Quality degradation 5-10% | Email notification | 4 hours |
# MAGIC | **Low** | Minor quality issues | Daily digest | 24 hours |
# MAGIC | **Info** | Quality improvements | Weekly report | N/A |

# COMMAND ----------

def generate_quality_alerts(table_name, current_metrics, historical_avg, sla_tier="silver"):
    """
    Generate alerts based on quality metrics
    """
    
    alerts = []
    
    # Check SLA violations
    sla_check = check_quality_sla(current_metrics, sla_tier)
    if not sla_check['sla_met']:
        alerts.append({
            'severity': 'CRITICAL',
            'type': 'SLA_VIOLATION',
            'message': f"SLA violation for {table_name}",
            'details': sla_check['violations'],
            'action_required': 'Immediate investigation required'
        })
    
    # Check for quality degradation
    if historical_avg:
        quality_drop = historical_avg.get('quality_score', 0) - current_metrics.get('quality_score', 0)
        
        if quality_drop > 10:
            alerts.append({
                'severity': 'HIGH',
                'type': 'QUALITY_DEGRADATION',
                'message': f"Quality score dropped by {quality_drop:.2f}%",
                'details': {
                    'current': current_metrics.get('quality_score'),
                    'historical_avg': historical_avg.get('quality_score'),
                    'drop': quality_drop
                },
                'action_required': 'Investigate root cause within 1 hour'
            })
        elif quality_drop > 5:
            alerts.append({
                'severity': 'MEDIUM',
                'type': 'QUALITY_DEGRADATION',
                'message': f"Quality score dropped by {quality_drop:.2f}%",
                'details': {
                    'current': current_metrics.get('quality_score'),
                    'historical_avg': historical_avg.get('quality_score'),
                    'drop': quality_drop
                },
                'action_required': 'Review within 4 hours'
            })
    
    # Check for critical failures spike
    if current_metrics.get('critical_failures', 0) > 0:
        alerts.append({
            'severity': 'HIGH',
            'type': 'CRITICAL_FAILURES',
            'message': f"{current_metrics['critical_failures']} critical validation failures",
            'details': current_metrics,
            'action_required': 'Review and quarantine affected records'
        })
    
    return alerts

# Example
current = {
    'quality_score': 84.0,  # Below acceptable threshold
    'completeness_rate': 95.0,
    'critical_failures': 3
}

historical = {
    'quality_score': 96.0,
    'completeness_rate': 98.0
}

alerts = generate_quality_alerts("orders_silver", current, historical, "silver")

print(f"\n{'='*60}")
print(f"QUALITY ALERTS: {len(alerts)} alert(s) generated")
print(f"{'='*60}")
for alert in alerts:
    print(f"\n[{alert['severity']}] {alert['type']}")
    print(f"Message: {alert['message']}")
    print(f"Action: {alert['action_required']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alert Notification Channels
# MAGIC
# MAGIC ```python
# MAGIC def send_quality_alert(alert, channels=['email', 'slack']):
# MAGIC     """
# MAGIC     Send quality alert through multiple channels
# MAGIC     """
# MAGIC     
# MAGIC     if 'email' in channels:
# MAGIC         send_email_alert(
# MAGIC             to=get_oncall_email(alert['severity']),
# MAGIC             subject=f"[{alert['severity']}] Data Quality Alert: {alert['type']}",
# MAGIC             body=format_alert_email(alert)
# MAGIC         )
# MAGIC     
# MAGIC     if 'slack' in channels:
# MAGIC         send_slack_message(
# MAGIC             channel=get_slack_channel(alert['severity']),
# MAGIC             message=format_alert_slack(alert),
# MAGIC             severity=alert['severity']
# MAGIC         )
# MAGIC     
# MAGIC     if alert['severity'] == 'CRITICAL':
# MAGIC         # Page on-call engineer
# MAGIC         send_pagerduty_alert(alert)
# MAGIC     
# MAGIC     # Log all alerts
# MAGIC     log_alert_to_tracking_table(alert)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring Best Practices

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Define Clear Baselines
# MAGIC - Establish expected quality levels
# MAGIC - Measure historical performance
# MAGIC - Set realistic thresholds
# MAGIC
# MAGIC ### 2. Monitor Trends, Not Just Points
# MAGIC - Look at 7-day and 30-day trends
# MAGIC - Identify gradual degradation
# MAGIC - Detect cyclical patterns
# MAGIC
# MAGIC ### 3. Automate Everything
# MAGIC - Automated metric collection
# MAGIC - Automated SLA checking
# MAGIC - Automated alerting
# MAGIC - Automated reporting
# MAGIC
# MAGIC ### 4. Right-Size Alerting
# MAGIC - Avoid alert fatigue
# MAGIC - Use severity levels appropriately
# MAGIC - Consolidate related alerts
# MAGIC - Provide actionable information
# MAGIC
# MAGIC ### 5. Close the Loop
# MAGIC - Track alert resolution time
# MAGIC - Document root causes
# MAGIC - Update thresholds based on learnings
# MAGIC - Prevent recurrence

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Monitoring Example

# COMMAND ----------

def monitor_table_quality(table_name, layer, sla_tier="silver"):
    """
    Complete quality monitoring workflow
    """
    
    print(f"\n{'='*60}")
    print(f"QUALITY MONITORING: {table_name}")
    print(f"{'='*60}\n")
    
    # 1. Calculate current metrics
    df = spark.table(table_name)
    
    validation_rules = {
        "id_not_null": col("order_id").isNotNull(),
        "amount_positive": col("amount") > 0,
        "status_valid": col("status").isin("pending", "completed", "cancelled")
    }
    
    current_metrics = {
        'total_records': df.count(),
        'quality_score': calculate_quality_score(df, validation_rules),
        'completeness_rate': calculate_completeness_rate(df, df.columns)['overall'],
        'critical_failures': df.filter(col("order_id").isNull()).count(),
        'table_name': table_name,
        'layer': layer
    }
    
    current_metrics['valid_records'] = int(current_metrics['total_records'] * current_metrics['quality_score'] / 100)
    current_metrics['invalid_records'] = current_metrics['total_records'] - current_metrics['valid_records']
    
    print(f"Current Metrics:")
    print(f"  Total Records: {current_metrics['total_records']}")
    print(f"  Quality Score: {current_metrics['quality_score']:.2f}%")
    print(f"  Completeness: {current_metrics['completeness_rate']:.2f}%")
    print(f"  Critical Failures: {current_metrics['critical_failures']}")
    
    # 2. Log metrics
    log_quality_metrics(table_name, layer, current_metrics)
    
    # 3. Get historical average
    historical_df = spark.sql(f"""
        SELECT 
            AVG(quality_score) as quality_score,
            AVG(completeness_rate) as completeness_rate
        FROM quality_metrics
        WHERE table_name = '{table_name}'
          AND metric_date >= CURRENT_DATE - INTERVAL 7 DAYS
          AND metric_date < CURRENT_DATE
    """)
    
    historical_avg = historical_df.collect()[0].asDict() if historical_df.count() > 0 else None
    
    # 4. Check SLA
    sla_result = check_quality_sla(current_metrics, sla_tier)
    print(f"\nSLA Status: {'✅ PASSED' if sla_result['sla_met'] else '❌ FAILED'}")
    
    # 5. Generate alerts
    alerts = generate_quality_alerts(table_name, current_metrics, historical_avg, sla_tier)
    
    if alerts:
        print(f"\n⚠️  {len(alerts)} ALERT(S) GENERATED:")
        for alert in alerts:
            print(f"  [{alert['severity']}] {alert['message']}")
    else:
        print("\n✅ No alerts - Quality is within acceptable range")
    
    return {
        'current_metrics': current_metrics,
        'sla_result': sla_result,
        'alerts': alerts
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Monitoring Essentials
# MAGIC 1. **Measure Continuously** - Quality metrics should be tracked over time
# MAGIC 2. **Visualize Trends** - Dashboards reveal patterns and anomalies
# MAGIC 3. **Set Clear SLAs** - Define what "good quality" means
# MAGIC 4. **Alert Intelligently** - Right severity, right person, right time
# MAGIC 5. **Act on Insights** - Monitoring without action is wasted effort
# MAGIC
# MAGIC ### Success Metrics
# MAGIC - Mean time to detect (MTTD) quality issues
# MAGIC - Mean time to resolve (MTTR) quality issues
# MAGIC - Percentage of tables meeting SLA
# MAGIC - Trend in quality scores over time
# MAGIC - Alert accuracy and false positive rate
# MAGIC
# MAGIC ### Remember
# MAGIC - Quality monitoring is a continuous process
# MAGIC - Automate metric collection and alerting
# MAGIC - Review and adjust thresholds regularly
# MAGIC - Focus on trends, not just point-in-time values
# MAGIC - Close the feedback loop with remediation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that you understand the foundations, continue to the hands-on labs:
# MAGIC
# MAGIC - **Lab 1**: Data Quality Profiling
# MAGIC - **Lab 2**: Validation Framework
# MAGIC - **Lab 3-5**: Bronze, Silver, Gold Layer Quality
# MAGIC - **Lab 6**: Great Expectations Integration
# MAGIC - **Lab 7**: Quarantine & Remediation
# MAGIC - **Lab 8**: Quality Monitoring Dashboard (Build what you learned here!)
# MAGIC - **Lab 9**: Anomaly Detection
# MAGIC - **Lab 10**: Production Pipeline
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 35 minutes | **Level**: 200/300 | **Type**: Lecture
