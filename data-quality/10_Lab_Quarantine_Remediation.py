# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Quarantine & Remediation
# MAGIC
# MAGIC **Module 10: Handling Data Quality Failures**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 40 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC 1. **Quarantine Strategy** - Isolate bad data without losing it
# MAGIC 2. **Remediation Patterns** - Common data fixes and transformations
# MAGIC 3. **Revalidation** - Test fixed data before releasing
# MAGIC 4. **Release Process** - Move remediated data back to production
# MAGIC 5. **Tracking** - Monitor quarantine and remediation metrics

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json

catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data with Quality Issues

# COMMAND ----------

quarantine_data = [
    (1, "john@example.com", "555-123-4567", 25, 150.50, "completed", "valid"),
    (2, "invalid-email", "555-234-5678", 32, 275.00, "completed", "bad_email"),
    (3, "jane@test.com", "(555) 345-6789", 45, 89.99, "pending", "bad_phone_format"),
    (None, "bob@test.com", "555-456-7890", 28, 450.00, "completed", "missing_id"),
    (5, "alice@example.com", "555-567-8901", 150, 125.75, "completed", "age_outlier"),
    (6, "charlie@test.com", "555-678-9012", -5, 200.00, "cancelled", "negative_age"),
    (7, "diana@example.com", "555-789-0123", 29, -50.00, "pending", "negative_amount"),
    (8, "EVE@TEST.COM", "555.890.1234", 41, 300.00, "INVALID_STATUS", "multiple_issues"),
]

quarantine_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("age", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("status", StringType()),
    StructField("issue_type", StringType())
])

df_quarantine = spark.createDataFrame(quarantine_data, quarantine_schema)
df_quarantine = df_quarantine.withColumn("quarantine_timestamp", current_timestamp())
df_quarantine.write.format("delta").mode("overwrite").saveAsTable("orders_quarantine")

print("✅ Quarantine test data created")
display(df_quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Analyze Quarantine Data

# COMMAND ----------

def analyze_quarantine(quarantine_table):
    """
    Analyze common issues in quarantined data
    """
    
    df_q = spark.table(quarantine_table)
    
    print("QUARANTINE ANALYSIS")
    print("="*80)
    print(f"Total Records: {df_q.count()}")
    print()
    
    # Issue breakdown
    print("Issues by Type:")
    issue_counts = df_q.groupBy("issue_type").count().orderBy(desc("count"))
    for row in issue_counts.collect():
        print(f"  {row.issue_type}: {row['count']}")
    print()
    
    # Age of quarantined data
    print("Data Age:")
    age_stats = df_q.select(
        min("quarantine_timestamp").alias("oldest"),
        max("quarantine_timestamp").alias("newest"),
        count("*").alias("total")
    ).collect()[0]
    print(f"  Oldest: {age_stats.oldest}")
    print(f"  Newest: {age_stats.newest}")
    print("="*80)

analyze_quarantine("orders_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Build Remediation Functions

# COMMAND ----------

def fix_email_format(email_col):
    """Fix common email issues"""
    return lower(trim(email_col))

def fix_phone_format(phone_col):
    """Standardize phone format to XXX-XXX-XXXX"""
    # Remove all non-digit characters
    cleaned = regexp_replace(phone_col, r'[^0-9]', '')
    # Format as XXX-XXX-XXXX
    formatted = regexp_replace(cleaned, r'^(\d{3})(\d{3})(\d{4})$', '$1-$2-$3')
    return formatted

def fix_status_format(status_col):
    """Standardize status values"""
    return when(upper(trim(status_col)).isin("PENDING", "COMPLETED", "CANCELLED"),
                upper(trim(status_col))).otherwise("PENDING")

def fix_age_outliers(age_col):
    """Cap age at reasonable bounds"""
    return when(age_col < 0, lit(None)) \
           .when(age_col > 120, lit(None)) \
           .otherwise(age_col)

def fix_amount(amount_col):
    """Fix negative amounts by taking absolute value"""
    return when(amount_col < 0, abs(amount_col)).otherwise(amount_col)

print("✅ Remediation functions created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Apply Remediation

# COMMAND ----------

def remediate_quarantine_data(quarantine_table):
    """
    Apply remediation transformations to quarantined data
    """
    
    print("APPLYING REMEDIATION")
    print("="*80)
    
    df_q = spark.table(quarantine_table)
    
    # Apply fixes
    df_remediated = df_q \
        .withColumn("email_fixed", fix_email_format(col("email"))) \
        .withColumn("phone_fixed", fix_phone_format(col("phone"))) \
        .withColumn("status_fixed", fix_status_format(col("status"))) \
        .withColumn("age_fixed", fix_age_outliers(col("age"))) \
        .withColumn("amount_fixed", fix_amount(col("amount"))) \
        .withColumn("remediation_timestamp", current_timestamp()) \
        .withColumn("remediated_by", lit("automated_remediation_v1"))
    
    # Replace original columns with fixed ones
    df_remediated = df_remediated \
        .withColumn("email", col("email_fixed")) \
        .withColumn("phone", col("phone_fixed")) \
        .withColumn("status", col("status_fixed")) \
        .withColumn("age", col("age_fixed")) \
        .withColumn("amount", col("amount_fixed")) \
        .drop("email_fixed", "phone_fixed", "status_fixed", "age_fixed", "amount_fixed")
    
    print(f"✅ Remediation applied to {df_remediated.count()} records")
    
    return df_remediated

df_remediated = remediate_quarantine_data("orders_quarantine")

print("\nSample Remediated Data:")
display(df_remediated.select("order_id", "email", "phone", "status", "age", "amount", "issue_type"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Revalidate Remediated Data

# COMMAND ----------

def revalidate_remediated_data(df):
    """
    Revalidate remediated data to check if issues are fixed
    """
    
    print("REVALIDATION")
    print("="*80)
    
    total = df.count()
    
    # Check each validation rule
    validations = {}
    
    # Email format
    invalid_email = df.filter(
        col("email").isNotNull() & 
        ~col("email").rlike(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
    ).count()
    validations['email_format'] = invalid_email == 0
    print(f"Email format: {'✅' if validations['email_format'] else f'❌ {invalid_email} invalid'}")
    
    # Phone format
    invalid_phone = df.filter(
        col("phone").isNotNull() &
        ~col("phone").rlike(r'^\d{3}-\d{3}-\d{4}$')
    ).count()
    validations['phone_format'] = invalid_phone == 0
    print(f"Phone format: {'✅' if validations['phone_format'] else f'❌ {invalid_phone} invalid'}")
    
    # Status values
    invalid_status = df.filter(
        ~col("status").isin("PENDING", "COMPLETED", "CANCELLED")
    ).count()
    validations['status_values'] = invalid_status == 0
    print(f"Status values: {'✅' if validations['status_values'] else f'❌ {invalid_status} invalid'}")
    
    # Age range
    invalid_age = df.filter(
        col("age").isNotNull() &
        ((col("age") < 0) | (col("age") > 120))
    ).count()
    validations['age_range'] = invalid_age == 0
    print(f"Age range: {'✅' if validations['age_range'] else f'❌ {invalid_age} invalid'}")
    
    # Amount positive
    invalid_amount = df.filter(
        col("amount").isNotNull() &
        (col("amount") < 0)
    ).count()
    validations['amount_positive'] = invalid_amount == 0
    print(f"Amount positive: {'✅' if validations['amount_positive'] else f'❌ {invalid_amount} invalid'}")
    
    # Overall result
    all_passed = all(validations.values())
    
    print()
    print(f"Overall: {'✅ ALL CHECKS PASSED' if all_passed else '❌ SOME CHECKS FAILED'}")
    print("="*80)
    
    return all_passed, validations

revalidation_passed, revalidation_results = revalidate_remediated_data(df_remediated)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Separate Successfully Remediated Records

# COMMAND ----------

def separate_remediated_records(df, validation_results):
    """
    Separate successfully remediated records from those still failing
    """
    
    # Build filter for valid records
    valid_filter = lit(True)
    
    # Email valid
    if not validation_results['email_format']:
        valid_filter = valid_filter & col("email").rlike(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$')
    
    # Phone valid
    if not validation_results['phone_format']:
        valid_filter = valid_filter & col("phone").rlike(r'^\d{3}-\d{3}-\d{4}$')
    
    # Status valid
    if not validation_results['status_values']:
        valid_filter = valid_filter & col("status").isin("PENDING", "COMPLETED", "CANCELLED")
    
    # Age valid
    if not validation_results['age_range']:
        valid_filter = valid_filter & ((col("age").isNull()) | ((col("age") >= 0) & (col("age") <= 120)))
    
    # Amount valid
    if not validation_results['amount_positive']:
        valid_filter = valid_filter & ((col("amount").isNull()) | (col("amount") >= 0))
    
    # Also ensure no critical nulls
    valid_filter = valid_filter & col("order_id").isNotNull()
    
    df_ready = df.filter(valid_filter)
    df_still_quarantined = df.filter(~valid_filter)
    
    print(f"✅ Ready for release: {df_ready.count()}")
    print(f"⚠️  Still quarantined: {df_still_quarantined.count()}")
    
    return df_ready, df_still_quarantined

df_ready_for_release, df_still_quarantined = separate_remediated_records(
    df_remediated, 
    revalidation_results
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Release to Production

# COMMAND ----------

def release_to_production(df_ready, target_table):
    """
    Release remediated data back to production
    """
    
    print(f"Releasing {df_ready.count()} records to {target_table}...")
    
    # Add release metadata
    df_release = df_ready \
        .withColumn("released_timestamp", current_timestamp()) \
        .withColumn("release_source", lit("quarantine_remediation"))
    
    # Write to production table
    df_release.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(target_table)
    
    print(f"✅ Released to {target_table}")

# Release to production
release_to_production(df_ready_for_release, "orders_silver_released")

# View released records
display(spark.table("orders_silver_released"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Track Quarantine Metrics

# COMMAND ----------

# Create quarantine metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS quarantine_metrics (
        metric_timestamp TIMESTAMP,
        quarantine_table STRING,
        total_quarantined INT,
        remediated_count INT,
        released_count INT,
        still_quarantined_count INT,
        success_rate DOUBLE,
        avg_time_in_quarantine_hours DOUBLE
    ) USING DELTA
""")

def log_quarantine_metrics(quarantine_table, remediated_count, released_count, still_quarantined_count):
    """
    Log quarantine and remediation metrics
    """
    
    df_q = spark.table(quarantine_table)
    total = df_q.count()
    success_rate = (released_count / remediated_count * 100) if remediated_count > 0 else 0
    
    # Calculate average time in quarantine (in hours)
    avg_time = df_q.select(
        avg((unix_timestamp(current_timestamp()) - unix_timestamp(col("quarantine_timestamp"))) / 3600)
    ).collect()[0][0] or 0
    
    record = {
        'metric_timestamp': datetime.now(),
        'quarantine_table': quarantine_table,
        'total_quarantined': total,
        'remediated_count': remediated_count,
        'released_count': released_count,
        'still_quarantined_count': still_quarantined_count,
        'success_rate': success_rate,
        'avg_time_in_quarantine_hours': avg_time
    }
    
    df_metric = spark.createDataFrame([record])
    df_metric.write.format("delta").mode("append").saveAsTable("quarantine_metrics")
    
    print(f"✅ Logged quarantine metrics")
    print(f"   Success Rate: {success_rate:.1f}%")
    print(f"   Avg Time in Quarantine: {avg_time:.2f} hours")

log_quarantine_metrics(
    "orders_quarantine",
    remediated_count=df_remediated.count(),
    released_count=df_ready_for_release.count(),
    still_quarantined_count=df_still_quarantined.count()
)

display(spark.table("quarantine_metrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Complete Quarantine Workflow

# COMMAND ----------

def complete_quarantine_workflow(quarantine_table, target_table):
    """
    End-to-end quarantine and remediation workflow
    """
    
    print("\n" + "="*80)
    print("QUARANTINE & REMEDIATION WORKFLOW")
    print("="*80)
    print()
    
    # Step 1: Analyze
    print("STEP 1: Analyze Quarantine")
    print("-"*80)
    analyze_quarantine(quarantine_table)
    print()
    
    # Step 2: Remediate
    print("STEP 2: Apply Remediation")
    print("-"*80)
    df_remediated = remediate_quarantine_data(quarantine_table)
    print()
    
    # Step 3: Revalidate
    print("STEP 3: Revalidate")
    print("-"*80)
    passed, validation_results = revalidate_remediated_data(df_remediated)
    print()
    
    # Step 4: Separate
    print("STEP 4: Separate Valid/Invalid")
    print("-"*80)
    df_ready, df_still_q = separate_remediated_records(df_remediated, validation_results)
    print()
    
    # Step 5: Release
    print("STEP 5: Release to Production")
    print("-"*80)
    release_to_production(df_ready, target_table)
    print()
    
    # Step 6: Track Metrics
    print("STEP 6: Log Metrics")
    print("-"*80)
    log_quarantine_metrics(
        quarantine_table,
        df_remediated.count(),
        df_ready.count(),
        df_still_q.count()
    )
    print()
    
    print("="*80)
    print("✅ WORKFLOW COMPLETE")
    print("="*80)
    
    return {
        'remediated': df_remediated.count(),
        'released': df_ready.count(),
        'still_quarantined': df_still_q.count()
    }

# Run complete workflow
result = complete_quarantine_workflow("orders_quarantine", "orders_silver_released_final")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Quarantine & Remediation Principles
# MAGIC 1. **Never Delete** - Quarantine, don't discard bad data
# MAGIC 2. **Automate Where Possible** - Fix common issues automatically
# MAGIC 3. **Always Revalidate** - Test fixes before releasing
# MAGIC 4. **Track Everything** - Monitor quarantine metrics
# MAGIC 5. **Enable Self-Service** - Provide tools for manual remediation
# MAGIC
# MAGIC ### What You Built
# MAGIC - Quarantine analysis functions
# MAGIC - Automated remediation transformations
# MAGIC - Revalidation logic
# MAGIC - Release workflow
# MAGIC - Metrics tracking
# MAGIC
# MAGIC ### Common Remediation Patterns
# MAGIC - **Format standardization** - Emails, phones, dates
# MAGIC - **Value normalization** - Uppercase, trim, etc.
# MAGIC - **Outlier handling** - Cap or NULL extreme values
# MAGIC - **Default values** - Fill missing required fields
# MAGIC - **Mathematical fixes** - Absolute value for negatives
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 8**: Quality monitoring dashboard
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 40 minutes | **Level**: 300 | **Type**: Lab
