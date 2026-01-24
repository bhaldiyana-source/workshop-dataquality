# Databricks notebook source
# MAGIC %md
# MAGIC # Lakeflow Pipelines (DLT) Integration
# MAGIC
# MAGIC **Module 7: DQX with Delta Live Tables**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
# MAGIC | Level           | 400           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Integrate DQX with DLT** pipelines
# MAGIC 2. **Implement medallion architecture** with quality checks
# MAGIC 3. **Combine DLT expectations** with DQX validations
# MAGIC 4. **Track quality metrics** in DLT pipelines
# MAGIC 5. **Monitor pipeline health** and quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Understanding DLT + DQX Integration
# MAGIC
# MAGIC ### Why Combine DLT and DQX?
# MAGIC
# MAGIC | Feature | DLT Expectations | DQX | Combined |
# MAGIC |---------|------------------|-----|----------|
# MAGIC | **Simple Checks** | ✅ Easy | ✅ Easy | 🎯 Use DLT |
# MAGIC | **Complex Rules** | ⚠️ Limited | ✅ Rich | 🎯 Use DQX |
# MAGIC | **Profiling** | ❌ No | ✅ Yes | 🎯 Use DQX |
# MAGIC | **Quarantine** | ⚠️ Manual | ✅ Built-in | 🎯 Use DQX |
# MAGIC | **Monitoring** | ✅ DLT UI | ✅ Metrics | 🎯 Both |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: DLT Pipeline with DQX
# MAGIC
# MAGIC ### 2.1 Import DLT and DQX

# COMMAND ----------

# DBTITLE 1,Cell 5
# Note: This notebook demonstrates DLT pipeline concepts
# DLT (Delta Live Tables) is only available when running as a DLT pipeline
# For this demo, we'll use regular PySpark to show the same concepts

from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

print("✅ Libraries imported")
print("\nNote: This notebook demonstrates DLT + DQX integration concepts.")
print("In production, this would run as a Delta Live Tables pipeline.")
print("\nWe'll demonstrate the same quality validation patterns using PySpark.")

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %md
# MAGIC %undefined
# MAGIC ## Demo: Medallion Architecture with Quality Checks
# MAGIC
# MAGIC We'll demonstrate Bronze → Silver → Gold layers with quality validation at each stage.

# COMMAND ----------

# DBTITLE 1,Create Sample Source Data
# Create sample transaction data (simulating bronze layer input)
source_transactions = [
    ("TXN000001", "CUST0001", 1000.00, 100.00, "completed", "credit_card", "2024-01-15 10:00:00"),
    ("TXN000002", None, 500.00, 50.00, "completed", "paypal", "2024-01-15 11:00:00"),  # Issue: null customer
    ("TXN000003", "CUST0003", -200.00, 20.00, "completed", "credit_card", "2024-01-15 12:00:00"),  # Issue: negative amount
    ("TXN000004", "CUST0004", 1500.00, 800.00, "pending", "credit_card", "2024-01-15 13:00:00"),  # Issue: excessive discount
    ("TXN000005", "CUST0005", 750.00, 75.00, "invalid_status", "debit_card", "2024-01-15 14:00:00"),  # Issue: invalid status
    ("TXN000006", "CUST0006", 2000.00, 200.00, "completed", "bank_transfer", "2024-01-15 15:00:00"),
    ("TXN000007", "CUST0007", 3000.00, 300.00, "processing", "credit_card", "2024-01-15 16:00:00"),
    ("TXN000008", "CUST0008", 1200.00, 120.00, "completed", "paypal", "2024-01-15 17:00:00"),
    ("TXN000009", "CUST0009", 800.00, 80.00, "cancelled", "debit_card", "2024-01-15 18:00:00"),
    ("TXN000010", "CUST0010", 1800.00, 180.00, "completed", "credit_card", "2024-01-15 19:00:00"),
    ("TXN000001", "CUST0001", 1000.00, 100.00, "completed", "credit_card", "2024-01-15 10:00:00"),  # Duplicate
]

schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("created_at", StringType(), True)
])

source_df = spark.createDataFrame(source_transactions, schema)

print("✅ Source data created (simulating raw input)")
print(f"Total records: {source_df.count()}")
display(source_df)

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %md
# MAGIC %undefined
# MAGIC ### Bronze Layer: Raw Data Ingestion
# MAGIC
# MAGIC Load data as-is with minimal transformations

# COMMAND ----------

# DBTITLE 1,Create Bronze Table
# Bronze layer: Load raw data with timestamp conversion
bronze_transactions = source_df.withColumn(
    "created_at",
    F.to_timestamp("created_at")
).withColumn(
    "ingestion_timestamp",
    F.current_timestamp()
)

print("✅ Bronze layer created")
print(f"Records: {bronze_transactions.count()}")
display(bronze_transactions)

# COMMAND ----------

# DBTITLE 1,Cell 10
# MAGIC %md
# MAGIC %undefined
# MAGIC ### Silver Layer: Validated and Cleaned Data
# MAGIC
# MAGIC Apply comprehensive quality checks and deduplication

# COMMAND ----------

# DBTITLE 1,Silver Layer Validation
# Silver layer: Apply quality validation
# Step 1: Deduplicate
deduped_df = bronze_transactions.dropDuplicates(["transaction_id"])

print(f"After deduplication: {deduped_df.count()} records (removed {bronze_transactions.count() - deduped_df.count()} duplicates)")

# Step 2: Apply comprehensive quality checks
silver_validated = deduped_df

# Check 1: transaction_id NOT NULL
silver_validated = silver_validated.withColumn(
    "check_transaction_id_not_null",
    F.when(F.col("transaction_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
)

# Check 2: customer_id NOT NULL
silver_validated = silver_validated.withColumn(
    "check_customer_id_not_null",
    F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
)

# Check 3: amount range (0 to 1,000,000)
silver_validated = silver_validated.withColumn(
    "check_amount_range",
    F.when(
        (F.col("amount") < 0) | (F.col("amount") > 1000000),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
)

# Check 4: valid status
valid_statuses = ["pending", "processing", "completed", "cancelled", "refunded"]
silver_validated = silver_validated.withColumn(
    "check_valid_status",
    F.when(F.col("status").isin(valid_statuses), F.lit("PASSED")).otherwise(F.lit("FAILED"))
)

# Check 5: valid payment method
valid_payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]
silver_validated = silver_validated.withColumn(
    "check_valid_payment_method",
    F.when(F.col("payment_method").isin(valid_payment_methods), F.lit("PASSED")).otherwise(F.lit("WARNING"))
)

# Check 6: discount validation (row-level rule)
silver_validated = silver_validated.withColumn(
    "check_discount_validation",
    F.when(F.col("discount") > F.col("amount") * 0.5, F.lit("FAILED")).otherwise(F.lit("PASSED"))
)

# Check 7: discount non-negative
silver_validated = silver_validated.withColumn(
    "check_discount_non_negative",
    F.when(F.col("discount") < 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
)

# Collect failed checks
silver_validated = silver_validated.withColumn(
    "dqx_failed_checks",
    F.concat_ws(", ",
        F.when(F.col("check_transaction_id_not_null") == "FAILED", F.lit("transaction_id_not_null")),
        F.when(F.col("check_customer_id_not_null") == "FAILED", F.lit("customer_id_not_null")),
        F.when(F.col("check_amount_range") == "FAILED", F.lit("amount_range")),
        F.when(F.col("check_valid_status") == "FAILED", F.lit("valid_status")),
        F.when(F.col("check_discount_validation") == "FAILED", F.lit("discount_validation")),
        F.when(F.col("check_discount_non_negative") == "FAILED", F.lit("discount_non_negative"))
    )
)

# Add overall quality status
silver_validated = silver_validated.withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_transaction_id_not_null") == "FAILED") |
        (F.col("check_customer_id_not_null") == "FAILED") |
        (F.col("check_amount_range") == "FAILED") |
        (F.col("check_valid_status") == "FAILED") |
        (F.col("check_discount_validation") == "FAILED") |
        (F.col("check_discount_non_negative") == "FAILED"),
        F.lit("FAILED")
    ).when(
        F.col("check_valid_payment_method") == "WARNING",
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

print("✅ Silver layer validation applied")
print(f"Total: {silver_validated.count()}")
print(f"Passed: {silver_validated.filter(F.col('dqx_quality_status') == 'PASSED').count()}")
print(f"Failed: {silver_validated.filter(F.col('dqx_quality_status') == 'FAILED').count()}")
print(f"Warnings: {silver_validated.filter(F.col('dqx_quality_status') == 'WARNING').count()}")

display(silver_validated)

# COMMAND ----------

# DBTITLE 1,Separate Clean and Quarantine
# Separate clean records from quarantined records
silver_transactions = silver_validated.filter(F.col("dqx_quality_status") == "PASSED").drop(
    "check_transaction_id_not_null", "check_customer_id_not_null", "check_amount_range",
    "check_valid_status", "check_valid_payment_method", "check_discount_validation",
    "check_discount_non_negative", "dqx_failed_checks", "dqx_quality_status"
)

quarantined_transactions = silver_validated.filter(F.col("dqx_quality_status") == "FAILED").withColumn(
    "quarantine_timestamp",
    F.current_timestamp()
).withColumn(
    "quarantine_reason",
    F.col("dqx_failed_checks")
)

print(f"✅ Silver layer (clean): {silver_transactions.count()} records")
print(f"✅ Quarantined: {quarantined_transactions.count()} records")

print("\nClean silver transactions:")
display(silver_transactions)

# COMMAND ----------

# DBTITLE 1,Show Quarantined Records
# Show quarantined records with failure details
print("Quarantined transactions:")
display(quarantined_transactions.select(
    "transaction_id", "customer_id", "amount", "discount", "status",
    "quarantine_timestamp", "quarantine_reason", "dqx_failed_checks"
))

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ### Gold Layer: Enriched and Aggregated Data
# MAGIC
# MAGIC Add calculated fields and create business metrics

# COMMAND ----------

# DBTITLE 1,Silver Enriched
# Enrich silver data with calculated fields
silver_transactions_enriched = silver_transactions.withColumn(
    "net_amount",
    F.col("amount") - F.col("discount")
).withColumn(
    "discount_percentage",
    (F.col("discount") / F.col("amount") * 100).cast("decimal(5,2)")
).withColumn(
    "processed_date",
    F.current_date()
)

print("✅ Silver enriched layer created")
print(f"Records: {silver_transactions_enriched.count()}")
display(silver_transactions_enriched)

# COMMAND ----------

# DBTITLE 1,Gold Daily Summary
# Gold layer: Daily transaction summary
gold_daily_summary = (
    silver_transactions_enriched
    .groupBy(
        F.to_date("created_at").alias("date"),
        "status",
        "payment_method"
    )
    .agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount"),
        F.sum("discount").alias("total_discount"),
        F.sum("net_amount").alias("total_net_amount"),
        F.avg("amount").alias("avg_amount"),
        F.max("amount").alias("max_amount"),
        F.min("amount").alias("min_amount")
    )
    .orderBy("date", "status")
)

print("✅ Gold daily summary created")
print(f"Summary records: {gold_daily_summary.count()}")
display(gold_daily_summary)

# COMMAND ----------

# DBTITLE 1,Gold Customer Summary
# Gold layer: Customer-level aggregations
gold_customer_summary = (
    silver_transactions_enriched
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("total_transactions"),
        F.sum("net_amount").alias("lifetime_value"),
        F.avg("net_amount").alias("avg_transaction_value"),
        F.max("created_at").alias("last_transaction_date"),
        F.min("created_at").alias("first_transaction_date")
    )
    .orderBy(F.desc("lifetime_value"))
)

print("✅ Gold customer summary created")
print(f"Customers: {gold_customer_summary.count()}")
display(gold_customer_summary)

# COMMAND ----------

# DBTITLE 1,Cell 18
# MAGIC %md
# MAGIC %undefined
# MAGIC ### Quality Metrics Tracking
# MAGIC
# MAGIC Track validation results for monitoring and alerting

# COMMAND ----------

# DBTITLE 1,Calculate Quality Metrics
# Calculate quality metrics from validation results
check_columns = [c for c in silver_validated.columns if c.startswith('check_')]

metrics_data = []
for check_col in check_columns:
    check_name = check_col.replace('check_', '')
    total_records = silver_validated.count()
    failed_records = silver_validated.filter(F.col(check_col) == "FAILED").count()
    warning_records = silver_validated.filter(F.col(check_col) == "WARNING").count()
    passed_records = total_records - failed_records - warning_records
    pass_rate = passed_records / total_records if total_records > 0 else 0
    
    metrics_data.append((
        "silver_transactions",
        check_name,
        check_col.split('_')[1] if len(check_col.split('_')) > 1 else "rule",
        datetime.now(),
        total_records,
        passed_records,
        failed_records,
        warning_records,
        pass_rate
    ))

quality_metrics = spark.createDataFrame(metrics_data, [
    "table_name", "check_name", "check_type", "timestamp",
    "records_processed", "records_passed", "records_failed", "records_warning", "pass_rate"
])

print("✅ Quality metrics calculated")
print(f"Total checks tracked: {quality_metrics.count()}")
display(quality_metrics)

# COMMAND ----------

# DBTITLE 1,Quality Metrics Summary
# Summarize quality metrics
metrics_summary = (
    quality_metrics
    .groupBy("table_name")
    .agg(
        F.sum("records_processed").alias("total_processed"),
        F.sum("records_failed").alias("total_failed"),
        F.avg("pass_rate").alias("avg_pass_rate")
    )
)

print("Quality Metrics Summary:")
display(metrics_summary)

print("\n=== Failed Checks ===")
failed_checks = quality_metrics.filter(F.col("records_failed") > 0).orderBy(F.desc("records_failed"))
display(failed_checks.select("check_name", "records_failed", "pass_rate"))

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ## Medallion Architecture Summary
# MAGIC
# MAGIC ### Data Flow Results:
# MAGIC
# MAGIC **Bronze Layer (Raw Data)**
# MAGIC * Input: 11 records (with 1 duplicate)
# MAGIC * Output: 11 records with ingestion timestamp
# MAGIC
# MAGIC **Silver Layer (Validated & Cleaned)**
# MAGIC * After deduplication: 10 records
# MAGIC * Quality validation applied: 7 checks
# MAGIC * Clean records: 6 (60% pass rate)
# MAGIC * Quarantined records: 4 (40% failed)
# MAGIC
# MAGIC **Gold Layer (Business Metrics)**
# MAGIC * Daily summary: 5 aggregated records
# MAGIC * Customer summary: 6 customer profiles
# MAGIC * Calculated fields: net_amount, discount_percentage
# MAGIC
# MAGIC ### Quality Issues Detected:
# MAGIC
# MAGIC 1. **customer_id_not_null**: 1 failure (TXN000002)
# MAGIC 2. **amount_range**: 1 failure (TXN000003 - negative amount)
# MAGIC 3. **valid_status**: 1 failure (TXN000005 - invalid_status)
# MAGIC 4. **discount_validation**: 2 failures (TXN000003, TXN000004 - excessive discounts)
# MAGIC
# MAGIC ### Overall Quality Score: 92.9%
# MAGIC
# MAGIC (Average pass rate across all checks)

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ## How This Works in Delta Live Tables
# MAGIC
# MAGIC In a real DLT pipeline, this notebook would:
# MAGIC
# MAGIC ### 1. **Run as DLT Pipeline**
# MAGIC ```python
# MAGIC @dlt.table(name="bronze_transactions")
# MAGIC def bronze_transactions():
# MAGIC     return spark.readStream.format("cloudFiles").load(source_path)
# MAGIC
# MAGIC @dlt.table(name="silver_transactions")
# MAGIC def silver_transactions():
# MAGIC     df = dlt.read_stream("bronze_transactions")
# MAGIC     # Apply DQX validation
# MAGIC     return validator.validate_dlt(df)
# MAGIC ```
# MAGIC
# MAGIC ### 2. **Automatic Orchestration**
# MAGIC * DLT manages dependencies between tables
# MAGIC * Incremental processing with checkpoints
# MAGIC * Automatic retries and error handling
# MAGIC
# MAGIC ### 3. **Built-in Monitoring**
# MAGIC * DLT UI shows pipeline DAG
# MAGIC * Data quality metrics tracked automatically
# MAGIC * Lineage and data flow visualization
# MAGIC
# MAGIC ### 4. **Quality Expectations**
# MAGIC * Combine DLT expectations (simple SQL checks)
# MAGIC * With DQX validations (complex business rules)
# MAGIC * Quarantine bad data automatically
# MAGIC
# MAGIC ### 5. **Production Benefits**
# MAGIC * Declarative pipeline definition
# MAGIC * Automatic optimization and scaling
# MAGIC * Built-in data quality tracking
# MAGIC * Easy monitoring and alerting

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC Bronze layer loads raw data with minimal transformations

# COMMAND ----------

@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data from source systems",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
def bronze_transactions():
    """
    Bronze layer: Raw data ingestion
    - Load data as-is
    - Minimal transformations
    - Basic DLT expectations for critical fields
    """
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/tmp/dqx_dlt_demo/schema")
        .load("/tmp/dqx_dlt_demo/source/transactions")
        .select(
            "transaction_id",
            "customer_id",
            "amount",
            "discount",
            "status",
            "payment_method",
            "created_at"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Silver Layer - Validated and Cleaned Data
# MAGIC
# MAGIC Silver layer applies comprehensive quality checks with DQX

# COMMAND ----------

@dlt.table(
    name="silver_transactions",
    comment="Validated and cleaned transaction data",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true"
    }
)
def silver_transactions():
    """
    Silver layer: Data quality and validation
    - Comprehensive DQX validation
    - Business logic checks
    - Quarantine bad records
    """
    # Read from bronze
    bronze_df = dlt.read_stream("bronze_transactions")
    
    # Create DQX validator for DLT
    validator = DLTValidator()
    
    # Add NOT NULL checks
    validator.add_check(
        name="transaction_id_not_null",
        check_type=CheckType.NOT_NULL,
        column="transaction_id",
        level="error"
    )
    
    validator.add_check(
        name="customer_id_not_null",
        check_type=CheckType.NOT_NULL,
        column="customer_id",
        level="error"
    )
    
    # Add uniqueness check
    validator.add_check(
        name="transaction_id_unique",
        check_type=CheckType.UNIQUE,
        column="transaction_id",
        level="error"
    )
    
    # Add range checks
    validator.add_check(
        name="amount_range",
        check_type=CheckType.BETWEEN,
        column="amount",
        min_value=0,
        max_value=1000000,
        level="error"
    )
    
    # Add set membership checks
    validator.add_check(
        name="valid_status",
        check_type=CheckType.IN_SET,
        column="status",
        valid_values=["pending", "processing", "completed", "cancelled", "refunded"],
        level="error"
    )
    
    validator.add_check(
        name="valid_payment_method",
        check_type=CheckType.IN_SET,
        column="payment_method",
        valid_values=["credit_card", "debit_card", "paypal", "bank_transfer"],
        level="warning"
    )
    
    # Add row-level business logic
    validator.add_rule(
        Rule(
            name="discount_validation",
            expression="discount <= amount * 0.5",
            level="error",
            description="Discount cannot exceed 50% of amount"
        )
    )
    
    validator.add_rule(
        Rule(
            name="discount_non_negative",
            expression="discount >= 0",
            level="error",
            description="Discount must be non-negative"
        )
    )
    
    # Validate and return clean data
    return validator.validate_dlt(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Silver Layer with Enrichment

# COMMAND ----------

@dlt.table(
    name="silver_transactions_enriched",
    comment="Silver transactions with calculated fields"
)
def silver_transactions_enriched():
    """
    Enrich silver data with calculated fields
    """
    return (
        dlt.read("silver_transactions")
        .withColumn("net_amount", F.col("amount") - F.col("discount"))
        .withColumn("discount_percentage", 
                   (F.col("discount") / F.col("amount") * 100).cast("decimal(5,2)"))
        .withColumn("processed_date", F.current_date())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Gold Layer - Aggregated Business Metrics

# COMMAND ----------

@dlt.table(
    name="gold_daily_transaction_summary",
    comment="Daily transaction summary metrics"
)
def gold_daily_transaction_summary():
    """
    Gold layer: Business aggregations
    - Daily summaries
    - KPI calculations
    - Quality validated data only
    """
    return (
        dlt.read("silver_transactions_enriched")
        .groupBy(
            F.to_date("created_at").alias("date"),
            "status",
            "payment_method"
        )
        .agg(
            F.count("*").alias("transaction_count"),
            F.sum("amount").alias("total_amount"),
            F.sum("discount").alias("total_discount"),
            F.sum("net_amount").alias("total_net_amount"),
            F.avg("amount").alias("avg_amount"),
            F.max("amount").alias("max_amount"),
            F.min("amount").alias("min_amount")
        )
    )

# COMMAND ----------

@dlt.table(
    name="gold_customer_summary",
    comment="Customer transaction summary"
)
def gold_customer_summary():
    """
    Customer-level aggregations
    """
    return (
        dlt.read("silver_transactions_enriched")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("total_transactions"),
            F.sum("net_amount").alias("lifetime_value"),
            F.avg("net_amount").alias("avg_transaction_value"),
            F.max("created_at").alias("last_transaction_date"),
            F.min("created_at").alias("first_transaction_date")
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Quality Metrics Tracking

# COMMAND ----------

@dlt.table(
    name="quality_metrics",
    comment="Data quality tracking metrics from DQX validation"
)
def quality_metrics():
    """
    Capture and store quality metrics from DQX validation
    """
    return DLTValidator.get_metrics_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Combining DLT Expectations and DQX
# MAGIC
# MAGIC ### 4.1 Use DLT for Simple Checks, DQX for Complex

# COMMAND ----------

@dlt.table(
    name="orders_combined_validation",
    comment="Orders with both DLT expectations and DQX validation"
)
@dlt.expect_all({
    "valid_timestamp": "created_at IS NOT NULL",
    "positive_amount": "amount > 0"
})
def orders_combined_validation():
    """
    Combine DLT expectations with DQX validation
    - DLT: Simple SQL checks
    - DQX: Complex business logic
    """
    # Read source
    df = dlt.read_stream("bronze_transactions")
    
    # Apply DQX validations for complex rules
    validator = DLTValidator()
    
    # Complex pattern matching
    validator.add_check(
        name="transaction_id_format",
        check_type=CheckType.REGEX_MATCH,
        column="transaction_id",
        pattern=r"^TXN\d{6,}$",
        level="warning",
        reaction=Reaction.MARK
    )
    
    # Complex business rules
    validator.add_rule(
        Rule(
            name="high_value_secure_payment",
            expression="amount < 5000 OR payment_method IN ('credit_card', 'bank_transfer')",
            level="warning",
            description="High-value transactions should use secure payment methods"
        )
    )
    
    return validator.validate_dlt(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Quarantine Table in DLT

# COMMAND ----------

@dlt.table(
    name="quarantined_transactions",
    comment="Transactions that failed quality validation"
)
def quarantined_transactions():
    """
    Separate table for quarantined records
    - Failed quality checks
    - Needs manual review
    - Can be reprocessed after fixes
    """
    # This would be populated by DQX quarantine mechanism
    # Quarantine configuration in silver layer handles this
    return spark.table("dqx_quarantine.transactions")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Pipeline Configuration
# MAGIC
# MAGIC ### DLT Pipeline Settings (Configure in DLT UI)
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "dqx_quality_pipeline",
# MAGIC   "storage": "/mnt/datalake/pipelines/dqx",
# MAGIC   "target": "dqx_demo",
# MAGIC   "continuous": false,
# MAGIC   "development": true,
# MAGIC   "configuration": {
# MAGIC     "dqx.quarantine.enabled": "true",
# MAGIC     "dqx.quarantine.catalog": "dqx_quarantine",
# MAGIC     "dqx.metrics.enabled": "true"
# MAGIC   },
# MAGIC   "clusters": [
# MAGIC     {
# MAGIC       "label": "default",
# MAGIC       "autoscale": {
# MAGIC         "min_workers": 1,
# MAGIC         "max_workers": 5
# MAGIC       }
# MAGIC     }
# MAGIC   ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Monitoring and Observability
# MAGIC
# MAGIC ### 7.1 Quality Metrics Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query quality metrics from DLT pipeline
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   check_name,
# MAGIC   check_type,
# MAGIC   SUM(records_processed) as total_processed,
# MAGIC   SUM(records_failed) as total_failed,
# MAGIC   AVG(pass_rate) as avg_pass_rate,
# MAGIC   MAX(timestamp) as last_run
# MAGIC FROM dqx_demo.quality_metrics
# MAGIC GROUP BY table_name, check_name, check_type
# MAGIC ORDER BY total_failed DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Pipeline Health Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Overall pipeline quality health
# MAGIC SELECT 
# MAGIC   date_trunc('hour', timestamp) as hour,
# MAGIC   COUNT(DISTINCT table_name) as tables_processed,
# MAGIC   SUM(records_processed) as total_records,
# MAGIC   SUM(records_failed) as total_failures,
# MAGIC   AVG(pass_rate) * 100 as avg_pass_rate_pct
# MAGIC FROM dqx_demo.quality_metrics
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY date_trunc('hour', timestamp)
# MAGIC ORDER BY hour DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Failed Checks Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Identify which checks are failing most
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   check_name,
# MAGIC   COUNT(*) as failure_count,
# MAGIC   SUM(records_failed) as total_failed_records,
# MAGIC   MAX(timestamp) as last_failure
# MAGIC FROM dqx_demo.quality_metrics
# MAGIC WHERE records_failed > 0
# MAGIC   AND timestamp >= current_timestamp() - INTERVAL 7 DAYS
# MAGIC GROUP BY table_name, check_name
# MAGIC ORDER BY total_failed_records DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Advanced Patterns
# MAGIC
# MAGIC ### 8.1 Incremental Processing with Quality Checks

# COMMAND ----------

@dlt.table(
    name="silver_transactions_incremental",
    comment="Incrementally processed transactions with quality checks"
)
def silver_transactions_incremental():
    """
    Incremental processing with DQX validation
    """
    # Read incrementally from bronze
    bronze_df = dlt.read_stream("bronze_transactions")
    
    # Apply deduplication
    dedup_df = bronze_df.dropDuplicates(["transaction_id"])
    
    # Create validator
    validator = DLTValidator()
    
    # Add checks
    validator.add_check(
        check_type=CheckType.NOT_NULL,
        column="transaction_id",
        level="error"
    )
    
    # Validate
    return validator.validate_dlt(dedup_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 SCD Type 2 with Quality Validation

# COMMAND ----------

@dlt.table(
    name="silver_customer_dimension",
    comment="Customer dimension with SCD Type 2 and quality checks"
)
def silver_customer_dimension():
    """
    Slowly Changing Dimension with quality validation
    """
    # Read customer updates
    updates_df = dlt.read_stream("bronze_customer_updates")
    
    # Validate before applying SCD
    validator = DLTValidator()
    
    validator.add_check(
        check_type=CheckType.NOT_NULL,
        column="customer_id",
        level="error"
    )
    
    validator.add_check(
        check_type=CheckType.NOT_NULL,
        column="effective_date",
        level="error"
    )
    
    # Validate first
    validated_df = validator.validate_dlt(updates_df)
    
    # Apply SCD Type 2 logic
    return (
        validated_df
        .withColumn("is_current", F.lit(True))
        .withColumn("end_date", F.lit(None).cast("timestamp"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **DQX integrates seamlessly** with DLT pipelines via DLTValidator
# MAGIC
# MAGIC ✅ **Combine DLT expectations** (simple) with DQX checks (complex)
# MAGIC
# MAGIC ✅ **Medallion architecture** benefits from layered quality checks
# MAGIC
# MAGIC ✅ **Quality metrics** tracked automatically in DLT pipelines
# MAGIC
# MAGIC ✅ **Quarantine in DLT** isolates bad data for review
# MAGIC
# MAGIC ✅ **Use DLT UI** for pipeline monitoring + DQX metrics for quality
# MAGIC
# MAGIC ✅ **Incremental and SCD** patterns work with DQX validation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Layer your checks** - Basic in Bronze, comprehensive in Silver
# MAGIC 2. **Use DLT for simple** - Save DQX for complex validations
# MAGIC 3. **Track metrics** - Enable quality metrics table
# MAGIC 4. **Monitor pipeline** - Use DLT UI + quality dashboards
# MAGIC 5. **Quarantine strategically** - Review and reprocess bad data
# MAGIC 6. **Version your rules** - Store quality rules in configuration
# MAGIC 7. **Test thoroughly** - Use development mode before production

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Configuration-based quality checks** using YAML/JSON
# MAGIC 2. **Dynamic rule loading** from config files
# MAGIC 3. **Environment-specific rules** (dev, staging, prod)
# MAGIC 4. **Version control** for quality standards
# MAGIC
# MAGIC **Continue to**: `07_Configuration_Based_Quality_Checks`
