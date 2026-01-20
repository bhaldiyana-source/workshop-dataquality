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

import dlt
from dqx import DLTValidator, CheckType, Reaction, Rule
from pyspark.sql import functions as F

print("✅ DLT and DQX imported")

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
# MAGIC **Continue to**: `07_Demo_Configuration_Based_Quality_Checks`
