# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture for Data Quality
# MAGIC
# MAGIC **Module 2: Quality Patterns Across Bronze-Silver-Gold**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 200/300       |
# MAGIC | Type            | Lecture       |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will understand:
# MAGIC
# MAGIC 1. **Bronze Layer Quality** - Raw data quality patterns and checks
# MAGIC 2. **Silver Layer Quality** - Business validation and cleansing strategies
# MAGIC 3. **Gold Layer Quality** - Aggregation validation and consistency checks
# MAGIC 4. **Layer-Specific Best Practices** - Where to implement different quality rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Medallion Architecture Overview
# MAGIC
# MAGIC The Medallion Architecture provides a structured approach to data quality:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    MEDALLION ARCHITECTURE                    │
# MAGIC ├─────────────────────────────────────────────────────────────┤
# MAGIC │                                                              │
# MAGIC │  BRONZE (Raw)           SILVER (Refined)      GOLD (Curated)│
# MAGIC │  ────────────           ────────────────      ──────────────│
# MAGIC │                                                              │
# MAGIC │  • Raw ingestion        • Cleansed data      • Business     │
# MAGIC │  • Minimal checks       • Validated          • Aggregated   │
# MAGIC │  • Schema validation    • Enriched           • Analytics    │
# MAGIC │  • Audit trail          • Quality scored     • Validated    │
# MAGIC │                                                              │
# MAGIC │  Quality Focus:         Quality Focus:       Quality Focus: │
# MAGIC │  - Completeness         - Accuracy           - Consistency  │
# MAGIC │  - Structure            - Business Rules     - Totals       │
# MAGIC │  - Metadata             - Deduplication      - Trends       │
# MAGIC │                                                              │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Layer: Raw Data Quality
# MAGIC
# MAGIC ### Purpose
# MAGIC Capture data exactly as received with minimal transformation
# MAGIC
# MAGIC ### Quality Focus
# MAGIC - ✅ Schema validation (can we parse the data?)
# MAGIC - ✅ Completeness checks (are critical fields present?)
# MAGIC - ✅ Data capture metadata (source, timestamp, batch ID)
# MAGIC - ✅ Full data retention (no filtering at this stage)
# MAGIC - ✅ Audit trail (track all ingestion events)
# MAGIC
# MAGIC ### What NOT to Do in Bronze
# MAGIC - ❌ Apply business rules
# MAGIC - ❌ Filter out "bad" records
# MAGIC - ❌ Transform or cleanse data
# MAGIC - ❌ Reject records for quality issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Layer Quality Pattern

# COMMAND ----------

# Example: Bronze layer ingestion with quality checks
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def ingest_to_bronze(source_path, bronze_table, source_system):
    """
    Ingest raw data to Bronze layer with minimal quality checks
    """
    
    # Define expected schema
    expected_schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("order_date", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True)
    ])
    
    try:
        # Read with schema validation
        df = spark.read.schema(expected_schema).json(source_path)
        
        # Add metadata columns (Bronze pattern)
        df_bronze = df \
            .withColumn("source_system", lit(source_system)) \
            .withColumn("ingestion_timestamp", current_timestamp()) \
            .withColumn("source_file", input_file_name()) \
            .withColumn("_ingested_date", current_timestamp().cast("date"))
        
        # Bronze quality checks (minimal)
        total_records = df_bronze.count()
        
        # Check: Did we get any data?
        if total_records == 0:
            raise Exception("No records ingested")
        
        # Check: Are critical technical fields present?
        critical_fields = ["source_system", "ingestion_timestamp"]
        for field in critical_fields:
            if field not in df_bronze.columns:
                raise Exception(f"Critical metadata field missing: {field}")
        
        # Write to Bronze (append-only)
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("_ingested_date") \
            .saveAsTable(bronze_table)
        
        print(f"✅ Bronze ingestion successful: {total_records} records")
        
        return {
            'status': 'success',
            'records_ingested': total_records,
            'table': bronze_table
        }
        
    except Exception as e:
        print(f"❌ Bronze ingestion failed: {str(e)}")
        
        # Log failure but don't lose data
        # In production, route to error/quarantine table
        return {
            'status': 'failed',
            'error': str(e),
            'table': bronze_table
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Quality Checklist
# MAGIC
# MAGIC | Check | Purpose | Action on Failure |
# MAGIC |-------|---------|-------------------|
# MAGIC | Schema compatibility | Can we parse the file? | Log error, move to error folder |
# MAGIC | Record count > 0 | Did we get any data? | Alert, investigate source |
# MAGIC | Critical fields present | Can we track lineage? | Reject batch |
# MAGIC | Source metadata | Can we audit? | Add default values |
# MAGIC | Partition key valid | Can we organize? | Use default partition |

# COMMAND ----------

# Example: Bronze validation function
def validate_bronze_quality(df):
    """
    Validate raw data quality at Bronze layer
    """
    
    checks = {}
    
    # Check 1: Schema match (columns present)
    required_columns = ["order_id", "customer_id", "order_date", "amount", "status"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    checks['schema_match'] = {
        'passed': len(missing_columns) == 0,
        'missing_columns': missing_columns
    }
    
    # Check 2: Record count
    record_count = df.count()
    checks['record_count'] = {
        'passed': record_count > 0,
        'count': record_count
    }
    
    # Check 3: Critical fields not ALL null
    from pyspark.sql.functions import col
    for field in ["order_id", "customer_id"]:
        non_null_count = df.filter(col(field).isNotNull()).count()
        checks[f'{field}_not_all_null'] = {
            'passed': non_null_count > 0,
            'non_null_count': non_null_count
        }
    
    # Check 4: Metadata present
    metadata_fields = ["source_system", "ingestion_timestamp"]
    for field in metadata_fields:
        checks[f'metadata_{field}'] = {
            'passed': field in df.columns
        }
    
    # Overall status
    all_passed = all(check['passed'] for check in checks.values())
    
    return {
        'overall_passed': all_passed,
        'checks': checks
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver Layer: Business Quality
# MAGIC
# MAGIC ### Purpose
# MAGIC Cleansed, validated, and enriched data ready for analytics
# MAGIC
# MAGIC ### Quality Focus
# MAGIC - ✅ Business rule validation
# MAGIC - ✅ Data standardization and normalization
# MAGIC - ✅ Duplicate detection and resolution
# MAGIC - ✅ Referential integrity
# MAGIC - ✅ Data enrichment and derivation
# MAGIC - ✅ Quality scoring per record
# MAGIC
# MAGIC ### Transformation Goals
# MAGIC - Apply business logic
# MAGIC - Cleanse and standardize
# MAGIC - Enrich with reference data
# MAGIC - Flag or quarantine quality issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Quality Pattern

# COMMAND ----------

# Example: Bronze to Silver transformation with quality
from pyspark.sql.functions import when, col, regexp_replace, trim, upper, row_number
from pyspark.sql.window import Window

def transform_to_silver(bronze_table, silver_table):
    """
    Transform Bronze to Silver with business quality validation
    """
    
    df_bronze = spark.table(bronze_table)
    
    # Cleansing transformations
    df_cleansed = df_bronze \
        .withColumn("status", upper(trim(col("status")))) \
        .withColumn("amount", col("amount").cast("decimal(10,2)"))
    
    # Deduplication (keep latest record per order_id)
    window = Window.partitionBy("order_id").orderBy(col("ingestion_timestamp").desc())
    df_deduped = df_cleansed \
        .withColumn("row_num", row_number().over(window)) \
        .filter(col("row_num") == 1) \
        .drop("row_num")
    
    # Quality scoring
    df_quality_scored = df_deduped \
        .withColumn("quality_score", lit(100)) \
        .withColumn("quality_score", 
            when(col("order_id").isNull(), col("quality_score") - 30)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when(col("customer_id").isNull(), col("quality_score") - 30)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when(col("amount").isNull() | (col("amount") <= 0), col("quality_score") - 20)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when(~col("status").isin("PENDING", "COMPLETED", "CANCELLED"), col("quality_score") - 20)
            .otherwise(col("quality_score"))
        )
    
    # Add quality flags
    df_silver = df_quality_scored \
        .withColumn("is_valid", col("quality_score") >= 70) \
        .withColumn("silver_timestamp", current_timestamp())
    
    # Write to Silver (only valid records, quarantine others)
    df_valid = df_silver.filter(col("is_valid"))
    df_invalid = df_silver.filter(~col("is_valid"))
    
    # Write valid records to Silver
    df_valid.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(silver_table)
    
    # Write invalid records to quarantine
    if df_invalid.count() > 0:
        df_invalid.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(f"{silver_table}_quarantine")
    
    return {
        'valid_records': df_valid.count(),
        'quarantined_records': df_invalid.count(),
        'avg_quality_score': df_silver.agg({'quality_score': 'avg'}).collect()[0][0]
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Quality Checklist
# MAGIC
# MAGIC | Dimension | Checks | Example |
# MAGIC |-----------|--------|---------|
# MAGIC | **Accuracy** | Format validation, Range checks | Email regex, Amount > 0 |
# MAGIC | **Completeness** | Required field validation | NOT NULL on business keys |
# MAGIC | **Consistency** | Cross-field rules, Referential integrity | Ship date >= Order date |
# MAGIC | **Validity** | Enumeration checks, Type validation | Status IN (valid values) |
# MAGIC | **Uniqueness** | Primary key validation, Duplicate detection | One order per order_id |
# MAGIC | **Timeliness** | Data freshness, SLA compliance | Within 24 hours |

# COMMAND ----------

# Example: Comprehensive Silver validation
def validate_silver_quality(df):
    """
    Comprehensive business quality validation for Silver layer
    """
    
    results = {}
    total_records = df.count()
    
    # Accuracy checks
    results['valid_emails'] = df.filter(
        col("email").rlike(r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
    ).count()
    
    results['valid_amounts'] = df.filter(
        (col("amount") >= 0.01) & (col("amount") <= 1000000)
    ).count()
    
    # Completeness checks
    required_fields = ['order_id', 'customer_id', 'order_date', 'amount', 'status']
    for field in required_fields:
        non_null = df.filter(col(field).isNotNull()).count()
        results[f'{field}_completeness'] = (non_null / total_records * 100) if total_records > 0 else 0
    
    # Uniqueness check
    distinct_orders = df.select("order_id").distinct().count()
    results['uniqueness_rate'] = (distinct_orders / total_records * 100) if total_records > 0 else 0
    
    # Validity checks
    valid_statuses = df.filter(col("status").isin("PENDING", "COMPLETED", "CANCELLED")).count()
    results['valid_status_rate'] = (valid_statuses / total_records * 100) if total_records > 0 else 0
    
    # Overall quality score
    completeness_avg = sum([v for k, v in results.items() if '_completeness' in k]) / len(required_fields)
    results['overall_quality_score'] = (
        results['valid_status_rate'] * 0.3 +
        completeness_avg * 0.4 +
        results['uniqueness_rate'] * 0.3
    )
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold Layer: Aggregation Quality
# MAGIC
# MAGIC ### Purpose
# MAGIC Business-ready metrics and aggregations for reporting and analytics
# MAGIC
# MAGIC ### Quality Focus
# MAGIC - ✅ Aggregation accuracy (sums, counts, averages)
# MAGIC - ✅ Metric consistency (derived metrics match source)
# MAGIC - ✅ Historical comparison validation
# MAGIC - ✅ Business logic correctness
# MAGIC - ✅ Performance optimization
# MAGIC - ✅ Completeness of dimensions
# MAGIC
# MAGIC ### Key Validations
# MAGIC - Reconciliation with source (Silver) data
# MAGIC - Cross-metric validation
# MAGIC - Time series completeness
# MAGIC - Anomaly detection

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Quality Pattern

# COMMAND ----------

# Example: Silver to Gold aggregation with validation
from pyspark.sql.functions import sum, count, avg, min, max, date_trunc

def create_gold_daily_summary(silver_table, gold_table):
    """
    Create Gold layer daily summary with quality validation
    """
    
    df_silver = spark.table(silver_table)
    
    # Create aggregations
    df_gold = df_silver \
        .filter(col("status") == "COMPLETED") \
        .groupBy(
            date_trunc("day", col("order_date")).alias("order_date"),
            col("status")
        ).agg(
            count("order_id").alias("order_count"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            min("amount").alias("min_order_value"),
            max("amount").alias("max_order_value"),
            count(col("customer_id").distinct()).alias("unique_customers")
        ) \
        .withColumn("created_timestamp", current_timestamp())
    
    # Write to Gold
    df_gold.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(gold_table)
    
    return df_gold

# Validate Gold aggregations
def validate_gold_quality(gold_table, silver_table):
    """
    Validate Gold layer aggregation quality
    """
    
    df_gold = spark.table(gold_table)
    df_silver = spark.table(silver_table).filter(col("status") == "COMPLETED")
    
    validations = {}
    
    # Validation 1: Reconciliation - Gold totals match Silver source
    gold_total_revenue = df_gold.agg(sum("total_revenue")).collect()[0][0] or 0
    silver_total_revenue = df_silver.agg(sum("amount")).collect()[0][0] or 0
    
    diff_pct = abs(gold_total_revenue - silver_total_revenue) / silver_total_revenue * 100 if silver_total_revenue > 0 else 0
    
    validations['reconciliation'] = {
        'passed': diff_pct < 0.01,  # Less than 0.01% difference
        'gold_total': gold_total_revenue,
        'silver_total': silver_total_revenue,
        'difference_pct': diff_pct
    }
    
    # Validation 2: Metric consistency - avg_order_value = total_revenue / order_count
    df_gold_check = df_gold.withColumn(
        "calculated_avg",
        col("total_revenue") / col("order_count")
    ).withColumn(
        "avg_matches",
        abs(col("calculated_avg") - col("avg_order_value")) < 0.01
    )
    
    inconsistent_count = df_gold_check.filter(~col("avg_matches")).count()
    validations['metric_consistency'] = {
        'passed': inconsistent_count == 0,
        'inconsistent_records': inconsistent_count
    }
    
    # Validation 3: Completeness - No missing dates in time series
    date_range = df_silver.agg(
        min("order_date").alias("min_date"),
        max("order_date").alias("max_date")
    ).collect()[0]
    
    if date_range.min_date and date_range.max_date:
        expected_days = (date_range.max_date - date_range.min_date).days + 1
        actual_days = df_gold.select("order_date").distinct().count()
        
        validations['time_series_completeness'] = {
            'passed': actual_days == expected_days,
            'expected_days': expected_days,
            'actual_days': actual_days,
            'missing_days': expected_days - actual_days
        }
    
    return validations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Quality Checklist
# MAGIC
# MAGIC | Check | Purpose | Validation Method |
# MAGIC |-------|---------|-------------------|
# MAGIC | Reconciliation | Gold totals = Silver source | Compare aggregated sums |
# MAGIC | Metric math | Derived metrics correct | Recalculate and compare |
# MAGIC | Completeness | All time periods present | Check for gaps in series |
# MAGIC | Reasonableness | Values in expected ranges | Statistical bounds checking |
# MAGIC | Trend validation | No unexpected spikes/drops | Compare to historical patterns |
# MAGIC | Cross-metric rules | Related metrics consistent | Business logic validation |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Strategy by Layer
# MAGIC
# MAGIC ### Bronze Layer Strategy
# MAGIC
# MAGIC **Philosophy**: "Preserve Everything, Validate Structure"
# MAGIC
# MAGIC ```python
# MAGIC # Bronze Quality Pattern
# MAGIC - Minimal rejection (only unparseable data)
# MAGIC - Append-only writes
# MAGIC - Schema evolution enabled
# MAGIC - Change Data Feed enabled
# MAGIC - Partition by ingestion date
# MAGIC - Full audit trail
# MAGIC ```
# MAGIC
# MAGIC **Key Principle**: Never lose source data, even if it has quality issues

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Strategy
# MAGIC
# MAGIC **Philosophy**: "Cleanse, Validate, Score"
# MAGIC
# MAGIC ```python
# MAGIC # Silver Quality Pattern
# MAGIC - Quarantine invalid records (don't discard)
# MAGIC - Add quality score columns
# MAGIC - Flag quality issues
# MAGIC - Implement progressive validation:
# MAGIC   1. Critical rules (reject if fail)
# MAGIC   2. Important rules (flag if fail)
# MAGIC   3. Nice-to-have rules (log if fail)
# MAGIC - Track data lineage
# MAGIC - Enable self-service remediation
# MAGIC ```
# MAGIC
# MAGIC **Key Principle**: Transform and validate, but enable recovery

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Strategy
# MAGIC
# MAGIC **Philosophy**: "Validate Aggregations, Ensure Consistency"
# MAGIC
# MAGIC ```python
# MAGIC # Gold Quality Pattern
# MAGIC - Reconcile with Silver source
# MAGIC - Validate metric relationships
# MAGIC - Check time series completeness
# MAGIC - Compare to expected totals
# MAGIC - Detect statistical anomalies
# MAGIC - Monitor query performance
# MAGIC - Validate against business rules
# MAGIC ```
# MAGIC
# MAGIC **Key Principle**: Analytics-ready data must be trustworthy

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example: End-to-End Quality Pipeline

# COMMAND ----------

# Complete quality pipeline across all layers
def quality_aware_pipeline(source_path, catalog, schema):
    """
    End-to-end data pipeline with layer-specific quality checks
    """
    
    bronze_table = f"{catalog}.{schema}.orders_bronze"
    silver_table = f"{catalog}.{schema}.orders_silver"
    gold_table = f"{catalog}.{schema}.orders_gold_daily"
    
    # Step 1: Bronze ingestion
    print("=" * 60)
    print("BRONZE LAYER")
    print("=" * 60)
    bronze_result = ingest_to_bronze(source_path, bronze_table, "web_api")
    
    if bronze_result['status'] == 'success':
        # Validate Bronze
        df_bronze = spark.table(bronze_table)
        bronze_validation = validate_bronze_quality(df_bronze)
        print(f"Bronze Quality: {'✅ PASSED' if bronze_validation['overall_passed'] else '❌ FAILED'}")
        
        if bronze_validation['overall_passed']:
            # Step 2: Silver transformation
            print("\n" + "=" * 60)
            print("SILVER LAYER")
            print("=" * 60)
            silver_result = transform_to_silver(bronze_table, silver_table)
            print(f"Valid records: {silver_result['valid_records']}")
            print(f"Quarantined: {silver_result['quarantined_records']}")
            print(f"Avg quality score: {silver_result['avg_quality_score']:.2f}")
            
            # Validate Silver
            df_silver = spark.table(silver_table)
            silver_validation = validate_silver_quality(df_silver)
            print(f"Silver Quality Score: {silver_validation['overall_quality_score']:.2f}%")
            
            # Step 3: Gold aggregation
            print("\n" + "=" * 60)
            print("GOLD LAYER")
            print("=" * 60)
            df_gold = create_gold_daily_summary(silver_table, gold_table)
            print(f"Gold records created: {df_gold.count()}")
            
            # Validate Gold
            gold_validation = validate_gold_quality(gold_table, silver_table)
            print(f"Reconciliation: {'✅ PASSED' if gold_validation['reconciliation']['passed'] else '❌ FAILED'}")
            print(f"Metric Consistency: {'✅ PASSED' if gold_validation['metric_consistency']['passed'] else '❌ FAILED'}")
            
            print("\n" + "=" * 60)
            print("PIPELINE COMPLETE")
            print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices by Layer
# MAGIC
# MAGIC ### Bronze Best Practices
# MAGIC 1. **Keep it simple** - Minimal transformation, maximum preservation
# MAGIC 2. **Schema enforcement** - Define expected schema, log violations
# MAGIC 3. **Audit everything** - Source, timestamp, file name, batch ID
# MAGIC 4. **Partition wisely** - By ingestion date for lifecycle management
# MAGIC 5. **Enable CDF** - Track all changes for quality auditing
# MAGIC
# MAGIC ### Silver Best Practices
# MAGIC 1. **Progressive validation** - Critical → Important → Nice-to-have
# MAGIC 2. **Quality scoring** - Tag each record with quality score
# MAGIC 3. **Quarantine, don't discard** - Enable recovery and analysis
# MAGIC 4. **Deduplicate intelligently** - Keep most recent or highest quality
# MAGIC 5. **Enrich thoughtfully** - Add value through joins and derivations
# MAGIC
# MAGIC ### Gold Best Practices
# MAGIC 1. **Always reconcile** - Validate aggregations against source
# MAGIC 2. **Check completeness** - Ensure all time periods/dimensions present
# MAGIC 3. **Validate relationships** - Cross-metric consistency checks
# MAGIC 4. **Monitor trends** - Detect unusual changes
# MAGIC 5. **Optimize performance** - Quality checks shouldn't slow queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Layer-Specific Focus
# MAGIC - **Bronze**: Structural quality, completeness, auditability
# MAGIC - **Silver**: Business quality, validation, enrichment
# MAGIC - **Gold**: Analytical quality, consistency, accuracy
# MAGIC
# MAGIC ### Quality Gates
# MAGIC - Bronze → Silver: Pass structural validation
# MAGIC - Silver → Gold: Pass business validation
# MAGIC - Gold → Consumption: Pass reconciliation
# MAGIC
# MAGIC ### Remember
# MAGIC - Each layer has different quality goals
# MAGIC - Quality checks should match layer purpose
# MAGIC - Progressive validation reduces complexity
# MAGIC - Always enable recovery mechanisms
# MAGIC - Measure quality at every layer

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Module 3: Delta Lake Quality Features** to learn:
# MAGIC - Built-in constraints and expectations
# MAGIC - Schema evolution and enforcement
# MAGIC - Change Data Feed for quality tracking
# MAGIC - Delta Live Tables quality patterns
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 45 minutes | **Level**: 200/300 | **Type**: Lecture
