# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Silver Layer Quality
# MAGIC
# MAGIC **Module 7: Business Quality Validation and Data Cleansing**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC 1. **Business validation** - Apply business rules and logic
# MAGIC 2. **Data cleansing** - Standardize and normalize data
# MAGIC 3. **Deduplication** - Handle duplicate records
# MAGIC 4. **Quality scoring** - Assign quality scores per record
# MAGIC 5. **Quarantine pattern** - Separate invalid records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any
from datetime import datetime
import json

catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Validation Framework

# COMMAND ----------

@dataclass
class ValidationResult:
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    failure_rate: float
    details: Dict[str, Any]
    severity: str = "ERROR"
    
    @property
    def success_rate(self):
        return 1 - self.failure_rate

class ValidationRule(ABC):
    def __init__(self, rule_name: str, severity: str = 'ERROR'):
        self.rule_name = rule_name
        self.severity = severity
    
    @abstractmethod
    def validate(self, df) -> ValidationResult:
        pass

class NotNullRule(ValidationRule):
    def __init__(self, column_name: str, severity: str = 'ERROR'):
        super().__init__(f"not_null_{column_name}", severity)
        self.column_name = column_name
    
    def validate(self, df) -> ValidationResult:
        total = df.count()
        null_count = df.filter(col(self.column_name).isNull()).count()
        return ValidationResult(
            rule_name=self.rule_name,
            passed=null_count == 0,
            failed_count=null_count,
            total_count=total,
            failure_rate=null_count / total if total > 0 else 0,
            details={'column': self.column_name, 'null_count': null_count},
            severity=self.severity
        )

class ValueRangeRule(ValidationRule):
    def __init__(self, column_name: str, min_value: float, max_value: float, severity: str = 'ERROR'):
        super().__init__(f"range_{column_name}", severity)
        self.column_name = column_name
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, df) -> ValidationResult:
        total = df.count()
        out_of_range = df.filter(
            (col(self.column_name) < self.min_value) | (col(self.column_name) > self.max_value)
        ).count()
        return ValidationResult(
            rule_name=self.rule_name,
            passed=out_of_range == 0,
            failed_count=out_of_range,
            total_count=total,
            failure_rate=out_of_range / total if total > 0 else 0,
            details={'column': self.column_name, 'violations': out_of_range},
            severity=self.severity
        )

class RegexRule(ValidationRule):
    def __init__(self, column_name: str, pattern: str, pattern_name: str, severity: str = 'ERROR'):
        super().__init__(f"regex_{column_name}_{pattern_name}", severity)
        self.column_name = column_name
        self.pattern = pattern
        self.pattern_name = pattern_name
    
    def validate(self, df) -> ValidationResult:
        total = df.filter(col(self.column_name).isNotNull()).count()
        invalid = df.filter(
            col(self.column_name).isNotNull() & ~col(self.column_name).rlike(self.pattern)
        ).count()
        return ValidationResult(
            rule_name=self.rule_name,
            passed=invalid == 0,
            failed_count=invalid,
            total_count=total,
            failure_rate=invalid / total if total > 0 else 0,
            details={'column': self.column_name, 'invalid_count': invalid},
            severity=self.severity
        )

class ValidationEngine:
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.results: List[ValidationResult] = []
    
    def add_rule(self, rule):
        self.rules.append(rule)
        return self
    
    def add_rules(self, rules):
        self.rules.extend(rules)
        return self
    
    def validate(self, df):
        self.results = []
        for rule in self.rules:
            self.results.append(rule.validate(df))
        return self.get_summary()
    
    def get_summary(self):
        total_rules = len(self.results)
        passed_rules = sum(1 for r in self.results if r.passed)
        critical_failures = [r for r in self.results if not r.passed and r.severity == 'ERROR']
        return {
            'total_rules': total_rules,
            'passed_rules': passed_rules,
            'failed_rules': total_rules - passed_rules,
            'critical_failures': len(critical_failures),
            'overall_passed': len(critical_failures) == 0,
            'results': self.results
        }

print("✅ Validation framework loaded")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Bronze Test Data

# COMMAND ----------

bronze_data = [
    (1, "john.doe@example.com", "555-123-4567", 25, 150.50, "2024-01-15", "completed", 100),
    (2, "JANE@TEST.COM", "555-234-5678", 32, 275.00, "2024-01-16", "COMPLETED", 200),
    (3, "invalid-email", "555-345-6789", 45, 89.99, "2024-01-17", "pending", 300),
    (4, "bob@test.com", "(555) 456-7890", 28, 450.00, "2024-01-18", "completed", 400),
    (5, "alice@example.com", "555.567.8901", 35, 125.75, "2024-01-19", "completed", 500),
    (6, "charlie@test.com", "555-678-9012", 150, 200.00, "2024-01-20", "cancelled", 600),  # Age outlier
    (7, "diana@example.com", "555-789-0123", 29, -50.00, "2024-01-21", "pending", 700),  # Negative amount
    (1, "john.doe@example.com", "555-123-4567", 25, 150.50, "2024-01-15", "completed", 100),  # Duplicate
    (9, "eve@test.com", "555-890-1234", None, 300.00, "2024-01-23", "completed", 900),  # NULL age
    (10, "frank@test.com", "555-901-2345", 28, 225.00, "2024-01-24", "invalid_status", 1000),
]

bronze_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("age", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("order_date", StringType()),
    StructField("status", StringType()),
    StructField("customer_id", IntegerType())
])

df_bronze = spark.createDataFrame(bronze_data, bronze_schema)
df_bronze.write.format("delta").mode("overwrite").saveAsTable("orders_bronze_for_silver")

print("✅ Bronze test data created")
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Data Cleansing
# MAGIC
# MAGIC Standardize and normalize data

# COMMAND ----------

def cleanse_silver_data(df_bronze):
    """
    Apply cleansing transformations for Silver layer
    """
    
    df_cleansed = df_bronze \
        .withColumn("email", lower(trim(col("email")))) \
        .withColumn("status", upper(trim(col("status")))) \
        .withColumn("phone", regexp_replace(col("phone"), r'[^0-9-]', '')) \
        .withColumn("phone", regexp_replace(col("phone"), r'^(\d{3})(\d{3})(\d{4})$', '$1-$2-$3')) \
        .withColumn("amount", col("amount").cast("decimal(10,2)")) \
        .withColumn("order_date", col("order_date").cast("date"))
    
    return df_cleansed

df_cleansed = cleanse_silver_data(df_bronze)

print("Data before and after cleansing:")
print("\nBefore (sample):")
display(df_bronze.select("email", "status", "phone").limit(3))

print("\nAfter:")
display(df_cleansed.select("email", "status", "phone").limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Deduplication
# MAGIC
# MAGIC Handle duplicate records

# COMMAND ----------

def deduplicate_records(df, key_columns, order_column="_ingestion_timestamp"):
    """
    Remove duplicates, keeping the latest record
    """
    
    # Add timestamp if not present
    if order_column not in df.columns:
        df = df.withColumn(order_column, current_timestamp())
    
    # Define window to keep latest record per key
    window = Window.partitionBy(*key_columns).orderBy(col(order_column).desc())
    
    df_deduped = df \
        .withColumn("_row_num", row_number().over(window)) \
        .filter(col("_row_num") == 1) \
        .drop("_row_num")
    
    duplicates_removed = df.count() - df_deduped.count()
    
    print(f"Duplicates removed: {duplicates_removed}")
    
    return df_deduped

# Add timestamp for deduplication
df_with_timestamp = df_cleansed.withColumn("_ingestion_timestamp", current_timestamp())

df_deduped = deduplicate_records(df_with_timestamp, ["order_id"])

print(f"Records before: {df_with_timestamp.count()}")
print(f"Records after: {df_deduped.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Quality Scoring
# MAGIC
# MAGIC Assign quality scores to each record

# COMMAND ----------

def add_quality_score(df):
    """
    Calculate quality score for each record (0-100)
    """
    
    df_scored = df.withColumn("quality_score", lit(100))
    
    # Deduct points for missing data
    df_scored = df_scored \
        .withColumn("quality_score", 
            when(col("order_id").isNull(), col("quality_score") - 30)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when(col("customer_id").isNull(), col("quality_score") - 30)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when(col("email").isNull(), col("quality_score") - 15)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when(col("age").isNull(), col("quality_score") - 10)
            .otherwise(col("quality_score"))
        )
    
    # Deduct for invalid formats
    df_scored = df_scored \
        .withColumn("quality_score",
            when(~col("email").rlike(r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$'), col("quality_score") - 15)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when(~col("status").isin("PENDING", "COMPLETED", "CANCELLED"), col("quality_score") - 20)
            .otherwise(col("quality_score"))
        )
    
    # Deduct for business rule violations
    df_scored = df_scored \
        .withColumn("quality_score",
            when((col("amount").isNull()) | (col("amount") <= 0), col("quality_score") - 20)
            .otherwise(col("quality_score"))
        ) \
        .withColumn("quality_score",
            when((col("age") < 0) | (col("age") > 120), col("quality_score") - 15)
            .otherwise(col("quality_score"))
        )
    
    # Add quality flag
    df_scored = df_scored \
        .withColumn("quality_tier",
            when(col("quality_score") >= 95, "GOLD")
            .when(col("quality_score") >= 80, "SILVER")
            .when(col("quality_score") >= 70, "BRONZE")
            .otherwise("QUARANTINE")
        )
    
    return df_scored

df_scored = add_quality_score(df_deduped)

print("Quality Score Distribution:")
display(df_scored.groupBy("quality_tier").count().orderBy("quality_tier"))

print("\nSample records with scores:")
display(df_scored.select("order_id", "email", "status", "amount", "quality_score", "quality_tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Split Valid and Quarantine Records

# COMMAND ----------

def split_valid_and_quarantine(df, quality_threshold=70):
    """
    Split DataFrame into valid and quarantine records
    """
    
    df_valid = df.filter(col("quality_score") >= quality_threshold)
    df_quarantine = df.filter(col("quality_score") < quality_threshold)
    
    return df_valid, df_quarantine

df_valid, df_quarantine = split_valid_and_quarantine(df_scored, quality_threshold=70)

print(f"Valid records (score >= 70): {df_valid.count()}")
print(f"Quarantine records (score < 70): {df_quarantine.count()}")

print("\nQuarantined records:")
display(df_quarantine.select("order_id", "email", "status", "quality_score", "quality_tier"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Add Silver Metadata

# COMMAND ----------

def add_silver_metadata(df):
    """
    Add Silver layer metadata
    """
    
    return df \
        .withColumn("_silver_timestamp", current_timestamp()) \
        .withColumn("_silver_date", current_date()) \
        .withColumn("_processed_by", lit("silver_pipeline_v1"))

df_silver_ready = add_silver_metadata(df_valid)

print("Silver metadata added:")
display(df_silver_ready.select("order_id", "_silver_timestamp", "_silver_date", "_processed_by", "quality_score").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Write to Silver Tables

# COMMAND ----------

# Write valid records to Silver
df_silver_ready.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("orders_silver")

print("✅ Written to orders_silver")

# Write quarantine records
df_quarantine_with_meta = add_silver_metadata(df_quarantine) \
    .withColumn("quarantine_reason", lit("quality_score_below_threshold"))

df_quarantine_with_meta.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("orders_silver_quarantine")

print("✅ Written to orders_silver_quarantine")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Validate Silver Quality

# COMMAND ----------

def validate_silver_quality(silver_table):
    """
    Comprehensive Silver layer validation
    """
    
    df = spark.table(silver_table)
    
    print(f"Validating Silver table: {silver_table}")
    print("="*80)
    
    engine = ValidationEngine()
    
    # Critical validations
    engine.add_rules([
        NotNullRule("order_id", "ERROR"),
        NotNullRule("customer_id", "ERROR"),
        NotNullRule("order_date", "ERROR"),
        NotNullRule("amount", "ERROR"),
        NotNullRule("status", "ERROR"),
        ValueRangeRule("amount", 0.01, 1000000, "ERROR"),
        ValueRangeRule("age", 0, 120, "WARNING"),
        RegexRule("email", r'^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$', "email_format", "WARNING")
    ])
    
    summary = engine.validate(df)
    
    # Print summary
    print(f"\nValidation Summary:")
    print(f"  Total Rules: {summary['total_rules']}")
    print(f"  Passed: {summary['passed_rules']}")
    print(f"  Failed: {summary['failed_rules']}")
    print(f"  Critical Failures: {summary['critical_failures']}")
    print(f"  Status: {'✅ PASSED' if summary['overall_passed'] else '❌ FAILED'}")
    
    return summary

silver_validation = validate_silver_quality("orders_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Calculate Silver Metrics

# COMMAND ----------

def calculate_silver_metrics(silver_table, quarantine_table=None):
    """
    Calculate comprehensive Silver metrics
    """
    
    df_silver = spark.table(silver_table)
    
    metrics = {
        'table': silver_table,
        'timestamp': datetime.now(),
        'total_records': df_silver.count(),
        'avg_quality_score': df_silver.agg(avg("quality_score")).collect()[0][0],
        'min_quality_score': df_silver.agg(min("quality_score")).collect()[0][0],
        'max_quality_score': df_silver.agg(max("quality_score")).collect()[0][0]
    }
    
    # Quality tier distribution
    tier_dist = df_silver.groupBy("quality_tier").count().collect()
    for row in tier_dist:
        metrics[f"records_{row.quality_tier.lower()}"] = row['count']
    
    # Quarantine metrics
    if quarantine_table:
        try:
            df_quarantine = spark.table(quarantine_table)
            metrics['quarantine_records'] = df_quarantine.count()
            metrics['quarantine_rate'] = metrics['quarantine_records'] / (metrics['total_records'] + metrics['quarantine_records'])
        except:
            metrics['quarantine_records'] = 0
            metrics['quarantine_rate'] = 0.0
    
    return metrics

silver_metrics = calculate_silver_metrics("orders_silver", "orders_silver_quarantine")

print("\nSILVER LAYER METRICS")
print("="*80)
for key, value in silver_metrics.items():
    if isinstance(value, float):
        print(f"{key}: {value:.2f}")
    else:
        print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: Complete Silver Pipeline

# COMMAND ----------

def silver_quality_pipeline(bronze_table, silver_table, quarantine_table, quality_threshold=70):
    """
    Complete Silver quality pipeline
    """
    
    print("="*80)
    print("SILVER QUALITY PIPELINE")
    print("="*80)
    print()
    
    # Step 1: Read Bronze
    print("STEP 1: Reading Bronze")
    df_bronze = spark.table(bronze_table)
    print(f"  Bronze records: {df_bronze.count()}")
    print()
    
    # Step 2: Cleanse
    print("STEP 2: Data Cleansing")
    df_cleansed = cleanse_silver_data(df_bronze)
    print("  ✅ Cleansing complete")
    print()
    
    # Step 3: Deduplicate
    print("STEP 3: Deduplication")
    df_cleansed = df_cleansed.withColumn("_ingestion_timestamp", current_timestamp())
    df_deduped = deduplicate_records(df_cleansed, ["order_id"])
    print()
    
    # Step 4: Quality Scoring
    print("STEP 4: Quality Scoring")
    df_scored = add_quality_score(df_deduped)
    avg_score = df_scored.agg(avg("quality_score")).collect()[0][0]
    print(f"  Average quality score: {avg_score:.2f}")
    print()
    
    # Step 5: Split and Write
    print("STEP 5: Split and Write")
    df_valid, df_quarantine = split_valid_and_quarantine(df_scored, quality_threshold)
    
    df_silver_ready = add_silver_metadata(df_valid)
    df_silver_ready.write.format("delta").mode("overwrite").saveAsTable(silver_table)
    print(f"  ✅ Written {df_valid.count()} records to {silver_table}")
    
    df_quarantine_ready = add_silver_metadata(df_quarantine) \
        .withColumn("quarantine_reason", lit("quality_score_below_threshold"))
    df_quarantine_ready.write.format("delta").mode("overwrite").saveAsTable(quarantine_table)
    print(f"  ✅ Quarantined {df_quarantine.count()} records to {quarantine_table}")
    print()
    
    # Step 6: Validate
    print("STEP 6: Validation")
    validation_summary = validate_silver_quality(silver_table)
    print()
    
    # Step 7: Metrics
    print("STEP 7: Calculate Metrics")
    metrics = calculate_silver_metrics(silver_table, quarantine_table)
    print(f"  Quality Score: {metrics['avg_quality_score']:.2f}")
    print(f"  Quarantine Rate: {metrics.get('quarantine_rate', 0)*100:.2f}%")
    print()
    
    print("="*80)
    print(f"PIPELINE COMPLETE: {'✅ SUCCESS' if validation_summary['overall_passed'] else '❌ FAILED'}")
    print("="*80)
    
    return {
        'validation': validation_summary,
        'metrics': metrics
    }

# Run pipeline
result = silver_quality_pipeline(
    bronze_table="orders_bronze_for_silver",
    silver_table="orders_silver_final",
    quarantine_table="orders_silver_quarantine_final"
)

# COMMAND ----------

# View final Silver table
display(spark.table("orders_silver_final"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Silver Layer Quality Principles
# MAGIC 1. **Apply Business Rules** - Validate against business requirements
# MAGIC 2. **Cleanse and Standardize** - Normalize formats
# MAGIC 3. **Quality Scoring** - Tag each record with quality score
# MAGIC 4. **Quarantine, Don't Delete** - Preserve bad data for analysis
# MAGIC 5. **Progressive Validation** - Critical → Important → Nice-to-have
# MAGIC
# MAGIC ### What You Built
# MAGIC - Data cleansing functions
# MAGIC - Deduplication logic
# MAGIC - Quality scoring system
# MAGIC - Quarantine pattern
# MAGIC - Complete Silver pipeline
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 5**: Gold layer aggregation quality
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 60 minutes | **Level**: 300 | **Type**: Lab
