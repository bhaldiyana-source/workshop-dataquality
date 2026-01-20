# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Bronze Layer Quality
# MAGIC
# MAGIC **Module 6: Raw Data Ingestion Quality Checks**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 40 minutes    |
# MAGIC | Level           | 200/300       |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC In this hands-on lab, you will:
# MAGIC
# MAGIC 1. **Implement Bronze layer ingestion** - Capture raw data with minimal transformation
# MAGIC 2. **Add metadata tracking** - Source, timestamp, batch ID
# MAGIC 3. **Schema validation** - Ensure data can be parsed
# MAGIC 4. **Completeness checks** - Verify critical fields present
# MAGIC 5. **Audit trail** - Track all ingestion events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import json
import uuid

# Reuse validation framework from Lab 2
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any

# Setup catalog and schema
catalog = "main"
schema = "default"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"✅ Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Validation Framework
# MAGIC
# MAGIC Copy the validation classes from Lab 2

# COMMAND ----------

# ValidationResult class
@dataclass
class ValidationResult:
    """Standard validation result"""
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

# ValidationRule base class
class ValidationRule(ABC):
    """Base class for validation rules"""
    
    def __init__(self, rule_name: str, severity: str = 'ERROR'):
        self.rule_name = rule_name
        self.severity = severity
    
    @abstractmethod
    def validate(self, df) -> ValidationResult:
        pass

# NotNullRule
class NotNullRule(ValidationRule):
    """Validate that column has no NULL values"""
    
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

# ValidationEngine
class ValidationEngine:
    """Execute and manage validation rules"""
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.results: List[ValidationResult] = []
    
    def add_rule(self, rule: ValidationRule):
        self.rules.append(rule)
        return self
    
    def add_rules(self, rules: List[ValidationRule]):
        self.rules.extend(rules)
        return self
    
    def validate(self, df) -> Dict[str, Any]:
        self.results = []
        
        for rule in self.rules:
            try:
                result = rule.validate(df)
                self.results.append(result)
            except Exception as e:
                self.results.append(ValidationResult(
                    rule_name=rule.rule_name,
                    passed=False,
                    failed_count=df.count(),
                    total_count=df.count(),
                    failure_rate=1.0,
                    details={'error': str(e)},
                    severity=rule.severity
                ))
        
        return self.get_summary()
    
    def get_summary(self) -> Dict[str, Any]:
        total_rules = len(self.results)
        passed_rules = sum(1 for r in self.results if r.passed)
        critical_failures = [r for r in self.results if not r.passed and r.severity == 'ERROR']
        
        return {
            'total_rules': total_rules,
            'passed_rules': passed_rules,
            'failed_rules': total_rules - passed_rules,
            'pass_rate': (passed_rules / total_rules * 100) if total_rules > 0 else 0,
            'critical_failures': len(critical_failures),
            'overall_passed': len(critical_failures) == 0,
            'results': self.results
        }
    
    def generate_report(self) -> str:
        summary = self.get_summary()
        
        report = []
        report.append("=" * 80)
        report.append("DATA QUALITY VALIDATION REPORT")
        report.append("=" * 80)
        report.append(f"Total Rules: {summary['total_rules']}")
        report.append(f"Passed: {summary['passed_rules']}")
        report.append(f"Failed: {summary['failed_rules']}")
        report.append(f"Critical Failures: {summary['critical_failures']}")
        report.append(f"Status: {'✅ PASSED' if summary['overall_passed'] else '❌ FAILED'}")
        report.append("")
        
        for result in self.results:
            status = "✅" if result.passed else "❌"
            report.append(f"{status} [{result.severity}] {result.rule_name}")
            if not result.passed:
                report.append(f"   Failed: {result.failed_count}/{result.total_count}")
        
        report.append("=" * 80)
        return "\n".join(report)

print("✅ Validation framework imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Simulated Source Data
# MAGIC
# MAGIC Simulate raw data files from various sources

# COMMAND ----------

# Create directory for source files (simulated)
import tempfile
import os

# Create temp directory for simulated data files
source_data_path = "/tmp/bronze_source_data"
dbutils.fs.mkdirs(f"file:{source_data_path}")

# Simulate API data (JSON)
api_data = [
    {"order_id": 1001, "customer_id": 100, "order_date": "2024-01-15", "amount": 150.50, "status": "completed"},
    {"order_id": 1002, "customer_id": 200, "order_date": "2024-01-16", "amount": 275.00, "status": "completed"},
    {"order_id": 1003, "customer_id": 300, "order_date": "2024-01-17", "amount": 89.99, "status": "pending"},
    {"order_id": 1004, "customer_id": None, "order_date": "2024-01-18", "amount": 450.00, "status": "completed"},  # Missing customer
    {"order_id": None, "customer_id": 500, "order_date": "2024-01-19", "amount": 125.75, "status": "completed"},  # Missing order_id
]

# Write to JSON
with open(f"{source_data_path}/api_orders_batch1.json", "w") as f:
    for record in api_data:
        f.write(json.dumps(record) + "\n")

# Simulate CSV data
csv_data = """order_id,customer_id,order_date,amount,status
2001,600,2024-01-20,200.00,cancelled
2002,700,2024-01-21,175.50,completed
2003,800,2024-01-22,300.00,pending
2004,900,2024-01-23,225.00,completed
2005,1000,2024-01-24,180.00,completed"""

with open(f"{source_data_path}/csv_orders_batch1.csv", "w") as f:
    f.write(csv_data)

print(f"✅ Created source data files in {source_data_path}")
print(f"   - api_orders_batch1.json ({len(api_data)} records)")
print(f"   - csv_orders_batch1.csv (5 records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Define Bronze Layer Schema
# MAGIC
# MAGIC Define expected schema for Bronze layer

# COMMAND ----------

# Define expected schema for orders
bronze_orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True)
])

print("Expected Bronze Schema:")
bronze_orders_schema.printTreeString()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Bronze Ingestion Function
# MAGIC
# MAGIC Build function to ingest data to Bronze with metadata

# COMMAND ----------

def ingest_to_bronze(
    source_path: str,
    source_format: str,
    bronze_table: str,
    source_system: str,
    batch_id: str = None,
    schema: StructType = None
):
    """
    Ingest raw data to Bronze layer with metadata
    
    Args:
        source_path: Path to source data
        source_format: Format of source data (json, csv, parquet, etc.)
        bronze_table: Target Bronze table name
        source_system: Name of source system
        batch_id: Optional batch identifier
        schema: Optional schema for validation
    
    Returns:
        Dictionary with ingestion results
    """
    
    if batch_id is None:
        batch_id = str(uuid.uuid4())
    
    ingestion_timestamp = current_timestamp()
    
    try:
        # Read source data
        print(f"Reading data from {source_path}...")
        
        if schema:
            df_raw = spark.read.schema(schema).format(source_format).load(source_path)
        else:
            df_raw = spark.read.format(source_format).option("inferSchema", "true").load(source_path)
        
        # Add Bronze metadata
        df_bronze = df_raw \
            .withColumn("_source_system", lit(source_system)) \
            .withColumn("_source_file", input_file_name()) \
            .withColumn("_batch_id", lit(batch_id)) \
            .withColumn("_ingestion_timestamp", ingestion_timestamp) \
            .withColumn("_ingested_date", ingestion_timestamp.cast("date"))
        
        # Count records
        record_count = df_bronze.count()
        
        print(f"  Records read: {record_count}")
        
        # Basic validation
        if record_count == 0:
            raise Exception("No records read from source")
        
        # Write to Bronze (append-only pattern)
        print(f"Writing to Bronze table: {bronze_table}...")
        
        df_bronze.write \
            .format("delta") \
            .mode("append") \
            .partitionBy("_ingested_date") \
            .option("mergeSchema", "true") \
            .saveAsTable(bronze_table)
        
        print(f"✅ Bronze ingestion successful")
        
        return {
            'status': 'success',
            'records_ingested': record_count,
            'batch_id': batch_id,
            'table': bronze_table,
            'source_system': source_system,
            'ingestion_timestamp': str(datetime.now())
        }
        
    except Exception as e:
        print(f"❌ Bronze ingestion failed: {str(e)}")
        
        return {
            'status': 'failed',
            'error': str(e),
            'batch_id': batch_id,
            'table': bronze_table,
            'source_system': source_system,
            'ingestion_timestamp': str(datetime.now())
        }

print("✅ Bronze ingestion function created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Ingest JSON Data

# COMMAND ----------

# Ingest JSON data
result_json = ingest_to_bronze(
    source_path=f"file:{source_data_path}/api_orders_batch1.json",
    source_format="json",
    bronze_table="orders_bronze",
    source_system="api_server",
    schema=bronze_orders_schema
)

print("\nIngestion Result:")
for key, value in result_json.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# View Bronze data
display(spark.table("orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Ingest CSV Data

# COMMAND ----------

# Ingest CSV data
result_csv = ingest_to_bronze(
    source_path=f"file:{source_data_path}/csv_orders_batch1.csv",
    source_format="csv",
    bronze_table="orders_bronze",
    source_system="legacy_system",
    schema=bronze_orders_schema
)

print("\nIngestion Result:")
for key, value in result_csv.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# View all Bronze data
print("Current Bronze table contents:")
display(spark.sql("SELECT * FROM orders_bronze ORDER BY _ingestion_timestamp"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Bronze Quality Validation
# MAGIC
# MAGIC Validate Bronze layer quality

# COMMAND ----------

def validate_bronze_quality(bronze_table: str):
    """
    Validate Bronze layer data quality
    
    Bronze quality focus:
    - Schema compatibility
    - Record count > 0
    - Critical metadata fields present
    - Audit trail completeness
    """
    
    df = spark.table(bronze_table)
    
    print(f"Validating Bronze table: {bronze_table}")
    print("="*80)
    
    # Initialize validation engine
    engine = ValidationEngine()
    
    # Bronze-specific validations (focused on technical/structural quality)
    
    # 1. Metadata completeness
    engine.add_rule(NotNullRule("_source_system", "ERROR"))
    engine.add_rule(NotNullRule("_batch_id", "ERROR"))
    engine.add_rule(NotNullRule("_ingestion_timestamp", "ERROR"))
    engine.add_rule(NotNullRule("_ingested_date", "ERROR"))
    
    # 2. Critical business fields not ALL null (at least some data present)
    # Note: We don't reject individual nulls in Bronze, just check that we have SOME data
    critical_fields = ["order_id", "customer_id", "order_date"]
    for field in critical_fields:
        # Check that at least ONE record has this field populated
        non_null_count = df.filter(col(field).isNotNull()).count()
        if non_null_count == 0:
            print(f"⚠️  WARNING: All records have NULL {field}")
    
    # Execute validation
    summary = engine.validate(df)
    
    # Generate report
    print("\n" + engine.generate_report())
    
    # Additional Bronze metrics
    print("\nBRONZE LAYER METRICS")
    print("-"*80)
    print(f"Total Records: {df.count()}")
    print(f"Distinct Batches: {df.select('_batch_id').distinct().count()}")
    print(f"Source Systems: {', '.join([row._source_system for row in df.select('_source_system').distinct().collect()])}")
    print(f"Date Range: {df.agg({'_ingested_date': 'min'}).collect()[0][0]} to {df.agg({'_ingested_date': 'max'}).collect()[0][0]}")
    
    return summary

# Run Bronze validation
bronze_summary = validate_bronze_quality("orders_bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Track Bronze Ingestion Metrics

# COMMAND ----------

# Create Bronze metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS bronze_ingestion_metrics (
        batch_id STRING,
        source_system STRING,
        table_name STRING,
        ingestion_timestamp TIMESTAMP,
        ingestion_date DATE,
        records_ingested BIGINT,
        status STRING,
        validation_passed BOOLEAN,
        error_message STRING
    ) USING DELTA
    PARTITIONED BY (ingestion_date)
""")

# COMMAND ----------

def log_bronze_ingestion(ingestion_result: Dict, validation_summary: Dict = None):
    """
    Log Bronze ingestion metrics
    """
    
    metric_record = {
        'batch_id': ingestion_result.get('batch_id'),
        'source_system': ingestion_result.get('source_system'),
        'table_name': ingestion_result.get('table'),
        'ingestion_timestamp': datetime.now(),
        'ingestion_date': datetime.now().date(),
        'records_ingested': ingestion_result.get('records_ingested', 0),
        'status': ingestion_result.get('status'),
        'validation_passed': validation_summary.get('overall_passed', False) if validation_summary else None,
        'error_message': ingestion_result.get('error')
    }
    
    df_metric = spark.createDataFrame([metric_record])
    df_metric.write.format("delta").mode("append").saveAsTable("bronze_ingestion_metrics")
    
    print(f"✅ Logged Bronze ingestion metrics")

# Log metrics
log_bronze_ingestion(result_json, bronze_summary)
log_bronze_ingestion(result_csv, bronze_summary)

# View metrics
display(spark.table("bronze_ingestion_metrics"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Handle Schema Evolution

# COMMAND ----------

# Simulate new data with additional column
evolved_data = [
    {"order_id": 3001, "customer_id": 1100, "order_date": "2024-01-25", "amount": 350.00, "status": "completed", "payment_method": "credit_card"},
    {"order_id": 3002, "customer_id": 1200, "order_date": "2024-01-26", "amount": 275.50, "status": "pending", "payment_method": "paypal"},
]

evolved_path = f"{source_data_path}/api_orders_evolved.json"
with open(evolved_path, "w") as f:
    for record in evolved_data:
        f.write(json.dumps(record) + "\n")

# Ingest with schema evolution
result_evolved = ingest_to_bronze(
    source_path=f"file:{evolved_path}",
    source_format="json",
    bronze_table="orders_bronze",
    source_system="api_server_v2",
    schema=None  # Allow schema inference for evolution
)

print("\nSchema Evolution Result:")
print(f"  Status: {result_evolved['status']}")
print(f"  Records: {result_evolved.get('records_ingested', 0)}")

# View evolved schema
print("\nEvolved Bronze Schema:")
spark.table("orders_bronze").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Bronze Data Lineage

# COMMAND ----------

# Query data lineage
print("BRONZE DATA LINEAGE")
print("="*80)

lineage_query = """
SELECT 
    _source_system,
    _batch_id,
    _ingested_date,
    COUNT(*) as record_count,
    MIN(_ingestion_timestamp) as first_ingested,
    MAX(_ingestion_timestamp) as last_ingested
FROM orders_bronze
GROUP BY _source_system, _batch_id, _ingested_date
ORDER BY _ingested_date DESC, _source_system
"""

display(spark.sql(lineage_query))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: Enable Change Data Feed

# COMMAND ----------

# Enable CDF for Bronze table (for tracking changes)
spark.sql("""
    ALTER TABLE orders_bronze
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("✅ Change Data Feed enabled for orders_bronze")

# View table properties
display(spark.sql("SHOW TBLPROPERTIES orders_bronze"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 10: Complete Bronze Pipeline Function

# COMMAND ----------

def bronze_quality_pipeline(source_path: str, source_format: str, bronze_table: str, source_system: str):
    """
    Complete Bronze pipeline with ingestion and validation
    """
    
    print("="*80)
    print("BRONZE QUALITY PIPELINE")
    print("="*80)
    print()
    
    # Step 1: Ingest
    print("STEP 1: Ingestion")
    print("-"*80)
    ingestion_result = ingest_to_bronze(
        source_path=source_path,
        source_format=source_format,
        bronze_table=bronze_table,
        source_system=source_system
    )
    
    if ingestion_result['status'] != 'success':
        print(f"❌ Pipeline failed at ingestion: {ingestion_result.get('error')}")
        return ingestion_result
    
    print()
    
    # Step 2: Validate
    print("STEP 2: Validation")
    print("-"*80)
    validation_summary = validate_bronze_quality(bronze_table)
    
    print()
    
    # Step 3: Log metrics
    print("STEP 3: Logging Metrics")
    print("-"*80)
    log_bronze_ingestion(ingestion_result, validation_summary)
    
    print()
    print("="*80)
    print(f"PIPELINE COMPLETE: {'✅ SUCCESS' if validation_summary['overall_passed'] else '⚠️  WITH WARNINGS'}")
    print("="*80)
    
    return {
        'ingestion': ingestion_result,
        'validation': validation_summary
    }

# Test complete pipeline
pipeline_result = bronze_quality_pipeline(
    source_path=f"file:{source_data_path}/api_orders_batch1.json",
    source_format="json",
    bronze_table="orders_bronze_test",
    source_system="api_server"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Bronze Layer Quality Principles
# MAGIC 1. **Preserve Everything** - Don't filter or reject data in Bronze
# MAGIC 2. **Add Metadata** - Track source, timestamp, batch ID
# MAGIC 3. **Minimal Validation** - Only check structural/technical issues
# MAGIC 4. **Enable Auditing** - Full lineage and change tracking
# MAGIC 5. **Schema Evolution** - Allow additive changes
# MAGIC
# MAGIC ### What You Built
# MAGIC - Bronze ingestion function with metadata
# MAGIC - Schema validation
# MAGIC - Quality validation focused on metadata
# MAGIC - Ingestion metrics tracking
# MAGIC - Data lineage queries
# MAGIC - Complete Bronze pipeline
# MAGIC
# MAGIC ### Bronze Quality Checks
# MAGIC ✅ **Do Check**:
# MAGIC - Metadata completeness
# MAGIC - Record count > 0
# MAGIC - Schema compatibility
# MAGIC - Audit trail fields
# MAGIC
# MAGIC ❌ **Don't Check**:
# MAGIC - Business rules
# MAGIC - Data formats
# MAGIC - Referential integrity
# MAGIC - Individual field nulls (unless ALL null)
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 4**: Apply business validation in Silver layer
# MAGIC - **Lab 5**: Validate aggregations in Gold layer
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 40 minutes | **Level**: 200/300 | **Type**: Lab
