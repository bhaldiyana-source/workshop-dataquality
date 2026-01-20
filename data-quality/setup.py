# Databricks notebook source
"""
Data Quality Workshop Setup
============================

Setup and dependency installation for Data Quality workshop.
Run this notebook first before starting the labs.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Workshop Setup
# MAGIC
# MAGIC This notebook installs all required dependencies and sets up the environment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Dependencies

# COMMAND ----------

print("Installing required packages...")
print("="*80)

# Install Great Expectations
print("\n1. Installing Great Expectations...")
%pip install great-expectations==0.18.8 --quiet

# Install other useful packages
print("\n2. Installing additional packages...")
%pip install matplotlib seaborn --quiet

print("\n✅ All packages installed successfully")
print("="*80)

# COMMAND ----------

# Restart Python to load new packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verify Installation

# COMMAND ----------

print("Verifying installations...")
print("="*80)

try:
    import great_expectations as gx
    print(f"✅ Great Expectations: {gx.__version__}")
except ImportError as e:
    print(f"❌ Great Expectations: Not installed - {e}")

try:
    from pyspark.sql.functions import *
    print(f"✅ PySpark: Available")
except ImportError as e:
    print(f"❌ PySpark: Not available - {e}")

try:
    import matplotlib
    print(f"✅ Matplotlib: {matplotlib.__version__}")
except ImportError as e:
    print(f"❌ Matplotlib: Not installed - {e}")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Database Schema

# COMMAND ----------

# Set default catalog and schema
catalog = "main"
schema = "default"

print(f"Setting up catalog and schema...")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")

# Create catalog if not exists
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
print(f"✅ Catalog '{catalog}' ready")

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
print(f"✅ Schema '{schema}' ready")

# Set as default
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"\n✅ Database setup complete")
print(f"   Current catalog: {spark.sql('SELECT current_catalog()').collect()[0][0]}")
print(f"   Current schema: {spark.sql('SELECT current_schema()').collect()[0][0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create Quality Tracking Tables

# COMMAND ----------

print("Creating quality tracking tables...")
print("="*80)

# Quality metrics table
spark.sql("""
    CREATE TABLE IF NOT EXISTS quality_metrics (
        metric_id STRING,
        table_name STRING,
        layer STRING,
        metric_timestamp TIMESTAMP,
        metric_date DATE,
        total_records BIGINT,
        valid_records BIGINT,
        invalid_records BIGINT,
        quality_score DOUBLE,
        completeness_rate DOUBLE,
        accuracy_rate DOUBLE,
        uniqueness_rate DOUBLE,
        timeliness_score DOUBLE,
        critical_failures INT,
        warning_failures INT,
        info_failures INT,
        validation_details STRING,
        pipeline_run_id STRING
    ) USING DELTA
    PARTITIONED BY (metric_date, layer)
""")
print("✅ quality_metrics table created")

# Validation results table
spark.sql("""
    CREATE TABLE IF NOT EXISTS validation_results (
        validation_timestamp TIMESTAMP,
        source_table STRING,
        rule_name STRING,
        severity STRING,
        passed BOOLEAN,
        failed_count BIGINT,
        total_count BIGINT,
        failure_rate DOUBLE,
        details STRING
    ) USING DELTA
""")
print("✅ validation_results table created")

# Data profiles table
spark.sql("""
    CREATE TABLE IF NOT EXISTS data_profiles (
        profile_timestamp TIMESTAMP,
        column_name STRING,
        profile_data STRING
    ) USING DELTA
""")
print("✅ data_profiles table created")

# Bronze ingestion metrics
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
print("✅ bronze_ingestion_metrics table created")

# Quarantine metrics
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
print("✅ quarantine_metrics table created")

# Anomaly detection results
spark.sql("""
    CREATE TABLE IF NOT EXISTS anomaly_detection_results (
        detection_timestamp TIMESTAMP,
        metric_date DATE,
        table_name STRING,
        metric_name STRING,
        metric_value DOUBLE,
        anomaly_score DOUBLE,
        anomaly_level STRING,
        detection_method STRING
    ) USING DELTA
""")
print("✅ anomaly_detection_results table created")

# Great Expectations validation results
spark.sql("""
    CREATE TABLE IF NOT EXISTS gx_validation_results (
        validation_timestamp TIMESTAMP,
        table_name STRING,
        overall_success BOOLEAN,
        total_expectations INT,
        successful_expectations INT,
        failed_expectations INT,
        success_percent DOUBLE,
        failed_details STRING
    ) USING DELTA
""")
print("✅ gx_validation_results table created")

print("="*80)
print("✅ All quality tracking tables created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Display Configuration Summary

# COMMAND ----------

print("\n" + "="*80)
print("DATA QUALITY WORKSHOP SETUP COMPLETE")
print("="*80)
print()
print("Configuration:")
print(f"  Catalog: {catalog}")
print(f"  Schema: {schema}")
print()
print("Installed Packages:")
print("  ✅ Great Expectations 0.18.8")
print("  ✅ PySpark (built-in)")
print("  ✅ Matplotlib")
print("  ✅ Seaborn")
print()
print("Quality Tracking Tables:")
print("  ✅ quality_metrics")
print("  ✅ validation_results")
print("  ✅ data_profiles")
print("  ✅ bronze_ingestion_metrics")
print("  ✅ quarantine_metrics")
print("  ✅ anomaly_detection_results")
print("  ✅ gx_validation_results")
print()
print("="*80)
print()
print("🎉 Ready to start the workshops!")
print()
print("Next Steps:")
print("  1. Review README.md for workshop overview")
print("  2. Start with 00_Lecture_Data_Quality_Fundamentals.py")
print("  3. Progress through lectures and labs in order")
print("  4. Complete the Production Pipeline demo")
print()
print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Troubleshooting

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Issues
# MAGIC
# MAGIC **Issue 1: Package Installation Fails**
# MAGIC - Solution: Ensure cluster has internet access
# MAGIC - Alternative: Pre-install packages in cluster init script
# MAGIC
# MAGIC **Issue 2: Catalog/Schema Permissions**
# MAGIC - Solution: Ensure you have CREATE privileges on catalog and schema
# MAGIC - Alternative: Ask admin to create catalog/schema for you
# MAGIC
# MAGIC **Issue 3: Table Already Exists**
# MAGIC - Solution: Tables are created with IF NOT EXISTS - safe to re-run
# MAGIC - Alternative: Drop and recreate if needed: `DROP TABLE IF EXISTS table_name`
# MAGIC
# MAGIC **Issue 4: Python Restart Required**
# MAGIC - Solution: Notebook automatically restarts Python after package install
# MAGIC - Alternative: Manually restart kernel if needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Cleanup (Optional)

# COMMAND ----------

def cleanup_workshop_tables():
    """
    Drop all workshop tables (use with caution!)
    """
    
    tables = [
        "quality_metrics",
        "validation_results",
        "data_profiles",
        "bronze_ingestion_metrics",
        "quarantine_metrics",
        "anomaly_detection_results",
        "gx_validation_results"
    ]
    
    print("⚠️  WARNING: This will drop all quality tracking tables!")
    print("="*80)
    
    for table in tables:
        try:
            spark.sql(f"DROP TABLE IF EXISTS {table}")
            print(f"  Dropped: {table}")
        except Exception as e:
            print(f"  Error dropping {table}: {e}")
    
    print("="*80)
    print("✅ Cleanup complete")

# Uncomment to run cleanup
# cleanup_workshop_tables()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workshop Resources
# MAGIC
# MAGIC ### Documentation
# MAGIC - [Delta Lake Documentation](https://docs.delta.io/)
# MAGIC - [Great Expectations Documentation](https://docs.greatexpectations.io/)
# MAGIC - [Databricks Data Quality](https://docs.databricks.com/data-governance/)
# MAGIC
# MAGIC ### Additional Reading
# MAGIC - Data Quality Dimensions
# MAGIC - Medallion Architecture Best Practices
# MAGIC - Delta Lake Constraints and Expectations
# MAGIC - Production Pipeline Patterns
# MAGIC
# MAGIC ---
# MAGIC **Setup Complete** | Ready to start learning! 🚀
