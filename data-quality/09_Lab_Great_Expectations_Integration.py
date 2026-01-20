# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Great Expectations Integration
# MAGIC
# MAGIC **Module 9: Enterprise Data Quality Testing**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 50 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC 1. **Setup Great Expectations** - Install and configure for Databricks
# MAGIC 2. **Create Expectations** - Define data quality expectations
# MAGIC 3. **Run Validations** - Execute expectation suites
# MAGIC 4. **View Results** - Analyze validation outputs
# MAGIC 5. **Integrate with Pipelines** - Automate quality testing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Install Great Expectations
%pip install great-expectations==0.18.8 --quiet

# Restart Python to load new packages
dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import great_expectations as gx
from great_expectations.dataset import SparkDFDataset
import json

catalog = "main"
schema = "default"
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"✅ Great Expectations version: {gx.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data

# COMMAND ----------

test_data = [
    (1, "john@example.com", 25, 150.50, "2024-01-15", "completed"),
    (2, "jane@test.com", 32, 275.00, "2024-01-16", "completed"),
    (3, "invalid-email", 45, 89.99, "2024-01-17", "pending"),
    (None, "bob@test.com", 28, 450.00, "2024-01-18", "completed"),
    (5, None, 35, 125.75, "2024-01-19", "completed"),
    (6, "alice@example.com", 150, 200.00, "2024-01-20", "cancelled"),
    (7, "charlie@test.com", 29, -50.00, "2024-01-21", "pending"),
]

test_schema = StructType([
    StructField("order_id", IntegerType()),
    StructField("email", StringType()),
    StructField("age", IntegerType()),
    StructField("amount", DoubleType()),
    StructField("order_date", StringType()),
    StructField("status", StringType())
])

df_test = spark.createDataFrame(test_data, test_schema)
df_test.write.format("delta").mode("overwrite").saveAsTable("orders_gx_test")

print("✅ Test data created")
display(df_test)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Basic Great Expectations Validation

# COMMAND ----------

# Convert DataFrame to GX dataset
df = spark.table("orders_gx_test")
gx_df = SparkDFDataset(df)

print("Great Expectations Dataset created")
print(f"Row count: {gx_df.count()}")
print(f"Columns: {gx_df.columns}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Define Expectations

# COMMAND ----------

# Add expectations to dataset
print("Adding expectations...")
print("="*80)

# Table-level expectations
exp1 = gx_df.expect_table_row_count_to_be_between(min_value=1, max_value=1000)
print(f"1. Row count check: {'✅' if exp1.success else '❌'}")

exp2 = gx_df.expect_table_column_count_to_equal(value=6)
print(f"2. Column count check: {'✅' if exp2.success else '❌'}")

# Column existence
exp3 = gx_df.expect_column_to_exist("order_id")
print(f"3. Column 'order_id' exists: {'✅' if exp3.success else '❌'}")

# NOT NULL expectations
exp4 = gx_df.expect_column_values_to_not_be_null("order_id")
print(f"4. order_id not null: {'✅' if exp4.success else '❌'} ({exp4.result.get('unexpected_count', 0)} nulls)")

exp5 = gx_df.expect_column_values_to_not_be_null("email")
print(f"5. email not null: {'✅' if exp5.success else '❌'} ({exp5.result.get('unexpected_count', 0)} nulls)")

# Uniqueness
exp6 = gx_df.expect_column_values_to_be_unique("order_id")
print(f"6. order_id unique: {'✅' if exp6.success else '❌'}")

# Value ranges
exp7 = gx_df.expect_column_values_to_be_between("amount", min_value=0, max_value=10000)
print(f"7. amount range (0-10000): {'✅' if exp7.success else '❌'} ({exp7.result.get('unexpected_count', 0)} out of range)")

exp8 = gx_df.expect_column_values_to_be_between("age", min_value=0, max_value=120)
print(f"8. age range (0-120): {'✅' if exp8.success else '❌'} ({exp8.result.get('unexpected_count', 0)} out of range)")

# Set membership
exp9 = gx_df.expect_column_values_to_be_in_set("status", ["pending", "completed", "cancelled"])
print(f"9. status in valid set: {'✅' if exp9.success else '❌'}")

# Regex matching
exp10 = gx_df.expect_column_values_to_match_regex("email", r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
print(f"10. email format: {'✅' if exp10.success else '❌'} ({exp10.result.get('unexpected_count', 0)} invalid)")

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Create Reusable Expectation Suite

# COMMAND ----------

def create_orders_expectation_suite(df):
    """
    Create comprehensive expectation suite for orders data
    """
    
    gx_df = SparkDFDataset(df)
    
    # Table-level
    gx_df.expect_table_row_count_to_be_between(min_value=1)
    gx_df.expect_table_column_count_to_equal(value=6)
    
    # Column existence
    for col in ["order_id", "email", "age", "amount", "order_date", "status"]:
        gx_df.expect_column_to_exist(col)
    
    # NOT NULL constraints
    gx_df.expect_column_values_to_not_be_null("order_id")
    gx_df.expect_column_values_to_not_be_null("amount")
    gx_df.expect_column_values_to_not_be_null("status")
    
    # Uniqueness
    gx_df.expect_column_values_to_be_unique("order_id")
    
    # Value ranges
    gx_df.expect_column_values_to_be_between("amount", min_value=0.01, max_value=1000000)
    gx_df.expect_column_values_to_be_between("age", min_value=0, max_value=120)
    
    # Set membership
    gx_df.expect_column_values_to_be_in_set("status", ["pending", "completed", "cancelled"])
    
    # Regex patterns
    gx_df.expect_column_values_to_match_regex(
        "email", 
        r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    )
    
    # Data types
    gx_df.expect_column_values_to_be_of_type("order_id", "IntegerType")
    gx_df.expect_column_values_to_be_of_type("amount", "DoubleType")
    
    return gx_df

print("✅ Expectation suite function created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Validate with Full Suite

# COMMAND ----------

# Apply full expectation suite
df_to_validate = spark.table("orders_gx_test")
gx_df_validated = create_orders_expectation_suite(df_to_validate)

# Run validation
results = gx_df_validated.validate()

print("VALIDATION RESULTS")
print("="*80)
print(f"Success: {results.success}")
print(f"Total Expectations: {results.statistics['evaluated_expectations']}")
print(f"Successful: {results.statistics['successful_expectations']}")
print(f"Failed: {results.statistics['unsuccessful_expectations']}")
print(f"Success Rate: {results.statistics['success_percent']:.2f}%")
print()

# Show failed expectations
if not results.success:
    print("FAILED EXPECTATIONS:")
    print("-"*80)
    for result in results.results:
        if not result.success:
            exp_type = result.expectation_config.expectation_type
            kwargs = result.expectation_config.kwargs
            unexpected_count = result.result.get('unexpected_count', 0)
            print(f"❌ {exp_type}")
            print(f"   Column: {kwargs.get('column', 'N/A')}")
            print(f"   Unexpected count: {unexpected_count}")
            print()

print("="*80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Extract Detailed Results

# COMMAND ----------

def extract_validation_metrics(validation_results):
    """
    Extract key metrics from GX validation results
    """
    
    metrics = {
        'overall_success': validation_results.success,
        'total_expectations': validation_results.statistics['evaluated_expectations'],
        'successful_expectations': validation_results.statistics['successful_expectations'],
        'failed_expectations': validation_results.statistics['unsuccessful_expectations'],
        'success_percent': validation_results.statistics['success_percent'],
        'failed_details': []
    }
    
    # Extract failed expectation details
    for result in validation_results.results:
        if not result.success:
            detail = {
                'expectation_type': result.expectation_config.expectation_type,
                'column': result.expectation_config.kwargs.get('column'),
                'unexpected_count': result.result.get('unexpected_count', 0),
                'unexpected_percent': result.result.get('unexpected_percent', 0)
            }
            metrics['failed_details'].append(detail)
    
    return metrics

metrics = extract_validation_metrics(results)

print("\nVALIDATION METRICS")
print("="*80)
for key, value in metrics.items():
    if key != 'failed_details':
        print(f"{key}: {value}")

if metrics['failed_details']:
    print("\nFailed Expectations Details:")
    for detail in metrics['failed_details']:
        print(f"  - {detail['expectation_type']} on {detail['column']}: {detail['unexpected_count']} failures")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Save Validation Results

# COMMAND ----------

# Create results table
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

def save_gx_results(table_name, validation_results):
    """
    Save GX validation results to Delta table
    """
    
    metrics = extract_validation_metrics(validation_results)
    
    record = {
        'validation_timestamp': datetime.now(),
        'table_name': table_name,
        'overall_success': metrics['overall_success'],
        'total_expectations': metrics['total_expectations'],
        'successful_expectations': metrics['successful_expectations'],
        'failed_expectations': metrics['failed_expectations'],
        'success_percent': metrics['success_percent'],
        'failed_details': json.dumps(metrics['failed_details'])
    }
    
    df_result = spark.createDataFrame([record])
    df_result.write.format("delta").mode("append").saveAsTable("gx_validation_results")
    
    print(f"✅ Saved validation results for {table_name}")

from datetime import datetime

save_gx_results("orders_gx_test", results)

display(spark.table("gx_validation_results"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: Integration with Data Pipeline

# COMMAND ----------

def validate_with_great_expectations(table_name, expectation_suite_func):
    """
    Pipeline integration function for GX validation
    
    Args:
        table_name: Table to validate
        expectation_suite_func: Function that creates GX expectations
    
    Returns:
        Boolean indicating if validation passed
    """
    
    print(f"Validating {table_name} with Great Expectations...")
    print("="*80)
    
    # Read data
    df = spark.table(table_name)
    
    # Apply expectations
    gx_df = expectation_suite_func(df)
    
    # Validate
    results = gx_df.validate()
    
    # Print summary
    print(f"Success: {results.success}")
    print(f"Pass Rate: {results.statistics['success_percent']:.2f}%")
    print(f"Failed Expectations: {results.statistics['unsuccessful_expectations']}")
    
    # Save results
    save_gx_results(table_name, results)
    
    # Return success status
    print("="*80)
    
    return results.success, results

# Test pipeline integration
success, validation_results = validate_with_great_expectations(
    "orders_gx_test",
    create_orders_expectation_suite
)

if success:
    print("✅ Data quality validation PASSED - proceeding with pipeline")
else:
    print("❌ Data quality validation FAILED - review issues before proceeding")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Great Expectations Benefits
# MAGIC 1. **Declarative** - Define expectations, not validation code
# MAGIC 2. **Comprehensive** - Rich library of built-in expectations
# MAGIC 3. **Standardized** - Industry-standard quality framework
# MAGIC 4. **Detailed Results** - Clear reporting of failures
# MAGIC 5. **Pipeline Integration** - Easy to integrate with workflows
# MAGIC
# MAGIC ### What You Learned
# MAGIC - Install and setup GX on Databricks
# MAGIC - Define expectations for data quality
# MAGIC - Run validations and interpret results
# MAGIC - Save results for tracking
# MAGIC - Integrate with data pipelines
# MAGIC
# MAGIC ### Common Expectations
# MAGIC - `expect_table_row_count_to_be_between` - Row count validation
# MAGIC - `expect_column_values_to_not_be_null` - Completeness
# MAGIC - `expect_column_values_to_be_unique` - Uniqueness
# MAGIC - `expect_column_values_to_be_between` - Range validation
# MAGIC - `expect_column_values_to_be_in_set` - Valid values
# MAGIC - `expect_column_values_to_match_regex` - Format validation
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Lab 7**: Quarantine and remediation workflows
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 50 minutes | **Level**: 300 | **Type**: Lab
