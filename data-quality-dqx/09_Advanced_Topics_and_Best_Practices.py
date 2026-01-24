# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced Topics and Best Practices
# MAGIC
# MAGIC **Module 10: Enterprise Data Quality Patterns**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
# MAGIC | Level           | 400           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ### ⚠️ Implementation Note
# MAGIC
# MAGIC This notebook demonstrates **advanced data quality patterns** using PySpark-based implementations.
# MAGIC
# MAGIC **Key Adaptations:**
# MAGIC * Custom check classes implemented with PySpark
# MAGIC * Multi-table validation using DataFrame joins
# MAGIC * Performance optimization techniques with Spark
# MAGIC * All concepts remain the same as the conceptual DQX API
# MAGIC
# MAGIC **Why?** The `databricks-labs-dqx` package uses a different API structure than the simplified demo API shown here. This implementation demonstrates the same advanced patterns using working PySpark code.
# MAGIC
# MAGIC **For production use**, refer to the [databricks-labs-dqx documentation](https://databrickslabs.github.io/dqx/) for the actual API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Create custom checks** for specific business needs
# MAGIC 2. **Implement multi-table validation** with referential integrity
# MAGIC 3. **Optimize performance** of quality checks
# MAGIC 4. **Integrate with CI/CD** pipelines
# MAGIC 5. **Version quality rules** effectively

# COMMAND ----------

# DBTITLE 1,Cell 3
# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import unittest
from datetime import datetime, timedelta
import random
from typing import Dict, List, Tuple, Any

print("✅ Libraries imported successfully")
print("Using PySpark for advanced data quality patterns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Custom Check Development
# MAGIC
# MAGIC ### 1.1 Create Custom Check Class

# COMMAND ----------

# DBTITLE 1,Cell 5
class BusinessRuleCheck:
    """
    Custom check for complex business rules
    Example: Revenue must exceed cost by minimum margin
    """
    
    def __init__(self, revenue_col, cost_col, min_margin=0.2):
        self.revenue_col = revenue_col
        self.cost_col = cost_col
        self.min_margin = min_margin
        # Use underscore instead of dot in column name
        margin_str = str(int(min_margin * 100))
        self.name = f"business_margin_check_{margin_str}pct"
        self.result_column = f"check_{self.name}"
    
    def validate(self, df):
        """
        Validate that revenue exceeds cost by minimum margin
        Returns DataFrame with validation results
        """
        return df.withColumn(
            self.result_column,
            F.when(
                F.col(self.revenue_col) >= F.col(self.cost_col) * (1 + self.min_margin),
                F.lit("PASSED")
            ).otherwise(F.lit("FAILED"))
        )
    
    def get_description(self):
        return f"Revenue must exceed cost by at least {self.min_margin*100}%"

print("✅ Custom BusinessRuleCheck class created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Use Custom Check

# COMMAND ----------

# Create sample financial data
financial_data = [
    (1, "PROD001", 1000.00, 600.00, 400.00),   # Valid: 67% margin
    (2, "PROD002", 1500.00, 1200.00, 300.00),  # Valid: 25% margin
    (3, "PROD003", 2000.00, 1800.00, 200.00),  # Invalid: 11% margin
    (4, "PROD004", 500.00, 450.00, 50.00),     # Invalid: 11% margin
    (5, "PROD005", 3000.00, 2000.00, 1000.00), # Valid: 50% margin
]

financial_df = spark.createDataFrame(
    financial_data,
    ["id", "product_id", "revenue", "cost", "profit"]
)

print("Financial data:")
display(financial_df)

# COMMAND ----------

# DBTITLE 1,Cell 8
# Apply custom business rule check
custom_check = BusinessRuleCheck(
    revenue_col="revenue",
    cost_col="cost",
    min_margin=0.20  # 20% minimum margin
)

# Apply the custom check
result_df = custom_check.validate(financial_df)

# Add standard checks
result_df = result_df.withColumn(
    "check_revenue_positive",
    F.when(F.col("revenue") > 0, F.lit("PASSED")).otherwise(F.lit("FAILED"))
)

result_df = result_df.withColumn(
    "check_cost_positive",
    F.when(F.col("cost") > 0, F.lit("PASSED")).otherwise(F.lit("FAILED"))
)

# Calculate overall pass/fail
check_cols = [col for col in result_df.columns if col.startswith("check_")]

# Count failures per row
failure_expr = sum([F.when(F.col(c) == "FAILED", 1).otherwise(0) for c in check_cols])
result_df = result_df.withColumn("total_failures", failure_expr)
result_df = result_df.withColumn(
    "overall_status",
    F.when(F.col("total_failures") == 0, "PASSED").otherwise("FAILED")
)

# Calculate summary
total_records = result_df.count()
failed_records = result_df.filter(F.col("overall_status") == "FAILED").count()
pass_rate = (total_records - failed_records) / total_records if total_records > 0 else 0

print(f"\nValidation with Custom Check:")
print(f"  Total Records: {total_records}")
print(f"  Failed Records: {failed_records}")
print(f"  Pass Rate: {pass_rate:.2%}")
print(f"\nResults:")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Multi-Table Validation
# MAGIC
# MAGIC ### 2.1 Create Related Tables

# COMMAND ----------

# Create orders table
orders_data = [
    (1, "ORD001", "CUST001", "PROD001", 2, 1000.00),
    (2, "ORD002", "CUST002", "PROD002", 1, 500.00),
    (3, "ORD003", "CUST003", "PROD999", 1, 750.00),  # Invalid: product doesn't exist
    (4, "ORD004", "CUST999", "PROD001", 3, 1500.00), # Invalid: customer doesn't exist
    (5, "ORD005", "CUST001", "PROD003", 1, 250.00),
]

orders_df = spark.createDataFrame(
    orders_data,
    ["id", "order_id", "customer_id", "product_id", "quantity", "total_amount"]
)

# Create customers table
customers_data = [
    (1, "CUST001", "John Doe", "john@example.com"),
    (2, "CUST002", "Jane Smith", "jane@example.com"),
    (3, "CUST003", "Bob Johnson", "bob@example.com"),
]

customers_df = spark.createDataFrame(
    customers_data,
    ["id", "customer_id", "name", "email"]
)

# Create products table
products_data = [
    (1, "PROD001", "Product A", 100.00),
    (2, "PROD002", "Product B", 150.00),
    (3, "PROD003", "Product C", 75.00),
]

products_df = spark.createDataFrame(
    products_data,
    ["id", "product_id", "name", "price"]
)

print("Sample data created:")
print("\nOrders:")
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Validate Referential Integrity

# COMMAND ----------

# DBTITLE 1,Cell 12
# Validate referential integrity using PySpark joins

# Check 1: orders.customer_id -> customers.customer_id
orders_with_customer_check = orders_df.alias("orders").join(
    customers_df.alias("customers"),
    F.col("orders.customer_id") == F.col("customers.customer_id"),
    "left"
).withColumn(
    "check_customer_exists",
    F.when(F.col("customers.customer_id").isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).select(
    F.col("orders.*"),
    F.col("check_customer_exists")
)

# Check 2: orders.product_id -> products.product_id
orders_with_all_checks = orders_with_customer_check.alias("orders").join(
    products_df.alias("products"),
    F.col("orders.product_id") == F.col("products.product_id"),
    "left"
).withColumn(
    "check_product_exists",
    F.when(F.col("products.product_id").isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).select(
    F.col("orders.*"),
    F.col("check_customer_exists"),
    F.col("check_product_exists")
)

# Add overall status
orders_with_all_checks = orders_with_all_checks.withColumn(
    "referential_integrity_status",
    F.when(
        (F.col("check_customer_exists") == "PASSED") & (F.col("check_product_exists") == "PASSED"),
        F.lit("PASSED")
    ).otherwise(F.lit("FAILED"))
)

print("Referential Integrity Validation:")
print(f"Total Orders: {orders_with_all_checks.count()}")
print(f"Failed Orders: {orders_with_all_checks.filter(F.col('referential_integrity_status') == 'FAILED').count()}")
print("\nResults:")
display(orders_with_all_checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Cross-Table Consistency Checks

# COMMAND ----------

# DBTITLE 1,Cell 14
# Cross-table consistency check: order total should match quantity * price
consistency_check = orders_df.alias("orders").join(
    products_df.alias("products"),
    F.col("orders.product_id") == F.col("products.product_id"),
    "left"
).withColumn(
    "expected_total",
    F.col("orders.quantity") * F.col("products.price")
).withColumn(
    "check_total_matches",
    F.when(
        (F.col("products.price").isNotNull()) & 
        (F.abs(F.col("orders.total_amount") - F.col("expected_total")) < 0.01),
        F.lit("PASSED")
    ).when(
        F.col("products.price").isNull(),
        F.lit("SKIPPED")  # Product doesn't exist
    ).otherwise(F.lit("FAILED"))
).select(
    F.col("orders.*"),
    F.col("products.price").alias("unit_price"),
    F.col("expected_total"),
    F.col("check_total_matches")
)

print("Consistency Check Results:")
print(f"Total Orders: {consistency_check.count()}")
print(f"Failed Consistency: {consistency_check.filter(F.col('check_total_matches') == 'FAILED').count()}")
print("\nResults:")
display(consistency_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Performance Optimization
# MAGIC
# MAGIC ### 3.1 Partitioning Strategies

# COMMAND ----------

# DBTITLE 1,Cell 16
# Create large dataset
large_data = [
    (
        i,
        f"TXN{i:08d}",
        f"CUST{random.randint(1,1000):04d}",
        random.uniform(10, 10000),
        datetime.now() - timedelta(days=random.randint(0, 365)),
        random.choice(['pending', 'completed', 'cancelled'])
    )
    for i in range(10000)
]

large_df = spark.createDataFrame(
    large_data,
    ["id", "transaction_id", "customer_id", "amount", "date", "status"]
).repartition(10, "date")  # Partition by date

print(f"Large dataset created: {large_df.count()} records")
print(f"Dataset partitioned by date for optimal performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Caching Strategies

# COMMAND ----------

# DBTITLE 1,Cell 18
# Note: Caching is not supported on serverless compute
# On classic clusters, you would use: large_df.cache()

print("✅ Running validation on large dataset")
print("   (Caching skipped - not supported on serverless)")

# Apply quality checks
import time

start_time = time.time()

# Apply checks
validated_df = large_df.withColumn(
    "check_transaction_id_not_null",
    F.when(F.col("transaction_id").isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).withColumn(
    "check_amount_positive",
    F.when(F.col("amount") > 0, F.lit("PASSED")).otherwise(F.lit("FAILED"))
)

# Count results to trigger execution
total_records = validated_df.count()
failed_records = validated_df.filter(
    (F.col("check_transaction_id_not_null") == "FAILED") | 
    (F.col("check_amount_positive") == "FAILED")
).count()

end_time = time.time()

print(f"\nValidation Performance:")
print(f"  Records: {total_records:,}")
print(f"  Failed Records: {failed_records}")
print(f"  Time: {end_time - start_time:.2f} seconds")
print(f"  Throughput: {total_records / (end_time - start_time):,.0f} records/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Check Ordering

# COMMAND ----------

# DBTITLE 1,Cell 20
# Order checks from cheapest to most expensive
# This is a best practice for performance optimization

print("✅ Check Ordering Best Practice:")
print("\n1. Cheap checks first (column-level, no computation):")
print("   - NOT NULL checks")
print("   - Simple comparisons (>, <, =)")
print("\n2. Medium cost (simple expressions):")
print("   - Range checks")
print("   - IN/NOT IN checks")
print("\n3. Expensive checks last (complex operations):")
print("   - Regex pattern matching")
print("   - Complex expressions")
print("   - UDFs and custom functions")
print("\nExample ordering applied to large_df:")

# Apply checks in optimal order
optimized_df = large_df

# 1. Cheap: NOT NULL
optimized_df = optimized_df.withColumn(
    "check_id_not_null",
    F.when(F.col("id").isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
)

# 2. Medium: Simple comparison
optimized_df = optimized_df.withColumn(
    "check_amount_positive",
    F.when(F.col("amount") > 0, F.lit("PASSED")).otherwise(F.lit("FAILED"))
)

# 3. Expensive: Regex
optimized_df = optimized_df.withColumn(
    "check_transaction_id_format",
    F.when(
        F.col("transaction_id").rlike(r"^TXN\d{8}$"),
        F.lit("PASSED")
    ).otherwise(F.lit("FAILED"))
)

print("\n✅ Checks applied in optimal order for best performance")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Parallel Validation

# COMMAND ----------

# DBTITLE 1,Cell 22
# Validate multiple tables in parallel
from concurrent.futures import ThreadPoolExecutor

def validate_table(table_name, df):
    """Validate a single table"""
    # Apply standard checks
    validated = df.withColumn(
        "check_not_null",
        F.when(F.col(df.columns[0]).isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
    )
    
    total = validated.count()
    failed = validated.filter(F.col("check_not_null") == "FAILED").count()
    pass_rate = (total - failed) / total if total > 0 else 0
    
    return table_name, {"total": total, "failed": failed, "pass_rate": pass_rate}

# Create sample tables
tables_to_validate = {
    "transactions": large_df.limit(1000),
    "customers": customers_df,
    "products": products_df
}

# Parallel validation
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(validate_table, table_name, df)
        for table_name, df in tables_to_validate.items()
    ]
    
    results = [future.result() for future in futures]

print("\nParallel Validation Results:")
for table_name, summary in results:
    print(f"  {table_name}:")
    print(f"    Total: {summary['total']}")
    print(f"    Failed: {summary['failed']}")
    print(f"    Pass Rate: {summary['pass_rate']:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: CI/CD Integration
# MAGIC
# MAGIC ### 4.1 Unit Tests for Quality Rules

# COMMAND ----------

# DBTITLE 1,Cell 24
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

class TestDataQualityRules(unittest.TestCase):
    """Unit tests for data quality rules"""
    
    def setUp(self):
        """Set up test data"""
        self.sample_df = spark.createDataFrame(
            [
                (1, "CUST001", 100.00, "active"),
                (2, "CUST002", 200.00, "active"),
            ],
            ["id", "customer_id", "amount", "status"]
        )
    
    def test_customer_id_not_null(self):
        """Test customer ID NOT NULL check"""
        result_df = self.sample_df.withColumn(
            "check_customer_id_not_null",
            F.when(F.col("customer_id").isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
        )
        failed_count = result_df.filter(F.col("check_customer_id_not_null") == "FAILED").count()
        self.assertEqual(failed_count, 0)
    
    def test_amount_positive(self):
        """Test amount is positive"""
        result_df = self.sample_df.withColumn(
            "check_amount_positive",
            F.when(F.col("amount") > 0, F.lit("PASSED")).otherwise(F.lit("FAILED"))
        )
        failed_count = result_df.filter(F.col("check_amount_positive") == "FAILED").count()
        self.assertEqual(failed_count, 0)
    
    def test_invalid_data_detection(self):
        """Test that invalid data is detected"""
        # Define schema explicitly to handle None values
        schema = StructType([
            StructField("id", LongType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("status", StringType(), True)
        ])
        
        invalid_df = spark.createDataFrame(
            [(1, None, -100.00, "active")],  # Null customer_id, negative amount
            schema=schema
        )
        
        result_df = invalid_df.withColumn(
            "check_customer_id_not_null",
            F.when(F.col("customer_id").isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
        )
        
        failed_count = result_df.filter(F.col("check_customer_id_not_null") == "FAILED").count()
        self.assertGreater(failed_count, 0)

# Run tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestDataQualityRules)
runner = unittest.TextTestRunner(verbosity=2)
test_results = runner.run(suite)

print(f"\n✅ Tests completed: {test_results.testsRun} tests, {len(test_results.failures)} failures, {len(test_results.errors)} errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 CI/CD Pipeline Script Example

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # ci_cd_quality_check.py
# MAGIC # Example CI/CD script for data quality validation
# MAGIC
# MAGIC import sys
# MAGIC from dqx import ConfigValidator
# MAGIC import yaml
# MAGIC
# MAGIC def run_quality_checks(config_path, data_path, environment):
# MAGIC     """
# MAGIC     Run quality checks in CI/CD pipeline
# MAGIC     Returns 0 for success, 1 for failure
# MAGIC     """
# MAGIC     try:
# MAGIC         # Load configuration
# MAGIC         with open(config_path, 'r') as f:
# MAGIC             config = yaml.safe_load(f)
# MAGIC         
# MAGIC         # Create validator
# MAGIC         validator = ConfigValidator.from_config(config)
# MAGIC         
# MAGIC         # Load data
# MAGIC         df = spark.read.parquet(data_path)
# MAGIC         
# MAGIC         # Run validation
# MAGIC         result_df, summary = validator.validate(df)
# MAGIC         
# MAGIC         # Check results
# MAGIC         pass_rate = summary.get('pass_rate', 0)
# MAGIC         threshold = 0.95 if environment == 'production' else 0.90
# MAGIC         
# MAGIC         if pass_rate >= threshold:
# MAGIC             print(f"✅ Quality check PASSED: {pass_rate:.2%}")
# MAGIC             return 0
# MAGIC         else:
# MAGIC             print(f"❌ Quality check FAILED: {pass_rate:.2%} (threshold: {threshold:.2%})")
# MAGIC             return 1
# MAGIC     
# MAGIC     except Exception as e:
# MAGIC         print(f"❌ Error running quality checks: {str(e)}")
# MAGIC         return 1
# MAGIC
# MAGIC if __name__ == "__main__":
# MAGIC     config_path = sys.argv[1]
# MAGIC     data_path = sys.argv[2]
# MAGIC     environment = sys.argv[3]
# MAGIC     
# MAGIC     sys.exit(run_quality_checks(config_path, data_path, environment))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Quality Rule Versioning
# MAGIC
# MAGIC ### 5.1 Version Management

# COMMAND ----------

# DBTITLE 1,Cell 28
# Quality Rule Versioning Concept
# In production, you would store rule definitions in a version control system
# or a metadata table in Unity Catalog

print("✅ Quality Rule Versioning Best Practices:")
print("\n1. Store rule definitions in version control (Git)")
print("2. Use semantic versioning (v1.0.0, v2.0.0, etc.)")
print("3. Track changes with commit messages")
print("4. Maintain a changelog for rule updates")
print("5. Use Unity Catalog for metadata storage")
print("\nExample: Store rules as JSON/YAML in Git repository")
print("  - rules/v1.0.0/transaction_rules.yaml")
print("  - rules/v2.0.0/transaction_rules.yaml")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Save Rule Version

# COMMAND ----------

# DBTITLE 1,Cell 30
# Example: Save rule definitions to Unity Catalog table
# Create schema and table to store rule versions

spark.sql("CREATE SCHEMA IF NOT EXISTS quality_rules")

spark.sql("""
CREATE TABLE IF NOT EXISTS quality_rules.rule_versions (
  version STRING,
  rule_name STRING,
  rule_type STRING,
  rule_config STRING,
  description STRING,
  author STRING,
  created_at TIMESTAMP,
  is_active BOOLEAN
)
USING DELTA
""")

print("✅ Rule version table created")
print("\nExample rule version record:")
print("  version: v1.0.0")
print("  rule_name: transaction_id_not_null")
print("  rule_type: NOT_NULL")
print("  rule_config: {\"column\": \"transaction_id\", \"level\": \"error\"}")
print("  description: Initial production rules for transaction validation")
print("  author: data-quality-team")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Load Specific Version

# COMMAND ----------

# DBTITLE 1,Cell 32
# Example: Load specific version from Unity Catalog
print("✅ Loading rules version v1.0.0")
print("\nIn production, you would:")
print("1. Query the rule_versions table for version v1.0.0")
print("2. Parse the rule_config JSON")
print("3. Reconstruct the validation logic")
print("4. Apply to your DataFrame")
print("\nExample query:")
print("  SELECT * FROM quality_rules.rule_versions")
print("  WHERE version = 'v1.0.0' AND is_active = true")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Compare Versions

# COMMAND ----------

# DBTITLE 1,Cell 34
# Example: Compare rule versions
print("Version Comparison (v1.0.0 -> v2.0.0):")
print("\nChanges:")
print("  + Added: transaction_id UNIQUE check (error level)")
print("  ~ Modified: amount_positive threshold from 0 to 0.01")
print("  - Removed: legacy_status_check")
print("\nImpact Analysis:")
print("  - Expected to catch ~50 duplicate transaction_ids per day")
print("  - May reject ~10 additional records due to stricter amount check")
print("  - Removes deprecated status validation")
print("\nIn production, you would:")
print("1. Query both versions from rule_versions table")
print("2. Compare rule_config JSON structures")
print("3. Generate a diff report")
print("4. Run impact analysis on sample data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Custom checks** enable business-specific validations
# MAGIC
# MAGIC ✅ **Multi-table validation** ensures referential integrity
# MAGIC
# MAGIC ✅ **Performance optimization** critical for large datasets
# MAGIC
# MAGIC ✅ **CI/CD integration** automates quality gates
# MAGIC
# MAGIC ✅ **Rule versioning** tracks quality standards evolution
# MAGIC
# MAGIC ✅ **Unit tests** ensure rule correctness
# MAGIC
# MAGIC ✅ **Parallel validation** speeds up multi-table checks

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices Summary
# MAGIC
# MAGIC ### Development
# MAGIC 1. **Write unit tests** for custom checks
# MAGIC 2. **Version control** all quality rules
# MAGIC 3. **Document business logic** in check descriptions
# MAGIC 4. **Use type hints** and docstrings
# MAGIC
# MAGIC ### Performance
# MAGIC 1. **Partition large datasets** appropriately
# MAGIC 2. **Cache frequently validated** data
# MAGIC 3. **Order checks** from cheap to expensive
# MAGIC 4. **Use parallel validation** for multiple tables
# MAGIC
# MAGIC ### Operations
# MAGIC 1. **Integrate with CI/CD** pipelines
# MAGIC 2. **Monitor quality metrics** continuously
# MAGIC 3. **Set up automated alerts** for degradation
# MAGIC 4. **Review and update rules** regularly
# MAGIC
# MAGIC ### Governance
# MAGIC 1. **Maintain rule versions** with descriptions
# MAGIC 2. **Require PR reviews** for rule changes
# MAGIC 3. **Document exceptions** and waivers
# MAGIC 4. **Track quality SLAs** and compliance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the final module, we'll explore:
# MAGIC 1. **Production implementation patterns** - Complete enterprise setup
# MAGIC 2. **End-to-end pipeline** with all DQX features
# MAGIC 3. **Operational playbooks** for quality management
# MAGIC 4. **Real-world case studies** and examples
# MAGIC
# MAGIC **Continue to**: `10_Production_Implementation_Patterns`
