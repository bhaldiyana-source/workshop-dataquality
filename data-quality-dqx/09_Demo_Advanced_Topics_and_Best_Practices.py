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

# Import required libraries
from dqx import Validator, CustomCheck, MultiTableValidator, CheckType, Rule
from pyspark.sql import functions as F
import unittest

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Custom Check Development
# MAGIC
# MAGIC ### 1.1 Create Custom Check Class

# COMMAND ----------

class BusinessRuleCheck(CustomCheck):
    """
    Custom check for complex business rules
    Example: Revenue must exceed cost by minimum margin
    """
    
    def __init__(self, revenue_col, cost_col, min_margin=0.2):
        self.revenue_col = revenue_col
        self.cost_col = cost_col
        self.min_margin = min_margin
        super().__init__(name=f"business_margin_check_{min_margin}")
    
    def validate(self, df):
        """
        Validate that revenue exceeds cost by minimum margin
        Returns DataFrame with validation results
        """
        return df.withColumn(
            self.result_column,
            (F.col(self.revenue_col) >= F.col(self.cost_col) * (1 + self.min_margin))
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

# Create validator with custom check
validator = Validator()

# Add custom business rule check
custom_check = BusinessRuleCheck(
    revenue_col="revenue",
    cost_col="cost",
    min_margin=0.20  # 20% minimum margin
)

validator.add_custom_check(custom_check)

# Add standard checks
validator.add_check(
    check_type=CheckType.GREATER_THAN,
    column="revenue",
    threshold=0,
    level="error"
)

validator.add_check(
    check_type=CheckType.GREATER_THAN,
    column="cost",
    threshold=0,
    level="error"
)

# Validate
result_df, summary = validator.validate(financial_df)

print(f"\nValidation with Custom Check:")
print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")
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

# Create multi-table validator
multi_validator = MultiTableValidator()

# Add foreign key check: orders.customer_id -> customers.customer_id
multi_validator.add_foreign_key_check(
    source_table="orders",
    source_column="customer_id",
    target_table="customers",
    target_column="customer_id",
    level="error"
)

# Add foreign key check: orders.product_id -> products.product_id
multi_validator.add_foreign_key_check(
    source_table="orders",
    source_column="product_id",
    target_table="products",
    target_column="product_id",
    level="error"
)

# Run multi-table validation
tables_dict = {
    "orders": orders_df,
    "customers": customers_df,
    "products": products_df
}

results = multi_validator.validate(tables_dict)

print("Referential Integrity Validation:")
display(results["orders"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Cross-Table Consistency Checks

# COMMAND ----------

# Add consistency check: order total should match quantity * price
multi_validator.add_consistency_check(
    tables=["orders", "products"],
    expression="orders.total_amount = orders.quantity * products.price",
    join_condition="orders.product_id = products.product_id",
    level="warning"
)

# Run validation
consistency_results = multi_validator.validate(tables_dict)

print("Consistency Check Results:")
display(consistency_results["orders"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Performance Optimization
# MAGIC
# MAGIC ### 3.1 Partitioning Strategies

# COMMAND ----------

# Create large dataset
import random
from datetime import datetime, timedelta

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
print(f"Partitions: {large_df.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Caching Strategies

# COMMAND ----------

# Cache frequently validated data
large_df.cache()
large_df.count()  # Materialize cache

print("✅ Dataset cached for faster validation")

# Create validator
perf_validator = Validator()

perf_validator.add_check(
    check_type=CheckType.NOT_NULL,
    column="transaction_id",
    level="error"
)

perf_validator.add_check(
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error"
)

# Time validation
import time

start_time = time.time()
result_df, summary = perf_validator.validate(large_df)
end_time = time.time()

print(f"\nValidation Performance:")
print(f"  Records: {large_df.count():,}")
print(f"  Time: {end_time - start_time:.2f} seconds")
print(f"  Throughput: {large_df.count() / (end_time - start_time):,.0f} records/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Check Ordering

# COMMAND ----------

# Order checks from cheapest to most expensive
optimized_validator = Validator()

# 1. Cheap checks first (column-level, no computation)
optimized_validator.add_check(
    name="id_not_null",
    check_type=CheckType.NOT_NULL,
    column="id",
    level="error"
)

# 2. Medium cost (simple comparisons)
optimized_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error"
)

# 3. Expensive checks last (regex, complex expressions)
optimized_validator.add_check(
    name="transaction_id_format",
    check_type=CheckType.REGEX_MATCH,
    column="transaction_id",
    pattern=r"^TXN\d{8}$",
    level="warning"
)

print("✅ Validator optimized with check ordering")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Parallel Validation

# COMMAND ----------

# Validate multiple tables in parallel
from concurrent.futures import ThreadPoolExecutor

def validate_table(table_name, df, validator):
    """Validate a single table"""
    result_df, summary = validator.validate(df, table_name=table_name)
    return table_name, summary

# Create sample tables
tables_to_validate = {
    "transactions": large_df.limit(1000),
    "customers": customers_df,
    "products": products_df
}

# Parallel validation
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [
        executor.submit(validate_table, table_name, df, optimized_validator)
        for table_name, df in tables_to_validate.items()
    ]
    
    results = [future.result() for future in futures]

print("\nParallel Validation Results:")
for table_name, summary in results:
    print(f"  {table_name}: {summary.get('pass_rate', 0):.2%} pass rate")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: CI/CD Integration
# MAGIC
# MAGIC ### 4.1 Unit Tests for Quality Rules

# COMMAND ----------

class TestDataQualityRules(unittest.TestCase):
    """Unit tests for data quality rules"""
    
    def setUp(self):
        """Set up test data and validator"""
        self.validator = Validator()
        self.sample_df = spark.createDataFrame(
            [
                (1, "CUST001", 100.00, "active"),
                (2, "CUST002", 200.00, "active"),
            ],
            ["id", "customer_id", "amount", "status"]
        )
    
    def test_customer_id_not_null(self):
        """Test customer ID NOT NULL check"""
        self.validator.add_check(
            check_type=CheckType.NOT_NULL,
            column="customer_id",
            level="error"
        )
        result_df, summary = self.validator.validate(self.sample_df)
        self.assertEqual(summary['failed_records'], 0)
    
    def test_amount_positive(self):
        """Test amount is positive"""
        self.validator.add_check(
            check_type=CheckType.GREATER_THAN,
            column="amount",
            threshold=0,
            level="error"
        )
        result_df, summary = self.validator.validate(self.sample_df)
        self.assertEqual(summary['failed_records'], 0)
    
    def test_invalid_data_detection(self):
        """Test that invalid data is detected"""
        invalid_df = spark.createDataFrame(
            [(1, None, -100.00, "active")],  # Null customer_id, negative amount
            ["id", "customer_id", "amount", "status"]
        )
        
        self.validator.add_check(
            check_type=CheckType.NOT_NULL,
            column="customer_id",
            level="error"
        )
        
        result_df, summary = self.validator.validate(invalid_df)
        self.assertGreater(summary['failed_records'], 0)

# Run tests
suite = unittest.TestLoader().loadTestsFromTestCase(TestDataQualityRules)
runner = unittest.TextTestRunner(verbosity=2)
test_results = runner.run(suite)

print(f"\n✅ Tests completed: {test_results.testsRun} tests, {len(test_results.failures)} failures")

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

from dqx import RuleVersionManager

# Create version manager
version_manager = RuleVersionManager(
    catalog="quality_rules",
    schema="versions"
)

print("✅ RuleVersionManager created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Save Rule Version

# COMMAND ----------

# Save current rules as version
version_manager.save_rules(
    validator=optimized_validator,
    version="v1.0.0",
    description="Initial production rules for transaction validation",
    author="data-quality-team"
)

print("✅ Rules saved as version v1.0.0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Load Specific Version

# COMMAND ----------

# Load specific version
validator_v1 = version_manager.load_rules(version="v1.0.0")

print("✅ Loaded rules version v1.0.0")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Compare Versions

# COMMAND ----------

# Save updated version
updated_validator = Validator()
updated_validator.add_check(
    check_type=CheckType.NOT_NULL,
    column="transaction_id",
    level="error"
)
updated_validator.add_check(
    check_type=CheckType.UNIQUE,
    column="transaction_id",
    level="error"
)

version_manager.save_rules(
    validator=updated_validator,
    version="v2.0.0",
    description="Added uniqueness check for transaction_id",
    author="data-quality-team"
)

# Compare versions
diff = version_manager.compare_versions("v1.0.0", "v2.0.0")

print("Version Comparison (v1.0.0 -> v2.0.0):")
display(diff)

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
# MAGIC **Continue to**: `10_Demo_Production_Implementation_Patterns`
