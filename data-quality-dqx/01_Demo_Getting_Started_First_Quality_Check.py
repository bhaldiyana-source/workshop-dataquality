# Databricks notebook source
# MAGIC %md
# MAGIC # Getting Started - Your First Data Quality Check
# MAGIC
# MAGIC **Module 2: Introduction to DQX Validation**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 200           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Install DQX** in your Databricks environment
# MAGIC 2. **Create a Validator** and define simple quality checks
# MAGIC 3. **Run validation** on sample data
# MAGIC 4. **Interpret validation results** and summaries
# MAGIC 5. **Understand check types** and severity levels

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Installation
# MAGIC
# MAGIC DQX can be installed from PyPI. Run the following command to install:

# COMMAND ----------

# Install DQX from PyPI
%pip install databricks-labs-dqx

# COMMAND ----------

# Restart Python kernel to load the newly installed package
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import DQX Components
# MAGIC
# MAGIC Let's import the core DQX components we'll use:

# COMMAND ----------

from dqx import Validator, CheckType
from pyspark.sql import functions as F

print("✅ DQX imported successfully!")
print(f"DQX Components available: Validator, CheckType")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Sample Data
# MAGIC
# MAGIC Let's create a simple dataset with some quality issues to demonstrate DQX:

# COMMAND ----------

# Create sample data with intentional quality issues
data = [
    (1, "John Doe", 30, "john.doe@example.com", "2024-01-15"),
    (2, "Jane Smith", -5, "jane.smith@example.com", "2024-01-16"),      # Issue: negative age
    (3, "Bob Johnson", 45, "bob.johnson@example.com", "2024-01-17"),
    (4, None, 35, "alice@example.com", "2024-01-18"),                   # Issue: null name
    (5, "Charlie Brown", 28, "charlie.brown@example.com", "2024-01-19"),
    (6, "Diana Prince", 32, "invalid-email", "2024-01-20"),             # Issue: invalid email format
    (7, "Eve Adams", 150, "eve.adams@example.com", "2024-01-21"),       # Issue: age > 120 (unlikely)
    (8, "Frank Miller", 41, "frank@example.com", "2024-01-22"),
    (9, "Grace Lee", 29, "grace.lee@", "2024-01-23"),                   # Issue: incomplete email
    (10, "Henry Ford", 38, "henry.ford@example.com", "2024-01-24")
]

# Create DataFrame
df = spark.createDataFrame(data, ["id", "name", "age", "email", "registration_date"])

print(f"✅ Created sample DataFrame with {df.count()} records")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create a Validator
# MAGIC
# MAGIC The `Validator` is the main component for defining and running quality checks:

# COMMAND ----------

# Create a new validator instance
validator = Validator()

print("✅ Validator created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Define Quality Checks
# MAGIC
# MAGIC Let's add various types of quality checks to our validator:
# MAGIC
# MAGIC ### Check 1: NOT NULL Check
# MAGIC Ensure the `name` column has no null values

# COMMAND ----------

# Add NOT NULL check for name column
validator.add_check(
    check_type=CheckType.NOT_NULL,
    column="name",
    level="error"
)

print("✅ Added NOT NULL check for 'name' column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 2: Range Check
# MAGIC Ensure age is positive (greater than 0)

# COMMAND ----------

# Add range check for age
validator.add_check(
    check_type=CheckType.GREATER_THAN,
    column="age",
    threshold=0,
    level="error"
)

print("✅ Added GREATER_THAN check for 'age' column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 3: Pattern Matching Check
# MAGIC Validate email format using regex

# COMMAND ----------

# Add regex pattern check for email
validator.add_check(
    check_type=CheckType.REGEX_MATCH,
    column="email",
    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    level="warning"
)

print("✅ Added REGEX_MATCH check for 'email' column")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 4: Additional Range Check
# MAGIC Ensure age is realistic (less than 120)

# COMMAND ----------

# Add upper bound check for age
validator.add_check(
    check_type=CheckType.LESS_THAN,
    column="age",
    threshold=120,
    level="warning"
)

print("✅ Added LESS_THAN check for 'age' column")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Run Validation
# MAGIC
# MAGIC Now let's run the validation on our sample data:

# COMMAND ----------

# Run validation
result_df, summary = validator.validate(df)

print("✅ Validation completed!")
print(f"\nValidation Summary:")
print(f"  Total Records: {summary.get('total_records', 'N/A')}")
print(f"  Valid Records: {summary.get('valid_records', 'N/A')}")
print(f"  Failed Records: {summary.get('failed_records', 'N/A')}")
print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Inspect Results
# MAGIC
# MAGIC ### Result DataFrame
# MAGIC The result DataFrame includes quality check columns:

# COMMAND ----------

# Display the result DataFrame
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary Statistics
# MAGIC Let's examine the detailed summary:

# COMMAND ----------

# Display detailed summary
print("Detailed Validation Summary:")
print("=" * 50)

for key, value in summary.items():
    print(f"{key:.<30} {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failed Records Analysis
# MAGIC Let's identify which records failed validation:

# COMMAND ----------

# Filter for records that failed error-level checks
failed_records = result_df.filter("dqx_quality_status = 'FAILED'")

print(f"Total Failed Records: {failed_records.count()}")
display(failed_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Warning Records Analysis
# MAGIC Let's identify records with warnings:

# COMMAND ----------

# Filter for records with warnings
warning_records = result_df.filter("dqx_quality_status = 'WARNING'")

print(f"Total Warning Records: {warning_records.count()}")
display(warning_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Understanding Check Types
# MAGIC
# MAGIC DQX provides various built-in check types:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Check Types
# MAGIC
# MAGIC | Check Type | Description | Example |
# MAGIC |-----------|-------------|---------|
# MAGIC | `NOT_NULL` | Column must not contain null values | `name IS NOT NULL` |
# MAGIC | `UNIQUE` | Column values must be unique | `id` column |
# MAGIC | `GREATER_THAN` | Value must exceed threshold | `age > 0` |
# MAGIC | `LESS_THAN` | Value must be below threshold | `age < 120` |
# MAGIC | `BETWEEN` | Value must be within range | `0 < age < 120` |
# MAGIC | `IN_SET` | Value must be in allowed list | `status IN ('A', 'I')` |
# MAGIC | `REGEX_MATCH` | Value must match pattern | Email validation |
# MAGIC | `EXPRESSION` | Custom SQL expression | Business logic |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Understanding Check Levels
# MAGIC
# MAGIC DQX supports two severity levels for checks:

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Levels
# MAGIC
# MAGIC | Level | Severity | Behavior | Use Case |
# MAGIC |-------|----------|----------|----------|
# MAGIC | **error** | High | Marks record as FAILED | Critical validations |
# MAGIC | **warning** | Low | Marks record as WARNING | Non-critical issues |
# MAGIC
# MAGIC **Best Practice**: 
# MAGIC - Use `error` for data that cannot be processed (nulls in required fields, invalid references)
# MAGIC - Use `warning` for data quality issues that should be flagged but don't prevent processing

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Clean Results Example
# MAGIC
# MAGIC Let's create a dataset that passes all checks:

# COMMAND ----------

# Create clean sample data
clean_data = [
    (1, "Alice Johnson", 30, "alice.johnson@example.com", "2024-01-15"),
    (2, "Bob Smith", 45, "bob.smith@example.com", "2024-01-16"),
    (3, "Carol White", 28, "carol.white@example.com", "2024-01-17"),
    (4, "David Brown", 35, "david.brown@example.com", "2024-01-18"),
    (5, "Emma Davis", 42, "emma.davis@example.com", "2024-01-19")
]

clean_df = spark.createDataFrame(clean_data, ["id", "name", "age", "email", "registration_date"])

# Create new validator
clean_validator = Validator()

# Add the same checks
clean_validator.add_check(
    check_type=CheckType.NOT_NULL,
    column="name",
    level="error"
)

clean_validator.add_check(
    check_type=CheckType.GREATER_THAN,
    column="age",
    threshold=0,
    level="error"
)

clean_validator.add_check(
    check_type=CheckType.REGEX_MATCH,
    column="email",
    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    level="warning"
)

# Validate clean data
clean_result_df, clean_summary = clean_validator.validate(clean_df)

print("Clean Data Validation Summary:")
print(f"  Total Records: {clean_summary.get('total_records', 'N/A')}")
print(f"  Valid Records: {clean_summary.get('valid_records', 'N/A')}")
print(f"  Failed Records: {clean_summary.get('failed_records', 'N/A')}")
print(f"  Pass Rate: {clean_summary.get('pass_rate', 0):.2%}")

display(clean_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Multiple Checks Example
# MAGIC
# MAGIC Let's see a comprehensive example with multiple check types:

# COMMAND ----------

# Create a more complex dataset
complex_data = [
    (1, "Customer001", 1250.50, "completed", "2024-01-15", 125.00),
    (2, "Customer002", -100.00, "completed", "2024-01-16", 10.00),     # Issue: negative amount
    (3, "Customer003", 3500.00, "pending", "2024-01-17", 350.00),
    (4, None, 500.00, "completed", "2024-01-18", 50.00),               # Issue: null customer_id
    (5, "Customer005", 750.00, "invalid", "2024-01-19", 75.00),        # Issue: invalid status
    (6, "Customer006", 2000.00, "cancelled", "2024-01-20", 1500.00),   # Issue: discount > 50%
]

transactions_df = spark.createDataFrame(
    complex_data, 
    ["id", "customer_id", "amount", "status", "transaction_date", "discount"]
)

print("Sample Transactions Data:")
display(transactions_df)

# COMMAND ----------

# Create validator with multiple check types
multi_validator = Validator()

# NOT NULL check
multi_validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error"
)

# Range check
multi_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error"
)

# Set membership check
multi_validator.add_check(
    name="valid_status",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "completed", "cancelled"],
    level="warning"
)

# Run validation
multi_result_df, multi_summary = multi_validator.validate(transactions_df)

print("\nMulti-Check Validation Summary:")
print(f"  Pass Rate: {multi_summary.get('pass_rate', 0):.2%}")
print(f"  Failed Checks: {multi_summary.get('failed_checks', 'N/A')}")

display(multi_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **DQX is easy to install** with `pip install databricks-labs-dqx`
# MAGIC
# MAGIC ✅ **Validator is the main component** for defining and running checks
# MAGIC
# MAGIC ✅ **Multiple check types available**: NOT_NULL, GREATER_THAN, REGEX_MATCH, IN_SET, etc.
# MAGIC
# MAGIC ✅ **Two severity levels**: `error` (critical) and `warning` (non-critical)
# MAGIC
# MAGIC ✅ **Validation returns** both result DataFrame and summary statistics
# MAGIC
# MAGIC ✅ **Result DataFrame includes** quality status columns for analysis
# MAGIC
# MAGIC ✅ **Summary provides** pass rates, failed record counts, and check details

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Row-level validation rules** for cross-column checks
# MAGIC 2. **Column-level rules** in more detail
# MAGIC 3. **Complex validation scenarios** with business logic
# MAGIC 4. **Custom expressions** for advanced checks
# MAGIC
# MAGIC **Continue to**: `02_Demo_Row_and_Column_Level_Rules`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise
# MAGIC
# MAGIC Try creating your own validation rules!
# MAGIC
# MAGIC 1. Create a DataFrame with product data (product_id, name, price, quantity)
# MAGIC 2. Add validation rules:
# MAGIC    - Product ID should not be null
# MAGIC    - Price should be greater than 0
# MAGIC    - Quantity should be between 0 and 10000
# MAGIC 3. Run validation and analyze results

# COMMAND ----------

# Your code here!
# Uncomment and complete:

# product_data = [
#     (1, "Product A", 19.99, 100),
#     (2, "Product B", -5.00, 50),  # Issue: negative price
#     (3, None, 29.99, 200),         # Issue: null product_id
#     (4, "Product D", 15.50, 150),
# ]
# 
# products_df = spark.createDataFrame(
#     product_data, 
#     ["product_id", "name", "price", "quantity"]
# )
# 
# # Create your validator and add checks
# my_validator = Validator()
# 
# # Add your checks here...
# 
# # Run validation
# my_result_df, my_summary = my_validator.validate(products_df)
# display(my_result_df)
