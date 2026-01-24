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

# DBTITLE 1,Cell 4
# Install DQX from PyPI
%pip install databricks-labs-dqx --quiet

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Import DQX Components
# MAGIC
# MAGIC Let's import the core DQX components we'll use:

# COMMAND ----------

# DBTITLE 1,Cell 6
# Note: The simplified API shown below is for demonstration purposes
# The actual databricks-labs-dqx uses: DQEngine, DQRule, Criticality

# Actual databricks-labs-dqx API:
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRule, Criticality
from pyspark.sql import functions as F

print("✅ DQX components imported successfully!")
print(f"Available: DQEngine, DQRule, Criticality")
print(f"\nCriticality levels: {[x for x in dir(Criticality) if not x.startswith('_')]}")
print("\nNote: This notebook will continue with PySpark-based validation for clarity")

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ### ⚠️ Important Note About This Demo
# MAGIC
# MAGIC This notebook demonstrates a **simplified conceptual API** for data quality validation. The actual `databricks-labs-dqx` package uses a different API structure:
# MAGIC
# MAGIC * **Actual API**: `DQEngine`, `DQRule`, `Criticality`
# MAGIC * **Demo API**: `Validator`, `CheckType` (simplified for learning)
# MAGIC
# MAGIC The concepts and patterns shown here are valid, but you'll need to adapt the code when using the real databricks-labs-dqx package.
# MAGIC
# MAGIC **For production use**, refer to the [databricks-labs-dqx documentation](https://github.com/databrickslabs/dqx) for the correct API.

# COMMAND ----------

# DBTITLE 1,Alternative: Use PySpark Built-in Validation
# Since the demo API doesn't match the actual package,
# let's demonstrate data quality checks using PySpark directly

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

print("✅ PySpark imported successfully!")
print("We'll use PySpark to demonstrate data quality validation concepts")

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

# DBTITLE 1,Cell 10
# Create a data quality validation function using PySpark
# This demonstrates the concepts from the demo using actual working code

def create_quality_checks(df):
    """
    Apply data quality checks and add quality status columns
    """
    result_df = df
    
    # We'll add check result columns as we go
    # This will be populated in the following cells
    
    return result_df

print("✅ Quality check function created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Define Quality Checks
# MAGIC
# MAGIC Let's add various types of quality checks to our validator:
# MAGIC
# MAGIC ### Check 1: NOT NULL Check
# MAGIC Ensure the `name` column has no null values

# COMMAND ----------

# DBTITLE 1,Cell 12
# Add NOT NULL check for name column using PySpark
# We'll create a column that flags null values

df_with_checks = df.withColumn(
    "check_name_not_null",
    F.when(F.col("name").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
)

print("✅ Added NOT NULL check for 'name' column")
print("\nSample with check results:")
display(df_with_checks.select("id", "name", "check_name_not_null"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 2: Range Check
# MAGIC Ensure age is positive (greater than 0)

# COMMAND ----------

# DBTITLE 1,Cell 14
# Add range check for age (must be greater than 0)
df_with_checks = df_with_checks.withColumn(
    "check_age_positive",
    F.when(F.col("age") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
)

print("✅ Added GREATER_THAN check for 'age' column")
print("\nSample with check results:")
display(df_with_checks.select("id", "age", "check_age_positive"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 3: Pattern Matching Check
# MAGIC Validate email format using regex

# COMMAND ----------

# DBTITLE 1,Cell 16
# Add regex pattern check for email
email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"

df_with_checks = df_with_checks.withColumn(
    "check_email_format",
    F.when(
        F.col("email").rlike(email_pattern),
        F.lit("PASSED")
    ).otherwise(F.lit("WARNING"))
)

print("✅ Added REGEX_MATCH check for 'email' column")
print("\nSample with check results:")
display(df_with_checks.select("id", "email", "check_email_format"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 4: Additional Range Check
# MAGIC Ensure age is realistic (less than 120)

# COMMAND ----------

# DBTITLE 1,Cell 18
# Add upper bound check for age (must be less than 120)
df_with_checks = df_with_checks.withColumn(
    "check_age_realistic",
    F.when(F.col("age") >= 120, F.lit("WARNING")).otherwise(F.lit("PASSED"))
)

print("✅ Added LESS_THAN check for 'age' column")
print("\nSample with check results:")
display(df_with_checks.select("id", "age", "check_age_realistic"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Run Validation
# MAGIC
# MAGIC Now let's run the validation on our sample data:

# COMMAND ----------

# DBTITLE 1,Cell 20
# Create overall quality status column
df_with_checks = df_with_checks.withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_name_not_null") == "FAILED") | 
        (F.col("check_age_positive") == "FAILED"),
        F.lit("FAILED")
    ).when(
        (F.col("check_email_format") == "WARNING") |
        (F.col("check_age_realistic") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary statistics
total_records = df_with_checks.count()
failed_records = df_with_checks.filter(F.col("dqx_quality_status") == "FAILED").count()
warning_records = df_with_checks.filter(F.col("dqx_quality_status") == "WARNING").count()
valid_records = df_with_checks.filter(F.col("dqx_quality_status") == "PASSED").count()
pass_rate = valid_records / total_records if total_records > 0 else 0

print("✅ Validation completed!")
print(f"\nValidation Summary:")
print(f"  Total Records: {total_records}")
print(f"  Valid Records: {valid_records}")
print(f"  Failed Records: {failed_records}")
print(f"  Warning Records: {warning_records}")
print(f"  Pass Rate: {pass_rate:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Inspect Results
# MAGIC
# MAGIC ### Result DataFrame
# MAGIC The result DataFrame includes quality check columns:

# COMMAND ----------

# DBTITLE 1,Cell 22
# Display the result DataFrame with all quality check columns
print("Result DataFrame with Quality Checks:")
print("=" * 50)
display(df_with_checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Summary Statistics
# MAGIC Let's examine the detailed summary:

# COMMAND ----------

# DBTITLE 1,Cell 24
# Display detailed summary
print("Detailed Validation Summary:")
print("=" * 50)
print(f"{'total_records':.<30} {total_records}")
print(f"{'valid_records':.<30} {valid_records}")
print(f"{'failed_records':.<30} {failed_records}")
print(f"{'warning_records':.<30} {warning_records}")
print(f"{'pass_rate':.<30} {pass_rate:.2%}")

# Show check-specific statistics
print("\nCheck-Specific Results:")
print("=" * 50)
for check_col in ["check_name_not_null", "check_age_positive", "check_email_format", "check_age_realistic"]:
    failed_count = df_with_checks.filter(F.col(check_col).isin(["FAILED", "WARNING"])).count()
    print(f"{check_col:.<30} {failed_count} issues")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Failed Records Analysis
# MAGIC Let's identify which records failed validation:

# COMMAND ----------

# DBTITLE 1,Cell 26
# Filter for records that failed error-level checks
failed_df = df_with_checks.filter(F.col("dqx_quality_status") == "FAILED")

print(f"Total Failed Records: {failed_df.count()}")
print("\nFailed Records Details:")
display(failed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Warning Records Analysis
# MAGIC Let's identify records with warnings:

# COMMAND ----------

# DBTITLE 1,Cell 28
# Filter for records with warnings
warning_df = df_with_checks.filter(F.col("dqx_quality_status") == "WARNING")

print(f"Total Warning Records: {warning_df.count()}")
print("\nWarning Records Details:")
display(warning_df)

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

# DBTITLE 1,Cell 36
# Create clean sample data
clean_data = [
    (1, "Alice Johnson", 30, "alice.johnson@example.com", "2024-01-15"),
    (2, "Bob Smith", 45, "bob.smith@example.com", "2024-01-16"),
    (3, "Carol White", 28, "carol.white@example.com", "2024-01-17"),
    (4, "David Brown", 35, "david.brown@example.com", "2024-01-18"),
    (5, "Emma Davis", 42, "emma.davis@example.com", "2024-01-19")
]

clean_df = spark.createDataFrame(clean_data, ["id", "name", "age", "email", "registration_date"])

# Apply the same quality checks as before
email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"

clean_df_checked = clean_df.withColumn(
    "check_name_not_null",
    F.when(F.col("name").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_age_positive",
    F.when(F.col("age") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_email_format",
    F.when(F.col("email").rlike(email_pattern), F.lit("PASSED")).otherwise(F.lit("WARNING"))
).withColumn(
    "check_age_realistic",
    F.when(F.col("age") >= 120, F.lit("WARNING")).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_name_not_null") == "FAILED") | 
        (F.col("check_age_positive") == "FAILED"),
        F.lit("FAILED")
    ).when(
        (F.col("check_email_format") == "WARNING") |
        (F.col("check_age_realistic") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary statistics
clean_total = clean_df_checked.count()
clean_valid = clean_df_checked.filter(F.col("dqx_quality_status") == "PASSED").count()
clean_failed = clean_df_checked.filter(F.col("dqx_quality_status") == "FAILED").count()
clean_warning = clean_df_checked.filter(F.col("dqx_quality_status") == "WARNING").count()
clean_pass_rate = clean_valid / clean_total if clean_total > 0 else 0

print("Clean Data Validation Summary:")
print(f"  Total Records: {clean_total}")
print(f"  Valid Records: {clean_valid}")
print(f"  Failed Records: {clean_failed}")
print(f"  Warning Records: {clean_warning}")
print(f"  Pass Rate: {clean_pass_rate:.2%}")

display(clean_df_checked)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Multiple Checks Example
# MAGIC
# MAGIC Let's see a comprehensive example with multiple check types:

# COMMAND ----------

# DBTITLE 1,Cell 38
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

# DBTITLE 1,Cell 39
# Apply multiple check types to the transactions data
valid_statuses = ["pending", "completed", "cancelled"]

multi_checked_df = transactions_df.withColumn(
    "check_customer_id_not_null",
    F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_amount_positive",
    F.when(F.col("amount") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_valid_status",
    F.when(F.col("status").isin(valid_statuses), F.lit("PASSED")).otherwise(F.lit("WARNING"))
).withColumn(
    "check_discount_reasonable",
    F.when(F.col("discount") > F.col("amount") * 0.5, F.lit("WARNING")).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_customer_id_not_null") == "FAILED") | 
        (F.col("check_amount_positive") == "FAILED"),
        F.lit("FAILED")
    ).when(
        (F.col("check_valid_status") == "WARNING") |
        (F.col("check_discount_reasonable") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
multi_total = multi_checked_df.count()
multi_valid = multi_checked_df.filter(F.col("dqx_quality_status") == "PASSED").count()
multi_failed = multi_checked_df.filter(F.col("dqx_quality_status") == "FAILED").count()
multi_warning = multi_checked_df.filter(F.col("dqx_quality_status") == "WARNING").count()
multi_pass_rate = multi_valid / multi_total if multi_total > 0 else 0

print("\nMulti-Check Validation Summary:")
print(f"  Total Records: {multi_total}")
print(f"  Valid Records: {multi_valid}")
print(f"  Failed Records: {multi_failed}")
print(f"  Warning Records: {multi_warning}")
print(f"  Pass Rate: {multi_pass_rate:.2%}")

display(multi_checked_df)

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
# MAGIC **Continue to**: `02_Row_and_Column_Level_Rules`

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

# DBTITLE 1,Cell 43
# Your code here!
# Exercise: Create product validation

product_data = [
    (1, "Product A", 19.99, 100),
    (2, "Product B", -5.00, 50),  # Issue: negative price
    (3, None, 29.99, 200),         # Issue: null product_id
    (4, "Product D", 15.50, 150),
    (5, "Product E", 99.99, 15000), # Issue: quantity > 10000
]

products_df = spark.createDataFrame(
    product_data, 
    ["product_id", "name", "price", "quantity"]
)

# Apply validation checks
products_checked = products_df.withColumn(
    "check_product_id_not_null",
    F.when(F.col("product_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_price_positive",
    F.when(F.col("price") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_quantity_range",
    F.when(
        (F.col("quantity") < 0) | (F.col("quantity") > 10000),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_product_id_not_null") == "FAILED") | 
        (F.col("check_price_positive") == "FAILED") |
        (F.col("check_quantity_range") == "FAILED"),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
)

print("Product Validation Results:")
display(products_checked)
