# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Basic Quality Checks
# MAGIC
# MAGIC **Hands-On Lab Exercise 1**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 200           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Objectives
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Set up the DQX environment
# MAGIC 2. Create sample datasets with quality issues
# MAGIC 3. Implement column-level checks (NOT NULL, UNIQUE, BETWEEN, IN_SET, REGEX)
# MAGIC 4. Run validation and analyze results
# MAGIC 5. Understand pass rates and failed records

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC First, install DQX and import necessary libraries:

# COMMAND ----------

# Install DQX
%pip install databricks-labs-dqx

# COMMAND ----------

# Restart Python kernel
dbutils.library.restartPython()

# COMMAND ----------

# Import libraries
from dqx import Validator, CheckType
from pyspark.sql import functions as F

print("✅ Libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: NOT NULL Checks (15 minutes)
# MAGIC
# MAGIC ### Task 1.1: Create Sample Data
# MAGIC Create a customer dataset with some null values in critical fields

# COMMAND ----------

# TODO: Create a DataFrame with customer data
# Include columns: customer_id, name, email, phone, status
# Add at least 10 records with some null values in different columns

# Your code here:
customer_data = [
    # Add your data rows here
    # Example: (1, "John Doe", "john@example.com", "123-456-7890", "active"),
]

customers_df = spark.createDataFrame(
    customer_data,
    ["customer_id", "name", "email", "phone", "status"]
)

# Display your data
display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Add NOT NULL Checks
# MAGIC Add NOT NULL checks for customer_id and name fields

# COMMAND ----------

# TODO: Create a validator and add NOT NULL checks
# Add checks for:
# 1. customer_id (error level)
# 2. name (error level)
# 3. email (warning level)

validator = Validator()

# Your code here:


# TODO: Run validation
# result_df, summary = validator.validate(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Analyze Results
# MAGIC
# MAGIC Answer these questions:
# MAGIC 1. What is the pass rate?
# MAGIC 2. How many records failed?
# MAGIC 3. Which columns had null values?

# COMMAND ----------

# TODO: Print validation summary
# Print:
# - Total records
# - Valid records
# - Failed records
# - Pass rate

# Your code here:


# COMMAND ----------

# TODO: Display only failed records
# Hint: Filter where dqx_quality_status = 'FAILED'

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Range and Set Membership Checks (15 minutes)
# MAGIC
# MAGIC ### Task 2.1: Create Product Data

# COMMAND ----------

# TODO: Create a product dataset with the following columns:
# - product_id (string)
# - name (string)
# - price (double) - include some negative and very high prices
# - quantity (int) - include some negative and unrealistic quantities
# - category (string) - include some invalid categories

# Your code here:
product_data = [
    # Add at least 10 products
]

products_df = spark.createDataFrame(
    product_data,
    ["product_id", "name", "price", "quantity", "category"]
)

display(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Add Range Checks

# COMMAND ----------

# TODO: Create a new validator and add the following checks:
# 1. price must be BETWEEN 0 and 10000 (error)
# 2. quantity must be BETWEEN 0 and 1000 (warning)
# 3. category must be IN_SET ["electronics", "clothing", "food", "books"] (error)

range_validator = Validator()

# Your code here:


# Run validation
range_result_df, range_summary = range_validator.validate(products_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Analyze Range Validation Results

# COMMAND ----------

# TODO: Answer the following:
# 1. How many products have invalid prices?
# 2. How many have invalid quantities?
# 3. How many have invalid categories?
# 4. What is the overall pass rate?

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Pattern Matching with Regex (15 minutes)
# MAGIC
# MAGIC ### Task 3.1: Create Contact Data

# COMMAND ----------

# TODO: Create a contact dataset with:
# - contact_id
# - email (include some invalid formats)
# - phone (include various formats, some invalid)
# - zip_code (5 digits, include some invalid)

# Your code here:
contact_data = [
    # Add at least 10 contacts
]

contacts_df = spark.createDataFrame(
    contact_data,
    ["contact_id", "email", "phone", "zip_code"]
)

display(contacts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Add Regex Checks

# COMMAND ----------

# TODO: Add regex pattern checks for:
# 1. email: pattern r"^[\w\.-]+@[\w\.-]+\.\w+$" (error)
# 2. phone: pattern r"^\d{3}-\d{3}-\d{4}$" (warning)
# 3. zip_code: pattern r"^\d{5}$" (warning)

regex_validator = Validator()

# Your code here:


# Run validation
regex_result_df, regex_summary = regex_validator.validate(contacts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Fix Data Quality Issues

# COMMAND ----------

# TODO: Based on the validation results:
# 1. Identify which pattern checks failed most
# 2. Create a corrected version of the data
# 3. Re-run validation to verify fixes

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Comprehensive Validation (Bonus - 15 minutes)
# MAGIC
# MAGIC ### Task 4.1: Create Transaction Data

# COMMAND ----------

# TODO: Create a comprehensive transaction dataset with:
# - transaction_id (should be unique, not null)
# - customer_id (not null)
# - amount (must be positive and < 100000)
# - status (must be in valid set)
# - payment_method (must be in valid set)
# - transaction_date (not null)
# 
# Include various quality issues

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Implement All Check Types

# COMMAND ----------

# TODO: Create a comprehensive validator with:
# - NOT NULL checks for critical fields
# - UNIQUE check for transaction_id
# - BETWEEN checks for amount
# - IN_SET checks for status and payment_method

comprehensive_validator = Validator()

# Your code here:


# Run validation
comp_result_df, comp_summary = comprehensive_validator.validate(transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3: Generate Quality Report

# COMMAND ----------

# TODO: Create a quality report that shows:
# 1. Overall pass rate
# 2. Number of records by quality status (PASSED, FAILED, WARNING)
# 3. Top 3 failing checks
# 4. Recommendations for improvement

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC ✅ NOT NULL checks ensure required fields have values
# MAGIC
# MAGIC ✅ BETWEEN checks validate numeric ranges
# MAGIC
# MAGIC ✅ IN_SET checks enforce valid value lists
# MAGIC
# MAGIC ✅ REGEX checks validate string patterns
# MAGIC
# MAGIC ✅ Validation results show pass rates and failed records
# MAGIC
# MAGIC ### Challenge Questions
# MAGIC 1. When should you use error vs warning level?
# MAGIC 2. How would you handle columns that can legitimately be null?
# MAGIC 3. What's the difference between UNIQUE and NOT NULL checks?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Solutions (Instructor Only)
# MAGIC
# MAGIC <details>
# MAGIC <summary>Click to expand solutions</summary>
# MAGIC
# MAGIC ### Exercise 1 Solution
# MAGIC ```python
# MAGIC # Task 1.1
# MAGIC customer_data = [
# MAGIC     (1, "John Doe", "john@example.com", "123-456-7890", "active"),
# MAGIC     (2, "Jane Smith", "jane@example.com", "234-567-8901", "active"),
# MAGIC     (3, None, "bob@example.com", "345-678-9012", "active"),  # Missing ID
# MAGIC     (4, "Alice Brown", None, "456-789-0123", "inactive"),     # Missing email
# MAGIC     (5, "Charlie Davis", "charlie@example.com", None, "active"),  # Missing phone
# MAGIC ]
# MAGIC
# MAGIC # Task 1.2
# MAGIC validator.add_check(
# MAGIC     check_type=CheckType.NOT_NULL,
# MAGIC     column="customer_id",
# MAGIC     level="error"
# MAGIC )
# MAGIC validator.add_check(
# MAGIC     check_type=CheckType.NOT_NULL,
# MAGIC     column="name",
# MAGIC     level="error"
# MAGIC )
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Lab 2: Advanced Validation Rules** to learn about:
# MAGIC - Row-level validation rules
# MAGIC - Cross-column checks
# MAGIC - Business logic validation
# MAGIC - Custom expressions
