# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Advanced Validation Rules
# MAGIC
# MAGIC **Hands-On Lab Exercise 2**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Objectives
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Implement row-level validation rules
# MAGIC 2. Create cross-column validation checks
# MAGIC 3. Test business logic rules
# MAGIC 4. Handle complex validation scenarios
# MAGIC 5. Deal with edge cases

# COMMAND ----------

# Import required libraries
from dqx import Validator, Rule, CheckType
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Cross-Column Validation (20 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC You have a sales order dataset where you need to validate relationships between columns:
# MAGIC - Net price should equal price minus discount
# MAGIC - Discount should not exceed 50% of price
# MAGIC - Tax amount should be calculated correctly

# COMMAND ----------

# TODO: Create sales order data with cross-column issues
# Columns: order_id, product, price, discount, tax_rate, tax_amount, net_price
# Include records where:
# - discount > price * 0.5
# - net_price != price - discount
# - tax_amount != price * tax_rate

# Your code here:
order_data = [
    # Add at least 15 orders with various issues
    # Example: (1, "Product A", 100.0, 10.0, 0.08, 8.0, 90.0),  # Valid
]

orders_df = spark.createDataFrame(
    order_data,
    ["order_id", "product", "price", "discount", "tax_rate", "tax_amount", "net_price"]
)

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1: Implement Cross-Column Rules

# COMMAND ----------

# TODO: Create validator with row-level rules:
# 1. discount <= price * 0.5
# 2. net_price = price - discount
# 3. tax_amount = price * tax_rate (within 0.01 tolerance)

validator = Validator()

# Add rules using Rule class
# Your code here:


# Run validation
result_df, summary = validator.validate(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Analyze Cross-Column Violations

# COMMAND ----------

# TODO: Identify and display:
# 1. Records where discount exceeds 50%
# 2. Records with incorrect net_price calculation
# 3. Records with incorrect tax calculation

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Business Logic Rules (20 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC You're validating employee compensation data with business rules:
# MAGIC - Interns: salary <= 40000
# MAGIC - Engineers: salary >= 60000
# MAGIC - Managers: salary >= 80000
# MAGIC - Bonus should not exceed 20% of salary
# MAGIC - Total compensation = salary + bonus

# COMMAND ----------

# TODO: Create employee data
# Columns: employee_id, name, role, salary, bonus, total_compensation
# Include violations of each business rule

# Your code here:
employee_data = [
    # Add at least 20 employees with various roles and violations
]

employees_df = spark.createDataFrame(
    employee_data,
    ["employee_id", "name", "role", "salary", "bonus", "total_compensation"]
)

display(employees_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Implement Business Rules

# COMMAND ----------

# TODO: Create rules for:
# 1. Role-based salary minimums/maximums
# 2. Bonus cap at 20% of salary
# 3. Total compensation validation
# Hint: Use conditional expressions with OR/AND logic

business_validator = Validator()

# Your code here:


# Run validation
biz_result_df, biz_summary = business_validator.validate(employees_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Identify Business Rule Violations

# COMMAND ----------

# TODO: For each business rule, count and display violations
# Create a summary showing:
# - Rule name
# - Number of violations
# - Example of violation

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Conditional Validation (20 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC Shipping cost validation with conditions:
# MAGIC - Domestic standard shipping: 5-10% of order value
# MAGIC - Domestic express: 15-25% of order value
# MAGIC - International standard: 10-20% of order value
# MAGIC - International express: 25-40% of order value
# MAGIC - Free shipping if order_value > 100

# COMMAND ----------

# TODO: Create shipping data
# Columns: order_id, order_value, shipping_type, region, shipping_cost
# shipping_type: standard, express
# region: domestic, international

# Your code here:
shipping_data = [
    # Add various combinations of shipping types and regions
]

shipping_df = spark.createDataFrame(
    shipping_data,
    ["order_id", "order_value", "shipping_type", "region", "shipping_cost"]
)

display(shipping_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Implement Conditional Rules

# COMMAND ----------

# TODO: Create conditional validation rules
# Use complex expressions with multiple conditions
# Consider all shipping type and region combinations

conditional_validator = Validator()

# Your code here:
# Example structure:
# shipping_type != 'express' OR region != 'domestic' OR 
# (shipping_cost >= order_value * 0.15 AND shipping_cost <= order_value * 0.25)


# Run validation
cond_result_df, cond_summary = conditional_validator.validate(shipping_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Test Edge Cases

# COMMAND ----------

# TODO: Test edge cases:
# 1. Free shipping threshold
# 2. Boundary values (exactly at min/max percentages)
# 3. Zero order values
# 4. Very large order values

# Create edge case dataset
edge_case_data = [
    # Add edge cases
]

# Validate edge cases
# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Complex Validation Scenario (Bonus - 30 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC Financial transactions validation with multiple rules:
# MAGIC - Transaction amount limits based on account type
# MAGIC - Daily transaction limits
# MAGIC - Fraud detection rules
# MAGIC - Currency conversion validation

# COMMAND ----------

# TODO: Create comprehensive financial transaction dataset
# Columns: transaction_id, account_id, account_type, amount, currency, 
#          exchange_rate, converted_amount, transaction_date, transaction_count_today

# Account types: basic (limit 1000), premium (limit 5000), business (limit 50000)
# Fraud rules: 
# - More than 5 transactions per day is suspicious
# - Transactions at unusual hours (2am-5am)
# - Large round numbers might be suspicious

# Your code here:
financial_data = [
    # Add various transactions
]

financial_df = spark.createDataFrame(
    financial_data,
    ["transaction_id", "account_id", "account_type", "amount", "currency",
     "exchange_rate", "converted_amount", "transaction_date", "transaction_count_today"]
)

display(financial_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.1: Implement All Validation Rules

# COMMAND ----------

# TODO: Create comprehensive validator with:
# 1. Account type based limits
# 2. Daily transaction count limits
# 3. Currency conversion validation
# 4. Suspicious activity detection

comprehensive_validator = Validator()

# Your code here:


# Run validation
comp_result_df, comp_summary = comprehensive_validator.validate(financial_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Generate Validation Report

# COMMAND ----------

# TODO: Create a comprehensive validation report:
# 1. Summary statistics by account type
# 2. List of suspicious transactions
# 3. Validation errors by category
# 4. Recommendations for data quality improvement

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC ✅ Row-level rules validate business logic across columns
# MAGIC
# MAGIC ✅ Use Rule class for complex SQL expressions
# MAGIC
# MAGIC ✅ Conditional rules enable context-specific validation
# MAGIC
# MAGIC ✅ Test edge cases to ensure robust validation
# MAGIC
# MAGIC ✅ Combine multiple rules for comprehensive validation
# MAGIC
# MAGIC ### Challenge Questions
# MAGIC 1. How do you balance strictness vs flexibility in business rules?
# MAGIC 2. When should you use row-level vs column-level checks?
# MAGIC 3. How do you handle validation rules that change over time?
# MAGIC 4. What's the performance impact of complex row-level rules?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Lab 3: Data Profiling** to learn about:
# MAGIC - Automatic data profiling
# MAGIC - Generating quality rules from profiles
# MAGIC - Fine-tuning rule thresholds
# MAGIC - Profile-based validation
