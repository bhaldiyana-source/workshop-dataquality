# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Reaction Strategies
# MAGIC
# MAGIC **Hands-On Lab Exercise 4**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 75 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Objectives
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Configure DROP reactions for invalid data
# MAGIC 2. Implement MARK strategies for warnings
# MAGIC 3. Set up quarantine tables for data isolation
# MAGIC 4. Build remediation workflows
# MAGIC 5. Test real-world scenarios

# COMMAND ----------

# Import required libraries
from dqx import Validator, CheckType, Reaction, QuarantineConfig, Rule
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: DROP Reaction (20 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC You're building an order processing pipeline. Orders with missing critical fields
# MAGIC (order_id, customer_id) cannot be processed and must be dropped.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.1: Create Order Data with Critical Issues

# COMMAND ----------

# TODO: Create order data with the following issues:
# - Some records missing order_id
# - Some records missing customer_id
# - Some records with negative amounts
# - Include at least 20 orders total

# Your code here:
order_data = [
    # Add orders with various critical issues
]

orders_df = spark.createDataFrame(
    order_data,
    ["order_id", "customer_id", "product_id", "quantity", "amount", "order_date"]
)

print(f"Total orders: {orders_df.count()}")
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Configure DROP Reactions

# COMMAND ----------

# TODO: Create validator with DROP reactions for:
# 1. order_id NOT NULL (error, DROP)
# 2. customer_id NOT NULL (error, DROP)
# 3. amount > 0 (error, DROP)

drop_validator = Validator()

# Your code here:


# Run validation
clean_df, summary = drop_validator.validate(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Analyze DROP Results

# COMMAND ----------

# TODO: Answer the following:
# 1. How many orders were dropped?
# 2. What was the pass rate?
# 3. What percentage of data was lost?
# 4. Is DROP the right strategy for this scenario?

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: MARK Reaction (20 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC Customer contact information has quality issues, but you want to process
# MAGIC all records and flag problematic ones for follow-up.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.1: Create Customer Contact Data

# COMMAND ----------

# TODO: Create customer data with:
# - customer_id (all valid)
# - email (some invalid formats)
# - phone (some invalid formats)
# - address (some missing)
# - Include at least 25 customers

# Your code here:
customer_data = [
    # Add customers with contact quality issues
]

customers_df = spark.createDataFrame(
    customer_data,
    ["customer_id", "name", "email", "phone", "address", "city", "zip_code"]
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Implement MARK Strategy

# COMMAND ----------

# TODO: Create validator with MARK reactions:
# 1. email format check (warning, MARK with "email_quality_flag")
# 2. phone format check (warning, MARK with "phone_quality_flag")
# 3. address not null (warning, MARK with "address_quality_flag")

mark_validator = Validator()

# Your code here:


# Run validation
marked_df, mark_summary = mark_validator.validate(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Analyze Marked Records

# COMMAND ----------

# TODO: Create a quality score for each customer:
# - Count how many quality flags are false
# - Categorize as: "excellent", "good", "needs_attention", "poor"
# - Display summary by quality category

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.4: Create Follow-Up List

# COMMAND ----------

# TODO: Generate a follow-up list for the data team:
# - Customers with poor quality scores
# - Specific issues to fix for each customer
# - Priority based on number of issues

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: QUARANTINE Reaction (25 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC Financial transactions need comprehensive validation. Failed records should be
# MAGIC quarantined for review before processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.1: Create Transaction Data

# COMMAND ----------

# TODO: Create transaction data with:
# - transaction_id
# - account_id
# - amount (some negative, some extreme)
# - transaction_type (some invalid)
# - merchant_id (some null)
# - transaction_date
# Include at least 30 transactions with various issues

# Your code here:
transaction_data = [
    # Add transactions
]

transactions_df = spark.createDataFrame(
    transaction_data,
    ["transaction_id", "account_id", "amount", "transaction_type", 
     "merchant_id", "transaction_date", "status"]
)

display(transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Configure Quarantine

# COMMAND ----------

# TODO: Set up QuarantineConfig:
# - target_table: "dqx_lab.quarantine_transactions"
# - partition_by: ["transaction_date"]
# - add_metadata: True

quarantine_config = QuarantineConfig(
    # Your configuration here
)

print("✅ Quarantine configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Implement Quarantine Validation

# COMMAND ----------

# TODO: Create validator with QUARANTINE reactions:
# 1. amount > 0 (error, QUARANTINE)
# 2. amount < 100000 (error, QUARANTINE)
# 3. transaction_type in valid set (error, QUARANTINE)
# 4. merchant_id not null (error, QUARANTINE)

quarantine_validator = Validator()

# Your code here:


# Run validation
clean_transactions, quar_summary = quarantine_validator.validate(transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.4: Analyze Quarantined Records

# COMMAND ----------

# TODO: Query the quarantine table and analyze:
# 1. Total quarantined transactions
# 2. Quarantine reasons (grouped by failed checks)
# 3. Most common quality issues
# 4. Sample of quarantined records

# Your code here:
quarantined_df = spark.table("dqx_lab.quarantine_transactions")

# Your analysis here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Remediation Workflow (30 minutes)
# MAGIC
# MAGIC ### Task 4.1: Build Remediation Logic

# COMMAND ----------

# TODO: Create a function to fix common issues in quarantined data:
# - Replace null merchant_ids with "UNKNOWN"
# - Cap amounts at reasonable limits
# - Fix invalid transaction types to "OTHER"
# - Add remediation notes

def remediate_quarantined_records(quarantine_df):
    """
    Apply remediation logic to quarantined records
    """
    # Your code here:
    pass

# Apply remediation
remediated_df = remediate_quarantined_records(quarantined_df)

display(remediated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Re-validate Remediated Records

# COMMAND ----------

# TODO: Re-validate remediated records:
# 1. Apply the same validation rules
# 2. Calculate recovery rate (% that now pass)
# 3. Identify records that still fail

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3: Merge Valid Records Back

# COMMAND ----------

# TODO: Merge successfully remediated records back to main table:
# 1. Filter for records that passed re-validation
# 2. Union with original clean transactions
# 3. Calculate final dataset statistics

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Multi-Tier Reaction Strategy (Bonus - 30 minutes)
# MAGIC
# MAGIC ### Scenario
# MAGIC Implement a comprehensive quality pipeline with all three reaction types.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.1: Create Complex E-commerce Dataset

# COMMAND ----------

# TODO: Create e-commerce order data with:
# - order_id, customer_id, customer_email, customer_phone
# - product_id, product_name, quantity, unit_price, total_price
# - shipping_address, payment_method, order_status
# - Include various quality issues for each reaction type

# Your code here:
ecommerce_data = [
    # Add comprehensive e-commerce orders
]

ecommerce_df = spark.createDataFrame(
    ecommerce_data,
    ["order_id", "customer_id", "customer_email", "customer_phone",
     "product_id", "product_name", "quantity", "unit_price", "total_price",
     "shipping_address", "payment_method", "order_status"]
)

display(ecommerce_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Implement Multi-Tier Strategy

# COMMAND ----------

# TODO: Create validator with three-tier reaction strategy:
# 
# Tier 1 - DROP (Critical, unrecoverable errors):
# - order_id null
# - customer_id null
# 
# Tier 2 - QUARANTINE (Important, needs review):
# - quantity <= 0
# - total_price != quantity * unit_price
# - invalid order_status
# 
# Tier 3 - MARK (Quality warnings):
# - email format issues
# - phone format issues
# - missing shipping address

multi_validator = Validator()

# Your code here:


# Run validation
multi_result_df, multi_summary = multi_validator.validate(ecommerce_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.3: Generate Comprehensive Report

# COMMAND ----------

# TODO: Create a comprehensive quality report:
# 1. Overall summary (total, dropped, quarantined, marked, clean)
# 2. Reaction breakdown
# 3. Quality score by customer
# 4. Recommendations for process improvement

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.4: Build Quality Dashboard Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write SQL queries for a quality dashboard:
# MAGIC -- 1. Daily quality metrics
# MAGIC -- 2. Quarantine trends
# MAGIC -- 3. Top quality issues
# MAGIC -- 4. Customer quality scores

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC ✅ DROP removes invalid records permanently - use carefully
# MAGIC
# MAGIC ✅ MARK flags quality issues while preserving all data
# MAGIC
# MAGIC ✅ QUARANTINE isolates bad data for review and remediation
# MAGIC
# MAGIC ✅ Remediation workflows can recover quarantined data
# MAGIC
# MAGIC ✅ Multi-tier strategies balance data loss and quality
# MAGIC
# MAGIC ✅ Quarantine metadata enables root cause analysis
# MAGIC
# MAGIC ### Decision Matrix
# MAGIC
# MAGIC | Issue Type | Can Fix? | Business Impact | Recommended Reaction |
# MAGIC |-----------|----------|-----------------|---------------------|
# MAGIC | Missing critical ID | No | High | DROP |
# MAGIC | Invalid format | Maybe | Medium | QUARANTINE |
# MAGIC | Incomplete contact | Yes | Low | MARK |
# MAGIC | Out of range value | Maybe | Medium | QUARANTINE |
# MAGIC | Suspicious pattern | Unknown | Medium | QUARANTINE + Alert |
# MAGIC
# MAGIC ### Challenge Questions
# MAGIC 1. What's an acceptable data loss rate from DROP reactions?
# MAGIC 2. How do you prioritize quarantine remediation?
# MAGIC 3. When should you automate remediation vs manual review?
# MAGIC 4. How do you measure the ROI of quality reactions?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Lab 5: Streaming Quality** to learn about:
# MAGIC - Real-time data validation
# MAGIC - Streaming quarantine patterns
# MAGIC - Quality monitoring dashboards
# MAGIC - Handling late-arriving data
