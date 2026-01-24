# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Data Profiling
# MAGIC
# MAGIC **Hands-On Lab Exercise 3**
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
# MAGIC 1. Profile datasets to understand data characteristics
# MAGIC 2. Generate quality rules automatically from profiles
# MAGIC 3. Fine-tune rule thresholds
# MAGIC 4. Apply profile-based validation
# MAGIC 5. Compare profiles over time

# COMMAND ----------

# Import required libraries
from dqx import Profiler, RuleGenerator, Validator
from pyspark.sql import functions as F
import random
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Basic Data Profiling (20 minutes)
# MAGIC
# MAGIC ### Task 1.1: Create Realistic Customer Dataset

# COMMAND ----------

# TODO: Create a customer dataset with realistic data
# Include:
# - customer_id (unique, sequential)
# - name (various names)
# - age (18-80, mostly 25-65)
# - email (valid format)
# - phone (XXX-XXX-XXXX format)
# - account_balance (0-100000, most under 50000)
# - membership_level (bronze, silver, gold, platinum - weighted distribution)
# - registration_date (last 2 years)
# - last_login (last 90 days, some nulls for inactive)
# 
# Create at least 100 records

# Your code here:
def generate_customer_data(num_records=100):
    # Implement data generation
    pass

customer_data = generate_customer_data(100)

customers_df = spark.createDataFrame(
    customer_data,
    ["customer_id", "name", "age", "email", "phone", "account_balance", 
     "membership_level", "registration_date", "last_login"]
)

display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Profile the Customer Data

# COMMAND ----------

# TODO: Create a Profiler and profile the customer dataset
# The profile should contain statistics for each column

profiler = Profiler()

# Your code here:
profile_results = # profile the data

# Display profile results
display(profile_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Analyze Profile Statistics

# COMMAND ----------

# TODO: Analyze the profile results and answer:
# 1. Which columns have null values? What percentage?
# 2. Which columns are unique?
# 3. What are the min/max/avg values for numeric columns?
# 4. What are the distinct counts for categorical columns?

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Auto-Generating Quality Rules (25 minutes)
# MAGIC
# MAGIC ### Task 2.1: Generate Rules from Profile

# COMMAND ----------

# TODO: Create a RuleGenerator and generate quality rules
# Use 95% confidence threshold initially

rule_generator = RuleGenerator()

# Your code here:
suggested_rules = # generate rules from profile

# Display suggested rules
display(suggested_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Analyze Generated Rules

# COMMAND ----------

# TODO: Analyze the generated rules:
# 1. Count rules by check type
# 2. List NOT NULL rules
# 3. List range rules (BETWEEN, GREATER_THAN, LESS_THAN)
# 4. List uniqueness rules

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Experiment with Different Thresholds

# COMMAND ----------

# TODO: Generate rules with different confidence thresholds:
# - 99% (very strict)
# - 95% (standard)
# - 90% (lenient)
# - 80% (very lenient)
# 
# Compare the number and types of rules generated

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Applying Profile-Based Validation (25 minutes)
# MAGIC
# MAGIC ### Task 3.1: Validate with Auto-Generated Rules

# COMMAND ----------

# TODO: Create a validator and add rules from the profile
# Use the 95% confidence rules

validator = Validator()

# Your code here:
# validator.add_rules_from_profile(suggested_rules)

# Validate the original data
result_df, summary = validator.validate(customers_df)

# Display results
print(f"Pass Rate: {summary.get('pass_rate', 0):.2%}")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Test with New Data

# COMMAND ----------

# TODO: Create new customer data with intentional quality issues:
# - Some nulls in previously non-null columns
# - Ages outside the normal range
# - Account balances beyond typical range
# - Invalid membership levels

new_customer_data = [
    # Add records with quality issues
]

new_customers_df = spark.createDataFrame(
    new_customer_data,
    ["customer_id", "name", "age", "email", "phone", "account_balance", 
     "membership_level", "registration_date", "last_login"]
)

# Validate new data with profile-based rules
new_result_df, new_summary = validator.validate(new_customers_df)

# Compare pass rates
print(f"Original Data Pass Rate: {summary.get('pass_rate', 0):.2%}")
print(f"New Data Pass Rate: {new_summary.get('pass_rate', 0):.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Analyze Validation Failures

# COMMAND ----------

# TODO: Analyze why new data failed validation:
# 1. Which checks failed most frequently?
# 2. Which columns had the most issues?
# 3. Were the failures legitimate quality issues or false positives?

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Profile Comparison and Drift Detection (Bonus - 30 minutes)
# MAGIC
# MAGIC ### Task 4.1: Create Evolved Dataset

# COMMAND ----------

# TODO: Create a new dataset representing data drift:
# - Customers are generally younger (shift in demographics)
# - Account balances are lower (economic factors)
# - More premium memberships (business growth)
# - Different registration patterns

# Your code here:
def generate_evolved_data(num_records=100):
    # Implement evolved data generation
    # Shift distributions compared to original
    pass

evolved_df = # generate evolved data

display(evolved_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Profile Evolved Data

# COMMAND ----------

# TODO: Profile the evolved dataset

evolved_profile = profiler.profile(evolved_df)

display(evolved_profile)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3: Compare Profiles

# COMMAND ----------

# TODO: Compare original and evolved profiles:
# 1. Compare age statistics (min, max, avg, stddev)
# 2. Compare account_balance statistics
# 3. Compare membership_level distributions
# 4. Identify significant changes

# Create comparison views
print("Age Statistics Comparison:")
original_age = profile_results.filter("column_name = 'age'")
evolved_age = evolved_profile.filter("column_name = 'age'")

# Your code here to display comparison


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.4: Detect Data Drift

# COMMAND ----------

# TODO: Implement data drift detection:
# 1. Calculate percentage change in key statistics
# 2. Identify columns with significant drift (>10% change)
# 3. Determine if profile-based rules need updating

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.5: Update Quality Rules

# COMMAND ----------

# TODO: Based on the evolved profile:
# 1. Generate new quality rules
# 2. Compare with original rules
# 3. Decide which rules to update
# 4. Validate evolved data with updated rules

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Production Profiling Strategy (Bonus)
# MAGIC
# MAGIC ### Task 5.1: Save Profiles to Delta Tables

# COMMAND ----------

# TODO: Design a strategy to save profiles over time:
# 1. Create a schema for storing profiles
# 2. Save profiles with timestamp
# 3. Enable historical comparison

# Your code here:
# profile_results.write.mode("append").saveAsTable("dqx_lab.customer_profiles")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Create Profile Comparison Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write SQL queries for a profile comparison dashboard:
# MAGIC -- 1. Latest profile summary
# MAGIC -- 2. Profile changes over time
# MAGIC -- 3. Columns with most drift
# MAGIC -- 4. Quality rule update recommendations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC ✅ Profiling analyzes data characteristics automatically
# MAGIC
# MAGIC ✅ RuleGenerator creates quality rules from profiles
# MAGIC
# MAGIC ✅ Confidence thresholds control rule generation sensitivity
# MAGIC
# MAGIC ✅ Profile comparison detects data drift
# MAGIC
# MAGIC ✅ Profile-based validation catches anomalies and changes
# MAGIC
# MAGIC ✅ Regular profiling helps maintain rule relevance
# MAGIC
# MAGIC ### Challenge Questions
# MAGIC 1. How often should you re-profile your data?
# MAGIC 2. What confidence threshold is appropriate for production?
# MAGIC 3. How do you distinguish between drift and quality issues?
# MAGIC 4. Should you update rules automatically or manually review?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Lab 4: Reaction Strategies** to learn about:
# MAGIC - Configuring DROP reactions
# MAGIC - Implementing MARK strategies
# MAGIC - Setting up quarantine tables
# MAGIC - Building remediation workflows
