# Databricks notebook source
# MAGIC %md
# MAGIC # Data Profiling and Auto-Generated Rules
# MAGIC
# MAGIC **Module 4: Automated Quality Rule Generation**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Profile datasets** to understand data characteristics
# MAGIC 2. **Generate statistics** on columns automatically
# MAGIC 3. **Auto-generate quality rules** from profiling results
# MAGIC 4. **Fine-tune rule thresholds** based on confidence levels
# MAGIC 5. **Apply profile-based validation** to new data

# COMMAND ----------

# Import required libraries
from dqx import Profiler, RuleGenerator, Validator
from pyspark.sql import functions as F
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Understanding Data Profiling
# MAGIC
# MAGIC Data profiling analyzes your dataset to understand:
# MAGIC - Column statistics (min, max, avg, stddev)
# MAGIC - Null percentages
# MAGIC - Uniqueness metrics
# MAGIC - Data distributions
# MAGIC - Common patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Create Sample Dataset
# MAGIC
# MAGIC Let's create a customer dataset to profile:

# COMMAND ----------

# Create customer data with various characteristics
customer_data = [
    (1, "CUST001", "John Doe", 28, "john.doe@example.com", "123-456-7890", "US", "Active", 15000.00),
    (2, "CUST002", "Jane Smith", 34, "jane.smith@example.com", "234-567-8901", "US", "Active", 25000.00),
    (3, "CUST003", "Bob Johnson", 45, "bob.j@example.com", "345-678-9012", "CA", "Active", 35000.00),
    (4, "CUST004", "Alice Brown", 29, "alice.b@example.com", "456-789-0123", "UK", "Active", 18000.00),
    (5, "CUST005", "Charlie Davis", 52, "charlie.d@example.com", "567-890-1234", "US", "Inactive", 42000.00),
    (6, "CUST006", "Diana Evans", 38, "diana.e@example.com", "678-901-2345", "CA", "Active", 28000.00),
    (7, "CUST007", "Frank Miller", 41, "frank.m@example.com", "789-012-3456", "US", "Active", 31000.00),
    (8, "CUST008", "Grace Lee", 33, "grace.l@example.com", "890-123-4567", "UK", "Active", 22000.00),
    (9, "CUST009", "Henry Wilson", 47, "henry.w@example.com", "901-234-5678", "US", "Active", 38000.00),
    (10, "CUST010", "Ivy Chen", 36, "ivy.c@example.com", "012-345-6789", "CA", "Inactive", 26000.00),
    (11, "CUST011", "Jack Taylor", 31, "jack.t@example.com", "123-456-7891", "US", "Active", 19000.00),
    (12, "CUST012", "Kate Anderson", 44, "kate.a@example.com", "234-567-8902", "UK", "Active", 34000.00),
    (13, "CUST013", "Leo Martin", 39, "leo.m@example.com", "345-678-9013", "US", "Active", 29000.00),
    (14, "CUST014", "Mia Garcia", 42, "mia.g@example.com", "456-789-0124", "CA", "Active", 33000.00),
    (15, "CUST015", "Noah Rodriguez", 27, "noah.r@example.com", "567-890-1235", "US", "Active", 16000.00),
    (16, "CUST016", "Olivia Martinez", 35, "olivia.m@example.com", "678-901-2346", "UK", "Active", 27000.00),
    (17, "CUST017", "Paul Hernandez", 49, "paul.h@example.com", "789-012-3457", "US", "Inactive", 41000.00),
    (18, "CUST018", "Quinn Lopez", 32, "quinn.l@example.com", "890-123-4568", "CA", "Active", 21000.00),
    (19, "CUST019", "Rachel Gonzalez", 37, "rachel.g@example.com", "901-234-5679", "US", "Active", 30000.00),
    (20, "CUST020", "Sam Wilson", 43, "sam.w@example.com", "012-345-6790", "UK", "Active", 36000.00),
]

customers_df = spark.createDataFrame(
    customer_data,
    ["id", "customer_id", "name", "age", "email", "phone", "country", "status", "lifetime_value"]
)

print(f"Created customer dataset with {customers_df.count()} records")
display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Profiling Your Data
# MAGIC
# MAGIC ### 2.1 Create a Profiler

# COMMAND ----------

# Create a profiler instance
profiler = Profiler()

print("✅ Profiler created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Run Profiling
# MAGIC
# MAGIC Profile the dataset to generate comprehensive statistics:

# COMMAND ----------

# Profile the customer data
profile_results = profiler.profile(customers_df)

print("✅ Profiling completed!")
print(f"\nProfile contains statistics for {profile_results.count()} columns")

# Display profile results
display(profile_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Analyze Profile Results
# MAGIC
# MAGIC Let's examine the profile statistics in detail:

# COMMAND ----------

# View statistics for numeric columns
print("Numeric Column Statistics:")
numeric_stats = profile_results.filter("data_type IN ('int', 'long', 'double', 'float')")
display(numeric_stats)

# COMMAND ----------

# View statistics for string columns
print("String Column Statistics:")
string_stats = profile_results.filter("data_type = 'string'")
display(string_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Null Analysis

# COMMAND ----------

# Analyze null percentages
print("Columns with Null Values:")
null_analysis = (
    profile_results
    .select("column_name", "null_count", "null_percentage")
    .filter("null_count > 0")
    .orderBy(F.desc("null_percentage"))
)

display(null_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 Uniqueness Analysis

# COMMAND ----------

# Analyze uniqueness
print("Uniqueness Analysis:")
uniqueness_analysis = (
    profile_results
    .select("column_name", "distinct_count", "unique_percentage")
    .orderBy(F.desc("unique_percentage"))
)

display(uniqueness_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Auto-Generating Quality Rules
# MAGIC
# MAGIC ### 3.1 Create RuleGenerator

# COMMAND ----------

# Create a rule generator
rule_generator = RuleGenerator()

print("✅ RuleGenerator created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Generate Rules from Profile
# MAGIC
# MAGIC Generate quality rules based on the profiling results:

# COMMAND ----------

# Generate rules with 95% confidence threshold
suggested_rules = rule_generator.generate_rules(
    profile_results,
    confidence_threshold=0.95
)

print(f"✅ Generated {suggested_rules.count()} quality rule suggestions")
display(suggested_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Review Generated Rules by Type

# COMMAND ----------

# Rules by check type
print("Generated Rules by Check Type:")
rules_by_type = (
    suggested_rules
    .groupBy("check_type")
    .agg(F.count("*").alias("rule_count"))
    .orderBy(F.desc("rule_count"))
)

display(rules_by_type)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 NOT NULL Rules

# COMMAND ----------

# View NOT NULL rule suggestions
print("NOT NULL Rule Suggestions:")
not_null_rules = suggested_rules.filter("check_type = 'NOT_NULL'")
display(not_null_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Range Rules

# COMMAND ----------

# View range rule suggestions (BETWEEN, GREATER_THAN, LESS_THAN)
print("Range Rule Suggestions:")
range_rules = suggested_rules.filter("check_type IN ('BETWEEN', 'GREATER_THAN', 'LESS_THAN')")
display(range_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Uniqueness Rules

# COMMAND ----------

# View uniqueness rule suggestions
print("Uniqueness Rule Suggestions:")
unique_rules = suggested_rules.filter("check_type = 'UNIQUE'")
display(unique_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Applying Generated Rules
# MAGIC
# MAGIC ### 4.1 Create Validator from Profile

# COMMAND ----------

# Create validator and add rules from profile
validator = Validator()
validator.add_rules_from_profile(suggested_rules)

print(f"✅ Added {suggested_rules.count()} rules to validator from profile")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Validate Original Data
# MAGIC
# MAGIC Validate the original data against the generated rules:

# COMMAND ----------

# Validate the customer data
result_df, summary = validator.validate(customers_df)

print(f"\nValidation Results:")
print(f"  Total Records: {summary.get('total_records', 'N/A')}")
print(f"  Valid Records: {summary.get('valid_records', 'N/A')}")
print(f"  Failed Records: {summary.get('failed_records', 'N/A')}")
print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Validate New Data
# MAGIC
# MAGIC Now let's test the rules on new data with quality issues:

# COMMAND ----------

# Create new data with various quality issues
new_customer_data = [
    (21, "CUST021", "Test User 1", 25, "test1@example.com", "111-222-3333", "US", "Active", 12000.00),
    (22, None, "Test User 2", 30, "test2@example.com", "222-333-4444", "CA", "Active", 18000.00),     # Issue: null customer_id
    (23, "CUST023", "Test User 3", -5, "test3@example.com", "333-444-5555", "UK", "Active", 15000.00), # Issue: negative age
    (24, "CUST024", "Test User 4", 150, "test4@example.com", "444-555-6666", "US", "Active", 20000.00), # Issue: age > reasonable max
    (25, "CUST025", None, 35, "test5@example.com", "555-666-7777", "CA", "Active", 22000.00),         # Issue: null name
    (26, "CUST026", "Test User 6", 40, "test6@example.com", "666-777-8888", "FR", "Active", 25000.00), # Issue: unexpected country
    (27, "CUST027", "Test User 7", 33, "test7@example.com", "777-888-9999", "US", "Pending", -5000.00), # Issue: negative lifetime_value
]

new_customers_df = spark.createDataFrame(
    new_customer_data,
    ["id", "customer_id", "name", "age", "email", "phone", "country", "status", "lifetime_value"]
)

print("New customer data to validate:")
display(new_customers_df)

# COMMAND ----------

# Validate new data with profile-based rules
new_result_df, new_summary = validator.validate(new_customers_df)

print(f"\nNew Data Validation Results:")
print(f"  Total Records: {new_summary.get('total_records', 'N/A')}")
print(f"  Valid Records: {new_summary.get('valid_records', 'N/A')}")
print(f"  Failed Records: {new_summary.get('failed_records', 'N/A')}")
print(f"  Pass Rate: {new_summary.get('pass_rate', 0):.2%}")

display(new_result_df)

# COMMAND ----------

# Show only failed records
print("Failed Records:")
display(new_result_df.filter("dqx_quality_status = 'FAILED'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Fine-Tuning Rules
# MAGIC
# MAGIC ### 5.1 Adjust Confidence Thresholds
# MAGIC
# MAGIC Lower confidence threshold generates more rules:

# COMMAND ----------

# Generate rules with 80% confidence threshold
relaxed_rules = rule_generator.generate_rules(
    profile_results,
    confidence_threshold=0.80
)

print(f"Rules at 80% confidence: {relaxed_rules.count()}")
print(f"Rules at 95% confidence: {suggested_rules.count()}")
print(f"Difference: {relaxed_rules.count() - suggested_rules.count()} additional rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Filter Rules by Column
# MAGIC
# MAGIC Apply rules only to specific columns:

# COMMAND ----------

# Generate rules only for critical columns
critical_columns = ["customer_id", "name", "email", "age"]

critical_rules = (
    suggested_rules
    .filter(F.col("column_name").isin(critical_columns))
)

print(f"Rules for critical columns: {critical_rules.count()}")
display(critical_rules)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Customize Generated Rules
# MAGIC
# MAGIC Modify rule parameters based on business requirements:

# COMMAND ----------

# Example: Customize age range based on business knowledge
customized_validator = Validator()

# Add NOT NULL rules from profile
customized_validator.add_rules_from_profile(
    suggested_rules.filter("check_type = 'NOT_NULL'")
)

# Add custom age range (business knows customers are 18-75)
from dqx import CheckType

customized_validator.add_check(
    name="age_business_range",
    check_type=CheckType.BETWEEN,
    column="age",
    min_value=18,
    max_value=75,
    level="warning"
)

# Add custom lifetime_value range
customized_validator.add_check(
    name="lifetime_value_positive",
    check_type=CheckType.GREATER_THAN,
    column="lifetime_value",
    threshold=0,
    level="error"
)

print("✅ Created customized validator with profile and business rules")

# COMMAND ----------

# Validate with customized rules
custom_result_df, custom_summary = customized_validator.validate(new_customers_df)

print(f"\nCustomized Validation Results:")
print(f"  Pass Rate: {custom_summary.get('pass_rate', 0):.2%}")

display(custom_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Saving and Loading Profiles
# MAGIC
# MAGIC ### 6.1 Save Profile Results

# COMMAND ----------

# Save profile results to Delta table
profile_table = "dqx_demo.customer_profile"

profile_results.write.mode("overwrite").saveAsTable(profile_table)

print(f"✅ Profile saved to {profile_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Save Generated Rules

# COMMAND ----------

# Save suggested rules to Delta table
rules_table = "dqx_demo.customer_quality_rules"

suggested_rules.write.mode("overwrite").saveAsTable(rules_table)

print(f"✅ Rules saved to {rules_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Load and Reuse

# COMMAND ----------

# Load saved rules
loaded_rules = spark.table(rules_table)

print(f"Loaded {loaded_rules.count()} rules from {rules_table}")
display(loaded_rules)

# COMMAND ----------

# Create new validator from loaded rules
reloaded_validator = Validator()
reloaded_validator.add_rules_from_profile(loaded_rules)

print("✅ Created validator from saved rules")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Iterative Profile Refinement
# MAGIC
# MAGIC ### 7.1 Compare Profiles Over Time

# COMMAND ----------

# Simulate data drift - create dataset with different characteristics
drifted_data = [
    (101, "CUST101", "New User 1", 22, "new1@example.com", "100-200-3000", "US", "Active", 5000.00),
    (102, "CUST102", "New User 2", 23, "new2@example.com", "200-300-4000", "CA", "Active", 6000.00),
    (103, "CUST103", "New User 3", 24, "new3@example.com", "300-400-5000", "US", "Active", 7000.00),
    (104, "CUST104", "New User 4", 21, "new4@example.com", "400-500-6000", "UK", "Active", 4500.00),
    (105, "CUST105", "New User 5", 25, "new5@example.com", "500-600-7000", "US", "Active", 8000.00),
    # Younger demographic, lower lifetime values
]

drifted_df = spark.createDataFrame(
    drifted_data,
    ["id", "customer_id", "name", "age", "email", "phone", "country", "status", "lifetime_value"]
)

print("Drifted customer data (younger demographic):")
display(drifted_df)

# COMMAND ----------

# Profile the drifted data
drifted_profile = profiler.profile(drifted_df)

print("Drifted data profile:")
display(drifted_profile)

# COMMAND ----------

# Compare age statistics
print("Age Statistics Comparison:")

original_age = profile_results.filter("column_name = 'age'").select("min_value", "max_value", "avg_value", "stddev_value")
drifted_age = drifted_profile.filter("column_name = 'age'").select("min_value", "max_value", "avg_value", "stddev_value")

print("\nOriginal Data:")
display(original_age)

print("\nDrifted Data:")
display(drifted_age)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Profiler analyzes data** to generate comprehensive statistics
# MAGIC
# MAGIC ✅ **RuleGenerator auto-creates** quality rules from profiles
# MAGIC
# MAGIC ✅ **Confidence thresholds** control rule generation sensitivity
# MAGIC
# MAGIC ✅ **Profile-based validation** catches data drift and anomalies
# MAGIC
# MAGIC ✅ **Rules can be customized** after generation for business needs
# MAGIC
# MAGIC ✅ **Save profiles and rules** for reuse and version control
# MAGIC
# MAGIC ✅ **Compare profiles over time** to detect data drift

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Profile representative data** - Use sufficient sample size
# MAGIC 2. **Review generated rules** - Don't blindly apply all suggestions
# MAGIC 3. **Combine with business rules** - Profile + domain knowledge
# MAGIC 4. **Version control profiles** - Track how data changes over time
# MAGIC 5. **Re-profile periodically** - Update rules as data evolves
# MAGIC 6. **Start with high confidence** - Lower threshold as you learn
# MAGIC 7. **Document customizations** - Explain why you modified rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Custom reactions** to failed checks (drop, mark, quarantine)
# MAGIC 2. **Quarantine strategies** for invalid data
# MAGIC 3. **Remediation workflows** for fixing data quality issues
# MAGIC 4. **Audit trails** for data quality tracking
# MAGIC
# MAGIC **Continue to**: `04_Demo_Custom_Reactions_to_Failed_Checks`
