# Databricks notebook source
# MAGIC %md
# MAGIC # Row-Level and Column-Level Quality Rules
# MAGIC
# MAGIC **Module 3: Advanced Validation Rules**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 15 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Define column-level rules** for individual field validation
# MAGIC 2. **Implement row-level rules** for cross-column business logic
# MAGIC 3. **Use various check types** (NOT NULL, UNIQUE, BETWEEN, IN_SET, REGEX)
# MAGIC 4. **Create custom expressions** for complex validations
# MAGIC 5. **Combine multiple rules** effectively

# COMMAND ----------

# DBTITLE 1,Cell 3
# Import required libraries
from pyspark.sql import functions as F
from datetime import datetime, timedelta

print("✅ Libraries imported successfully!")
print("Using PySpark for data quality validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Column-Level Rules
# MAGIC
# MAGIC Column-level rules validate individual columns independently.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 NOT NULL Checks
# MAGIC
# MAGIC Ensure critical columns have no null values:

# COMMAND ----------

# Create sample customer data
customer_data = [
    (1, "CUST001", "John Doe", "john@example.com", "123-456-7890", "Active"),
    (2, "CUST002", "Jane Smith", "jane@example.com", None, "Active"),           # Issue: null phone
    (3, "CUST003", None, "bob@example.com", "234-567-8901", "Active"),          # Issue: null name
    (4, "CUST004", "Alice Brown", "alice@example.com", "345-678-9012", "Active"),
    (5, None, "Charlie Davis", "charlie@example.com", "456-789-0123", "Active"), # Issue: null customer_id
]

customers_df = spark.createDataFrame(
    customer_data,
    ["id", "customer_id", "name", "email", "phone", "status"]
)

print("Sample Customer Data:")
display(customers_df)

# COMMAND ----------

# DBTITLE 1,Cell 7
# Apply NOT NULL checks using PySpark
result_df = customers_df.withColumn(
    "check_customer_id_not_null",
    F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_name_not_null",
    F.when(F.col("name").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_email_not_null",
    F.when(F.col("email").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_phone_not_null",
    F.when(F.col("phone").isNull(), F.lit("WARNING")).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_customer_id_not_null") == "FAILED") | 
        (F.col("check_name_not_null") == "FAILED") |
        (F.col("check_email_not_null") == "FAILED"),
        F.lit("FAILED")
    ).when(
        F.col("check_phone_not_null") == "WARNING",
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
total_records = result_df.count()
valid_records = result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
failed_records = result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
warning_records = result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
pass_rate = valid_records / total_records if total_records > 0 else 0

print(f"\nValidation Results:")
print(f"Pass Rate: {pass_rate:.2%}")
print(f"Total: {total_records}, Valid: {valid_records}, Failed: {failed_records}, Warnings: {warning_records}")
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 UNIQUE Checks
# MAGIC
# MAGIC Ensure columns that should be unique contain no duplicates:

# COMMAND ----------

# Create data with duplicates
duplicate_data = [
    (1, "CUST001", "john@example.com"),
    (2, "CUST002", "jane@example.com"),
    (3, "CUST001", "bob@example.com"),      # Issue: duplicate customer_id
    (4, "CUST004", "alice@example.com"),
    (5, "CUST005", "john@example.com"),     # Issue: duplicate email
]

dup_df = spark.createDataFrame(duplicate_data, ["id", "customer_id", "email"])

print("Data with Duplicates:")
display(dup_df)

# COMMAND ----------

# DBTITLE 1,Cell 10
# Validate uniqueness using window functions
from pyspark.sql.window import Window

# Count occurrences of each customer_id and email
dup_result_df = dup_df.withColumn(
    "customer_id_count",
    F.count("customer_id").over(Window.partitionBy("customer_id"))
).withColumn(
    "email_count",
    F.count("email").over(Window.partitionBy("email"))
).withColumn(
    "check_customer_id_unique",
    F.when(F.col("customer_id_count") > 1, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_email_unique",
    F.when(F.col("email_count") > 1, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_customer_id_unique") == "FAILED") | 
        (F.col("check_email_unique") == "FAILED"),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
).drop("customer_id_count", "email_count")

# Calculate summary
dup_total = dup_result_df.count()
dup_valid = dup_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
dup_failed = dup_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
dup_pass_rate = dup_valid / dup_total if dup_total > 0 else 0

print(f"\nUniqueness Validation Results:")
print(f"Pass Rate: {dup_pass_rate:.2%}")
print(f"Total: {dup_total}, Valid: {dup_valid}, Failed: {dup_failed}")
display(dup_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Range Checks (GREATER_THAN, LESS_THAN, BETWEEN)
# MAGIC
# MAGIC Validate numeric ranges:

# COMMAND ----------

# Create transaction data with range issues
transaction_data = [
    (1, "TXN001", 1500.00, 100.00, "2024-01-15"),
    (2, "TXN002", -250.00, 50.00, "2024-01-16"),    # Issue: negative amount
    (3, "TXN003", 3000.00, 1600.00, "2024-01-17"),  # Issue: discount > amount
    (4, "TXN004", 750.00, 75.00, "2024-01-18"),
    (5, "TXN005", 2000000.00, 200.00, "2024-01-19"), # Issue: suspiciously high amount
]

transactions_df = spark.createDataFrame(
    transaction_data,
    ["id", "transaction_id", "amount", "discount", "date"]
)

print("Transaction Data:")
display(transactions_df)

# COMMAND ----------

# DBTITLE 1,Cell 13
# Add range checks using PySpark
txn_result_df = transactions_df.withColumn(
    "check_amount_positive",
    F.when(F.col("amount") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_discount_non_negative",
    F.when(F.col("discount") < 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_amount_reasonable",
    F.when(F.col("amount") >= 1000000, F.lit("WARNING")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_discount_range",
    F.when(
        (F.col("discount") < 0) | (F.col("discount") > 1000),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_amount_positive") == "FAILED") | 
        (F.col("check_discount_non_negative") == "FAILED"),
        F.lit("FAILED")
    ).when(
        (F.col("check_amount_reasonable") == "WARNING") |
        (F.col("check_discount_range") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
txn_total = txn_result_df.count()
txn_valid = txn_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
txn_failed = txn_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
txn_warning = txn_result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
txn_pass_rate = txn_valid / txn_total if txn_total > 0 else 0

print(f"\nRange Validation Results:")
print(f"Pass Rate: {txn_pass_rate:.2%}")
print(f"Total: {txn_total}, Valid: {txn_valid}, Failed: {txn_failed}, Warnings: {txn_warning}")
display(txn_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Set Membership Checks (IN_SET)
# MAGIC
# MAGIC Ensure values belong to allowed set:

# COMMAND ----------

# Create order data with invalid statuses
order_data = [
    (1, "ORD001", "pending", "standard", "US"),
    (2, "ORD002", "completed", "express", "CA"),
    (3, "ORD003", "shipped", "overnight", "UK"),
    (4, "ORD004", "invalid_status", "standard", "US"),    # Issue: invalid status
    (5, "ORD005", "completed", "super_fast", "CA"),       # Issue: invalid shipping
    (6, "ORD006", "cancelled", "standard", "XX"),         # Issue: invalid country
]

orders_df = spark.createDataFrame(
    order_data,
    ["id", "order_id", "status", "shipping_method", "country"]
)

print("Order Data:")
display(orders_df)

# COMMAND ----------

# DBTITLE 1,Cell 16
# Add set membership checks using PySpark
valid_statuses = ["pending", "processing", "shipped", "completed", "cancelled"]
valid_shipping = ["standard", "express", "overnight"]
valid_countries = ["US", "CA", "UK", "AU", "DE", "FR"]

order_result_df = orders_df.withColumn(
    "check_valid_status",
    F.when(F.col("status").isin(valid_statuses), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).withColumn(
    "check_valid_shipping",
    F.when(F.col("shipping_method").isin(valid_shipping), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).withColumn(
    "check_valid_country",
    F.when(F.col("country").isin(valid_countries), F.lit("PASSED")).otherwise(F.lit("WARNING"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_valid_status") == "FAILED") | 
        (F.col("check_valid_shipping") == "FAILED"),
        F.lit("FAILED")
    ).when(
        F.col("check_valid_country") == "WARNING",
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
order_total = order_result_df.count()
order_valid = order_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
order_failed = order_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
order_warning = order_result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
order_pass_rate = order_valid / order_total if order_total > 0 else 0

print(f"\nSet Membership Validation Results:")
print(f"Pass Rate: {order_pass_rate:.2%}")
print(f"Total: {order_total}, Valid: {order_valid}, Failed: {order_failed}, Warnings: {order_warning}")
display(order_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.5 Pattern Matching (REGEX_MATCH)
# MAGIC
# MAGIC Validate string patterns using regular expressions:

# COMMAND ----------

# Create contact data with format issues
contact_data = [
    (1, "john.doe@example.com", "123-456-7890", "12345", "http://example.com"),
    (2, "invalid-email", "234-567-8901", "67890", "https://test.com"),           # Issue: invalid email
    (3, "jane@example.com", "345-678", "ABCDE", "ftp://files.com"),              # Issue: invalid phone & zip
    (4, "bob@test.com", "456-789-0123", "54321", "not-a-url"),                   # Issue: invalid URL
    (5, "alice@example.com", "567-890-1234", "98765", "https://valid.com"),
]

contacts_df = spark.createDataFrame(
    contact_data,
    ["id", "email", "phone", "zip_code", "website"]
)

print("Contact Data:")
display(contacts_df)

# COMMAND ----------

# DBTITLE 1,Cell 19
# Add regex pattern checks using PySpark
email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
phone_pattern = r"^\d{3}-\d{3}-\d{4}$"
zip_pattern = r"^\d{5}$"
url_pattern = r"^https?://[\w\.-]+\.\w+.*$"

contact_result_df = contacts_df.withColumn(
    "check_valid_email_format",
    F.when(F.col("email").rlike(email_pattern), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).withColumn(
    "check_valid_phone_format",
    F.when(F.col("phone").rlike(phone_pattern), F.lit("PASSED")).otherwise(F.lit("WARNING"))
).withColumn(
    "check_valid_zip_format",
    F.when(F.col("zip_code").rlike(zip_pattern), F.lit("PASSED")).otherwise(F.lit("WARNING"))
).withColumn(
    "check_valid_url_format",
    F.when(F.col("website").rlike(url_pattern), F.lit("PASSED")).otherwise(F.lit("WARNING"))
).withColumn(
    "dqx_quality_status",
    F.when(
        F.col("check_valid_email_format") == "FAILED",
        F.lit("FAILED")
    ).when(
        (F.col("check_valid_phone_format") == "WARNING") |
        (F.col("check_valid_zip_format") == "WARNING") |
        (F.col("check_valid_url_format") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
contact_total = contact_result_df.count()
contact_valid = contact_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
contact_failed = contact_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
contact_warning = contact_result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
contact_pass_rate = contact_valid / contact_total if contact_total > 0 else 0

print(f"\nPattern Validation Results:")
print(f"Pass Rate: {contact_pass_rate:.2%}")
print(f"Total: {contact_total}, Valid: {contact_valid}, Failed: {contact_failed}, Warnings: {contact_warning}")
display(contact_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Row-Level Rules
# MAGIC
# MAGIC Row-level rules validate business logic across multiple columns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Cross-Column Validations
# MAGIC
# MAGIC Validate relationships between columns:

# COMMAND ----------

# Create data with cross-column issues
cross_column_data = [
    (1, "Item A", 100.00, 10.00, 90.00),
    (2, "Item B", 200.00, 50.00, 150.00),
    (3, "Item C", 150.00, 80.00, 70.00),      # Issue: discount > 50%
    (4, "Item D", 300.00, 200.00, 120.00),    # Issue: net_price != price - discount
    (5, "Item E", 250.00, 25.00, 225.00),
]

items_df = spark.createDataFrame(
    cross_column_data,
    ["id", "name", "price", "discount", "net_price"]
)

print("Items Data:")
display(items_df)

# COMMAND ----------

# DBTITLE 1,Cell 23
# Add row-level rules using PySpark expressions
item_result_df = items_df.withColumn(
    "check_discount_validation",
    F.when(F.col("discount") > F.col("price") * 0.5, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_net_price_calculation",
    F.when(
        F.abs(F.col("net_price") - (F.col("price") - F.col("discount"))) > 0.01,
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "check_discount_less_than_price",
    F.when(F.col("discount") >= F.col("price"), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_discount_validation") == "FAILED") | 
        (F.col("check_net_price_calculation") == "FAILED") |
        (F.col("check_discount_less_than_price") == "FAILED"),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
item_total = item_result_df.count()
item_valid = item_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
item_failed = item_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
item_pass_rate = item_valid / item_total if item_total > 0 else 0

print(f"\nCross-Column Validation Results:")
print(f"Pass Rate: {item_pass_rate:.2%}")
print(f"Total: {item_total}, Valid: {item_valid}, Failed: {item_failed}")
print(f"\nRule Descriptions:")
print("  - Discount cannot exceed 50% of price")
print("  - Net price must equal price minus discount")
print("  - Discount must be less than price")
display(item_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Business Logic Rules
# MAGIC
# MAGIC Implement complex business validation:

# COMMAND ----------

# Create employee data with business rule violations
employee_data = [
    (1, "John Doe", "Manager", 85000, 5000, 90000),
    (2, "Jane Smith", "Engineer", 75000, 8000, 83000),
    (3, "Bob Johnson", "Manager", 90000, 50000, 140000),   # Issue: bonus > 20% of salary
    (4, "Alice Brown", "Intern", 35000, 5000, 40000),      # Issue: intern salary > 40k
    (5, "Charlie Davis", "Director", 120000, 15000, 135000),
    (6, "Eve Wilson", "Engineer", 80000, 10000, 85000),    # Issue: total != salary + bonus
]

employees_df = spark.createDataFrame(
    employee_data,
    ["id", "name", "role", "salary", "bonus", "total_compensation"]
)

print("Employee Data:")
display(employees_df)

# COMMAND ----------

# DBTITLE 1,Cell 26
# Add business logic validation rules using PySpark
emp_result_df = employees_df.withColumn(
    "check_bonus_cap",
    F.when(F.col("bonus") > F.col("salary") * 0.2, F.lit("WARNING")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_compensation_calculation",
    F.when(
        F.abs(F.col("total_compensation") - (F.col("salary") + F.col("bonus"))) > 0.01,
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "check_intern_salary_cap",
    F.when(
        (F.col("role") == "Intern") & (F.col("salary") > 40000),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "check_manager_minimum_salary",
    F.when(
        (F.col("role") == "Manager") & (F.col("salary") < 80000),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_compensation_calculation") == "FAILED") | 
        (F.col("check_intern_salary_cap") == "FAILED"),
        F.lit("FAILED")
    ).when(
        (F.col("check_bonus_cap") == "WARNING") |
        (F.col("check_manager_minimum_salary") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
emp_total = emp_result_df.count()
emp_valid = emp_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
emp_failed = emp_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
emp_warning = emp_result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
emp_pass_rate = emp_valid / emp_total if emp_total > 0 else 0

print(f"\nBusiness Logic Validation Results:")
print(f"Pass Rate: {emp_pass_rate:.2%}")
print(f"Total: {emp_total}, Valid: {emp_valid}, Failed: {emp_failed}, Warnings: {emp_warning}")
print(f"\nRules Applied:")
print("  - Bonus cannot exceed 20% of salary (warning)")
print("  - Total compensation must equal salary + bonus (error)")
print("  - Intern salary must be <= $40,000 (error)")
print("  - Manager salary must be >= $80,000 (warning)")
display(emp_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Conditional Checks
# MAGIC
# MAGIC Apply validations based on conditions:

# COMMAND ----------

# Create order data with conditional rules
conditional_data = [
    (1, "ORD001", "express", 50.00, 25.00, "US"),
    (2, "ORD002", "standard", 30.00, 5.00, "US"),
    (3, "ORD003", "express", 40.00, 5.00, "CA"),     # Issue: express shipping too cheap
    (4, "ORD004", "standard", 100.00, 15.00, "UK"),  # Issue: international standard too expensive
    (5, "ORD005", "overnight", 75.00, 35.00, "US"),
]

shipping_df = spark.createDataFrame(
    conditional_data,
    ["id", "order_id", "shipping_type", "order_value", "shipping_cost", "country"]
)

print("Shipping Data:")
display(shipping_df)

# COMMAND ----------

# DBTITLE 1,Cell 29
# Add conditional validation rules using PySpark
ship_result_df = shipping_df.withColumn(
    "check_express_shipping_minimum",
    F.when(
        (F.col("shipping_type") == "express") & 
        (F.col("country") == "US") & 
        (F.col("shipping_cost") < F.col("order_value") * 0.2),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "check_international_shipping_cap",
    F.when(
        (F.col("shipping_type") == "standard") & 
        (F.col("country") != "US") & 
        (F.col("shipping_cost") > F.col("order_value") * 0.3),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "check_overnight_minimum",
    F.when(
        (F.col("shipping_type") == "overnight") & 
        (F.col("shipping_cost") < 30),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        F.col("check_overnight_minimum") == "FAILED",
        F.lit("FAILED")
    ).when(
        (F.col("check_express_shipping_minimum") == "WARNING") |
        (F.col("check_international_shipping_cap") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate summary
ship_total = ship_result_df.count()
ship_valid = ship_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
ship_failed = ship_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
ship_warning = ship_result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
ship_pass_rate = ship_valid / ship_total if ship_total > 0 else 0

print(f"\nConditional Validation Results:")
print(f"Pass Rate: {ship_pass_rate:.2%}")
print(f"Total: {ship_total}, Valid: {ship_valid}, Failed: {ship_failed}, Warnings: {ship_warning}")
print(f"\nConditional Rules:")
print("  - Express US shipping must be >= 20% of order value (warning)")
print("  - Standard international shipping must be <= 30% of order value (warning)")
print("  - Overnight shipping must be >= $30 (error)")
display(ship_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Comprehensive Example
# MAGIC
# MAGIC Combining column-level and row-level rules:

# COMMAND ----------

# Create comprehensive transaction dataset
comprehensive_data = [
    (1, "CUST001", "TXN001", 1000.00, 100.00, "completed", "2024-01-15", "credit_card"),
    (2, "CUST002", None, 500.00, 50.00, "completed", "2024-01-16", "paypal"),           # Issue: null txn_id
    (3, "CUST003", "TXN003", -200.00, 20.00, "completed", "2024-01-17", "credit_card"), # Issue: negative amount
    (4, "CUST004", "TXN004", 1500.00, 800.00, "pending", "2024-01-18", "credit_card"),  # Issue: discount > 50%
    (5, None, "TXN005", 750.00, 75.00, "completed", "2024-01-19", "debit_card"),        # Issue: null customer
    (6, "CUST006", "TXN006", 2000.00, 200.00, "invalid", "2024-01-20", "credit_card"),  # Issue: invalid status
    (7, "CUST007", "TXN007", 3000.00, 150.00, "completed", "2024-01-21", "bitcoin"),    # Issue: invalid payment
]

comprehensive_df = spark.createDataFrame(
    comprehensive_data,
    ["id", "customer_id", "transaction_id", "amount", "discount", "status", "date", "payment_method"]
)

print("Comprehensive Transaction Data:")
display(comprehensive_df)

# COMMAND ----------

# DBTITLE 1,Cell 32
# Create comprehensive validator combining column and row-level checks
valid_statuses = ["pending", "processing", "completed", "cancelled"]
valid_payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]

comp_result_df = comprehensive_df.withColumn(
    "check_customer_id_not_null",
    F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_transaction_id_not_null",
    F.when(F.col("transaction_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_amount_range",
    F.when(
        (F.col("amount") < 0) | (F.col("amount") > 100000),
        F.lit("FAILED")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "check_valid_status",
    F.when(F.col("status").isin(valid_statuses), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).withColumn(
    "check_valid_payment_method",
    F.when(F.col("payment_method").isin(valid_payment_methods), F.lit("PASSED")).otherwise(F.lit("FAILED"))
).withColumn(
    "check_discount_validation",
    F.when(F.col("discount") > F.col("amount") * 0.3, F.lit("WARNING")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_discount_positive",
    F.when(F.col("discount") < 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
).withColumn(
    "check_high_value_transaction",
    F.when(
        (F.col("amount") > 50000) & (F.col("payment_method") == "paypal"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
).withColumn(
    "dqx_quality_status",
    F.when(
        (F.col("check_customer_id_not_null") == "FAILED") | 
        (F.col("check_transaction_id_not_null") == "FAILED") |
        (F.col("check_amount_range") == "FAILED") |
        (F.col("check_valid_status") == "FAILED") |
        (F.col("check_valid_payment_method") == "FAILED") |
        (F.col("check_discount_positive") == "FAILED"),
        F.lit("FAILED")
    ).when(
        (F.col("check_discount_validation") == "WARNING") |
        (F.col("check_high_value_transaction") == "WARNING"),
        F.lit("WARNING")
    ).otherwise(F.lit("PASSED"))
)

# Calculate comprehensive summary
comp_total = comp_result_df.count()
comp_valid = comp_result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
comp_failed = comp_result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
comp_warning = comp_result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
comp_pass_rate = comp_valid / comp_total if comp_total > 0 else 0

print(f"\n=== Comprehensive Validation Summary ===")
print(f"Pass Rate: {comp_pass_rate:.2%}")
print(f"Total Records: {comp_total}")
print(f"Valid Records: {comp_valid}")
print(f"Failed Records: {comp_failed}")
print(f"Warning Records: {comp_warning}")
print(f"\nColumn-Level Checks:")
print("  - Customer ID not null (error)")
print("  - Transaction ID not null (error)")
print("  - Amount between 0 and 100,000 (error)")
print("  - Valid status values (error)")
print("  - Valid payment methods (error)")
print(f"\nRow-Level Rules:")
print("  - Discount <= 30% of amount (warning)")
print("  - Discount must be positive (error)")
print("  - High-value PayPal transactions flagged (warning)")

display(comp_result_df)

# COMMAND ----------

# DBTITLE 1,Cell 33
# Show only failed records
failed_records = comp_result_df.filter(F.col("dqx_quality_status") == "FAILED")

print(f"\nFailed Records: {failed_records.count()}")
print("These records have critical data quality issues that must be addressed.\n")
display(failed_records)

# COMMAND ----------

# DBTITLE 1,Cell 34
# Show only warning records
warning_records = comp_result_df.filter(F.col("dqx_quality_status") == "WARNING")

print(f"\nWarning Records: {warning_records.count()}")
print("These records have potential issues that should be reviewed.\n")
display(warning_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Column-level rules** validate individual fields (NOT NULL, UNIQUE, BETWEEN, IN_SET, REGEX)
# MAGIC
# MAGIC ✅ **Row-level rules** validate business logic across columns using expressions
# MAGIC
# MAGIC ✅ **Use Rule class** for custom SQL expressions in row-level checks
# MAGIC
# MAGIC ✅ **Combine multiple check types** for comprehensive validation
# MAGIC
# MAGIC ✅ **Choose appropriate levels** - error for critical, warning for non-critical
# MAGIC
# MAGIC ✅ **Conditional rules** enable complex business logic validation
# MAGIC
# MAGIC ✅ **Name your checks** for easier debugging and reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Data profiling** to understand your data
# MAGIC 2. **Auto-generating quality rules** from profiles
# MAGIC 3. **Iterative rule refinement** based on data patterns
# MAGIC 4. **Profile-based validation** strategies
# MAGIC
# MAGIC **Continue to**: `03_Data_Profiling_and_Auto_Generated_Rules`
