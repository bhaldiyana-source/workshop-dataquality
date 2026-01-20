# Databricks notebook source
# MAGIC %md
# MAGIC # Row-Level and Column-Level Quality Rules
# MAGIC
# MAGIC **Module 3: Advanced Validation Rules**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
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

# Import required libraries
from dqx import Validator, CheckType, Rule
from pyspark.sql import functions as F
from datetime import datetime, timedelta

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

# Create validator with NOT NULL checks
validator = Validator()

# Critical columns that should never be null
validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error"
)

validator.add_check(
    name="name_not_null",
    check_type=CheckType.NOT_NULL,
    column="name",
    level="error"
)

validator.add_check(
    name="email_not_null",
    check_type=CheckType.NOT_NULL,
    column="email",
    level="error"
)

# Phone is optional, but flag if missing
validator.add_check(
    name="phone_not_null",
    check_type=CheckType.NOT_NULL,
    column="phone",
    level="warning"
)

# Run validation
result_df, summary = validator.validate(customers_df)

print(f"\nValidation Results:")
print(f"Pass Rate: {summary.get('pass_rate', 0):.2%}")
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

# Validate uniqueness
unique_validator = Validator()

unique_validator.add_check(
    name="customer_id_unique",
    check_type=CheckType.UNIQUE,
    column="customer_id",
    level="error"
)

unique_validator.add_check(
    name="email_unique",
    check_type=CheckType.UNIQUE,
    column="email",
    level="error"
)

dup_result_df, dup_summary = unique_validator.validate(dup_df)

print(f"\nUniqueness Validation Results:")
print(f"Pass Rate: {dup_summary.get('pass_rate', 0):.2%}")
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

# Add range checks
range_validator = Validator()

# Amount must be positive
range_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error"
)

# Discount must be non-negative
range_validator.add_check(
    name="discount_non_negative",
    check_type=CheckType.GREATER_THAN_OR_EQUAL,
    column="discount",
    threshold=0,
    level="error"
)

# Amount should be reasonable (less than 1M)
range_validator.add_check(
    name="amount_reasonable",
    check_type=CheckType.LESS_THAN,
    column="amount",
    threshold=1000000,
    level="warning"
)

# Discount should be between 0 and amount
range_validator.add_check(
    name="discount_range",
    check_type=CheckType.BETWEEN,
    column="discount",
    min_value=0,
    max_value=1000,
    level="warning"
)

txn_result_df, txn_summary = range_validator.validate(transactions_df)

print(f"\nRange Validation Results:")
print(f"Pass Rate: {txn_summary.get('pass_rate', 0):.2%}")
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

# Add set membership checks
set_validator = Validator()

# Valid order statuses
set_validator.add_check(
    name="valid_status",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "processing", "shipped", "completed", "cancelled"],
    level="error"
)

# Valid shipping methods
set_validator.add_check(
    name="valid_shipping",
    check_type=CheckType.IN_SET,
    column="shipping_method",
    valid_values=["standard", "express", "overnight"],
    level="error"
)

# Valid countries
set_validator.add_check(
    name="valid_country",
    check_type=CheckType.IN_SET,
    column="country",
    valid_values=["US", "CA", "UK", "AU", "DE", "FR"],
    level="warning"
)

order_result_df, order_summary = set_validator.validate(orders_df)

print(f"\nSet Membership Validation Results:")
print(f"Pass Rate: {order_summary.get('pass_rate', 0):.2%}")
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

# Add regex pattern checks
regex_validator = Validator()

# Email format
regex_validator.add_check(
    name="valid_email_format",
    check_type=CheckType.REGEX_MATCH,
    column="email",
    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    level="error"
)

# Phone format (XXX-XXX-XXXX)
regex_validator.add_check(
    name="valid_phone_format",
    check_type=CheckType.REGEX_MATCH,
    column="phone",
    pattern=r"^\d{3}-\d{3}-\d{4}$",
    level="warning"
)

# ZIP code (5 digits)
regex_validator.add_check(
    name="valid_zip_format",
    check_type=CheckType.REGEX_MATCH,
    column="zip_code",
    pattern=r"^\d{5}$",
    level="warning"
)

# URL format
regex_validator.add_check(
    name="valid_url_format",
    check_type=CheckType.REGEX_MATCH,
    column="website",
    pattern=r"^https?://[\w\.-]+\.\w+.*$",
    level="warning"
)

contact_result_df, contact_summary = regex_validator.validate(contacts_df)

print(f"\nPattern Validation Results:")
print(f"Pass Rate: {contact_summary.get('pass_rate', 0):.2%}")
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

# Add row-level rules using Rule class
row_validator = Validator()

# Discount should not exceed 50% of price
row_validator.add_rule(
    Rule(
        name="discount_validation",
        expression="discount <= price * 0.5",
        level="error",
        description="Discount cannot exceed 50% of price"
    )
)

# Net price should equal price minus discount
row_validator.add_rule(
    Rule(
        name="net_price_calculation",
        expression="net_price = price - discount",
        level="error",
        description="Net price must equal price minus discount"
    )
)

# Discount must be less than price
row_validator.add_rule(
    Rule(
        name="discount_less_than_price",
        expression="discount < price",
        level="error",
        description="Discount must be less than price"
    )
)

item_result_df, item_summary = row_validator.validate(items_df)

print(f"\nCross-Column Validation Results:")
print(f"Pass Rate: {item_summary.get('pass_rate', 0):.2%}")
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

# Add business logic rules
business_validator = Validator()

# Bonus should not exceed 20% of salary
business_validator.add_rule(
    Rule(
        name="bonus_cap",
        expression="bonus <= salary * 0.2",
        level="warning",
        description="Bonus should not exceed 20% of salary"
    )
)

# Total compensation = salary + bonus
business_validator.add_rule(
    Rule(
        name="compensation_calculation",
        expression="total_compensation = salary + bonus",
        level="error",
        description="Total compensation must equal salary plus bonus"
    )
)

# Role-based salary rules
business_validator.add_rule(
    Rule(
        name="intern_salary_cap",
        expression="role != 'Intern' OR salary <= 40000",
        level="error",
        description="Intern salary must not exceed 40,000"
    )
)

business_validator.add_rule(
    Rule(
        name="manager_minimum_salary",
        expression="role != 'Manager' OR salary >= 80000",
        level="warning",
        description="Manager salary should be at least 80,000"
    )
)

emp_result_df, emp_summary = business_validator.validate(employees_df)

print(f"\nBusiness Logic Validation Results:")
print(f"Pass Rate: {emp_summary.get('pass_rate', 0):.2%}")
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

# Add conditional rules
conditional_validator = Validator()

# Express shipping should cost at least 20% of order value for domestic
conditional_validator.add_rule(
    Rule(
        name="express_shipping_minimum",
        expression="shipping_type != 'express' OR country != 'US' OR shipping_cost >= order_value * 0.2",
        level="warning",
        description="Express domestic shipping should be at least 20% of order value"
    )
)

# Standard international shipping should be reasonable
conditional_validator.add_rule(
    Rule(
        name="international_shipping_cap",
        expression="shipping_type != 'standard' OR country = 'US' OR shipping_cost <= order_value * 0.3",
        level="warning",
        description="Standard international shipping should not exceed 30% of order value"
    )
)

# Overnight must have minimum shipping cost
conditional_validator.add_rule(
    Rule(
        name="overnight_minimum",
        expression="shipping_type != 'overnight' OR shipping_cost >= 30",
        level="error",
        description="Overnight shipping must cost at least $30"
    )
)

shipping_result_df, shipping_summary = conditional_validator.validate(shipping_df)

print(f"\nConditional Validation Results:")
print(f"Pass Rate: {shipping_summary.get('pass_rate', 0):.2%}")
display(shipping_result_df)

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

# Create comprehensive validator
comprehensive_validator = Validator()

# Column-level checks
comprehensive_validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error"
)

comprehensive_validator.add_check(
    name="transaction_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="transaction_id",
    level="error"
)

comprehensive_validator.add_check(
    name="amount_range_check",
    check_type=CheckType.BETWEEN,
    column="amount",
    min_value=0,
    max_value=1000000,
    level="error"
)

comprehensive_validator.add_check(
    name="status_valid_values",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "processing", "completed", "cancelled", "refunded"],
    level="error"
)

comprehensive_validator.add_check(
    name="payment_method_valid",
    check_type=CheckType.IN_SET,
    column="payment_method",
    valid_values=["credit_card", "debit_card", "paypal", "bank_transfer"],
    level="warning"
)

# Row-level checks
comprehensive_validator.add_rule(
    Rule(
        name="discount_validation",
        expression="discount <= amount * 0.5",
        level="error",
        description="Discount cannot exceed 50% of amount"
    )
)

comprehensive_validator.add_rule(
    Rule(
        name="discount_positive",
        expression="discount >= 0",
        level="error",
        description="Discount must be non-negative"
    )
)

comprehensive_validator.add_rule(
    Rule(
        name="high_value_transaction",
        expression="amount < 5000 OR payment_method IN ('credit_card', 'bank_transfer')",
        level="warning",
        description="High-value transactions should use secure payment methods"
    )
)

# Run comprehensive validation
comp_result_df, comp_summary = comprehensive_validator.validate(comprehensive_df)

print(f"\nComprehensive Validation Results:")
print(f"  Total Records: {comp_summary.get('total_records', 'N/A')}")
print(f"  Valid Records: {comp_summary.get('valid_records', 'N/A')}")
print(f"  Failed Records: {comp_summary.get('failed_records', 'N/A')}")
print(f"  Pass Rate: {comp_summary.get('pass_rate', 0):.2%}")

display(comp_result_df)

# COMMAND ----------

# Analyze failed checks in detail
print("Records with Errors:")
display(comp_result_df.filter("dqx_quality_status = 'FAILED'"))

# COMMAND ----------

print("Records with Warnings:")
display(comp_result_df.filter("dqx_quality_status = 'WARNING'"))

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
# MAGIC **Continue to**: `03_Demo_Data_Profiling_and_Auto_Generated_Rules`
