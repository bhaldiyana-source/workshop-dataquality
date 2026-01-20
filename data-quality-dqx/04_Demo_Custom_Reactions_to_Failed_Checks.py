# Databricks notebook source
# MAGIC %md
# MAGIC # Custom Reactions to Failed Checks
# MAGIC
# MAGIC **Module 5: Handling Data Quality Failures**
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
# MAGIC 1. **Configure DROP reactions** to remove invalid records
# MAGIC 2. **Implement MARK reactions** to flag bad data
# MAGIC 3. **Set up QUARANTINE reactions** to isolate failed records
# MAGIC 4. **Build remediation workflows** for fixing quarantined data
# MAGIC 5. **Track data quality** through the entire lifecycle

# COMMAND ----------

# Import required libraries
from dqx import Validator, CheckType, Reaction, QuarantineConfig
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Understanding Reactions
# MAGIC
# MAGIC DQX provides three main reaction types:
# MAGIC
# MAGIC | Reaction | Description | Use Case |
# MAGIC |----------|-------------|----------|
# MAGIC | **DROP** | Remove invalid records completely | Critical field violations |
# MAGIC | **MARK** | Add quality flag column(s) | Warnings, non-critical issues |
# MAGIC | **QUARANTINE** | Move to separate table | Review and remediation |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: DROP Reaction
# MAGIC
# MAGIC ### 2.1 When to Use DROP
# MAGIC
# MAGIC Use DROP when:
# MAGIC - Data is completely unusable
# MAGIC - Critical fields are missing or invalid
# MAGIC - Cannot proceed with bad records
# MAGIC
# MAGIC **Warning**: Dropped data is lost! Use carefully.

# COMMAND ----------

# Create sample transaction data
transaction_data = [
    (1, "TXN001", "CUST001", 1000.00, "2024-01-15", "completed"),
    (2, None, "CUST002", 500.00, "2024-01-16", "completed"),          # Issue: null transaction_id
    (3, "TXN003", None, 750.00, "2024-01-17", "completed"),           # Issue: null customer_id
    (4, "TXN004", "CUST004", -200.00, "2024-01-18", "completed"),     # Issue: negative amount
    (5, "TXN005", "CUST005", 1500.00, "2024-01-19", "completed"),
    (6, "TXN006", "CUST006", 0.00, "2024-01-20", "completed"),        # Issue: zero amount
    (7, "TXN007", "CUST007", 2000.00, "2024-01-21", "completed"),
]

transactions_df = spark.createDataFrame(
    transaction_data,
    ["id", "transaction_id", "customer_id", "amount", "date", "status"]
)

print(f"Original transaction count: {transactions_df.count()}")
display(transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Configure DROP Reactions

# COMMAND ----------

# Create validator with DROP reactions
drop_validator = Validator()

# Drop records with null transaction_id (critical field)
drop_validator.add_check(
    name="transaction_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="transaction_id",
    level="error",
    reaction=Reaction.DROP
)

# Drop records with null customer_id (critical field)
drop_validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error",
    reaction=Reaction.DROP
)

# Drop records with invalid amounts
drop_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error",
    reaction=Reaction.DROP
)

print("✅ Configured DROP reactions for critical validations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Execute DROP Validation

# COMMAND ----------

# Run validation with DROP reactions
clean_df, summary = drop_validator.validate(transactions_df)

print(f"\nValidation Summary:")
print(f"  Original Records: {transactions_df.count()}")
print(f"  Clean Records: {clean_df.count()}")
print(f"  Dropped Records: {transactions_df.count() - clean_df.count()}")
print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")

print("\n✅ Clean data (invalid records dropped):")
display(clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: MARK Reaction
# MAGIC
# MAGIC ### 3.1 When to Use MARK
# MAGIC
# MAGIC Use MARK when:
# MAGIC - Want to process all records but flag issues
# MAGIC - Issues are warnings, not errors
# MAGIC - Need downstream systems to see quality flags
# MAGIC - Want to track quality metrics

# COMMAND ----------

# Create customer data with various quality levels
customer_data = [
    (1, "CUST001", "John Doe", "john.doe@example.com", "123-456-7890", 25000.00),
    (2, "CUST002", "Jane Smith", "jane.smith@example.com", "234-567-8901", 35000.00),
    (3, "CUST003", "Bob Johnson", "bob@email", "345-678-9012", 18000.00),          # Issue: invalid email
    (4, "CUST004", "Alice Brown", "alice.b@example.com", "456-789", 22000.00),     # Issue: invalid phone
    (5, "CUST005", "Charlie Davis", "charlie.d@example.com", "567-890-1234", 150000.00),  # Issue: unusually high value
    (6, "CUST006", "Diana Evans", "diana.e@example.com", "678-901-2345", 28000.00),
]

customers_df = spark.createDataFrame(
    customer_data,
    ["id", "customer_id", "name", "email", "phone", "lifetime_value"]
)

print("Customer data:")
display(customers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Configure MARK Reactions

# COMMAND ----------

# Create validator with MARK reactions
mark_validator = Validator()

# Mark records with invalid email format
mark_validator.add_check(
    name="email_format_check",
    check_type=CheckType.REGEX_MATCH,
    column="email",
    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    level="warning",
    reaction=Reaction.MARK,
    mark_column="email_quality_flag"
)

# Mark records with invalid phone format
mark_validator.add_check(
    name="phone_format_check",
    check_type=CheckType.REGEX_MATCH,
    column="phone",
    pattern=r"^\d{3}-\d{3}-\d{4}$",
    level="warning",
    reaction=Reaction.MARK,
    mark_column="phone_quality_flag"
)

# Mark records with unusually high lifetime value
mark_validator.add_check(
    name="lifetime_value_outlier",
    check_type=CheckType.LESS_THAN,
    column="lifetime_value",
    threshold=100000,
    level="warning",
    reaction=Reaction.MARK,
    mark_column="value_quality_flag"
)

print("✅ Configured MARK reactions for quality flags")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Execute MARK Validation

# COMMAND ----------

# Run validation with MARK reactions
marked_df, mark_summary = mark_validator.validate(customers_df)

print(f"\nValidation Summary:")
print(f"  Total Records: {marked_df.count()}")
print(f"  Records with Warnings: {marked_df.filter('dqx_quality_status = \"WARNING\"').count()}")
print(f"  Pass Rate: {mark_summary.get('pass_rate', 0):.2%}")

print("\n✅ All records kept, with quality flags:")
display(marked_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Analyze Marked Records

# COMMAND ----------

# Filter records with email issues
email_issues = marked_df.filter("email_quality_flag = false")
print(f"Records with email format issues: {email_issues.count()}")
display(email_issues)

# COMMAND ----------

# Filter records with phone issues
phone_issues = marked_df.filter("phone_quality_flag = false")
print(f"Records with phone format issues: {phone_issues.count()}")
display(phone_issues)

# COMMAND ----------

# Filter records with lifetime value outliers
value_outliers = marked_df.filter("value_quality_flag = false")
print(f"Records with lifetime value outliers: {value_outliers.count()}")
display(value_outliers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: QUARANTINE Reaction
# MAGIC
# MAGIC ### 4.1 When to Use QUARANTINE
# MAGIC
# MAGIC Use QUARANTINE when:
# MAGIC - Need to review and fix bad data
# MAGIC - Want to track remediation progress
# MAGIC - Need audit trail of quality issues
# MAGIC - Plan to reprocess after fixes

# COMMAND ----------

# Create order data with various issues
order_data = [
    (1, "ORD001", "CUST001", 1000.00, 100.00, "completed", "2024-01-15"),
    (2, "ORD002", "CUST002", 500.00, 50.00, "completed", "2024-01-16"),
    (3, "ORD003", None, 750.00, 75.00, "completed", "2024-01-17"),           # Issue: null customer
    (4, "ORD004", "CUST004", -200.00, 20.00, "completed", "2024-01-18"),     # Issue: negative amount
    (5, "ORD005", "CUST005", 1500.00, 800.00, "completed", "2024-01-19"),    # Issue: discount > 50%
    (6, "ORD006", "CUST006", 2000.00, 200.00, "invalid_status", "2024-01-20"), # Issue: invalid status
    (7, "ORD007", "CUST007", 3000.00, 300.00, "completed", "2024-01-21"),
]

orders_df = spark.createDataFrame(
    order_data,
    ["id", "order_id", "customer_id", "amount", "discount", "status", "date"]
)

print("Order data:")
display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Configure QUARANTINE with QuarantineConfig

# COMMAND ----------

# Configure quarantine settings
quarantine_config = QuarantineConfig(
    target_table="dqx_demo.quarantined_orders",
    partition_by=["date"],
    add_metadata=True
)

print("✅ Quarantine configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Create Validator with QUARANTINE Reactions

# COMMAND ----------

# Create validator with QUARANTINE reactions
quarantine_validator = Validator()

# Quarantine records with null customer_id
quarantine_validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_config
)

# Quarantine records with invalid amounts
quarantine_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_config
)

# Quarantine records with excessive discounts
from dqx import Rule

quarantine_validator.add_rule(
    Rule(
        name="discount_cap",
        expression="discount <= amount * 0.5",
        level="error",
        description="Discount cannot exceed 50% of amount",
        reaction=Reaction.QUARANTINE,
        quarantine_config=quarantine_config
    )
)

# Quarantine records with invalid status
quarantine_validator.add_check(
    name="valid_status",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "processing", "completed", "cancelled"],
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_config
)

print("✅ Configured QUARANTINE reactions for data isolation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Execute QUARANTINE Validation

# COMMAND ----------

# Run validation with QUARANTINE reactions
clean_orders_df, quarantine_summary = quarantine_validator.validate(orders_df)

print(f"\nValidation Summary:")
print(f"  Original Records: {orders_df.count()}")
print(f"  Clean Records: {clean_orders_df.count()}")
print(f"  Quarantined Records: {quarantine_summary.get('quarantined_records', 0)}")
print(f"  Pass Rate: {quarantine_summary.get('pass_rate', 0):.2%}")

print("\n✅ Clean orders:")
display(clean_orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Query Quarantined Records

# COMMAND ----------

# Query the quarantine table
quarantined_df = spark.table("dqx_demo.quarantined_orders")

print(f"Quarantined records: {quarantined_df.count()}")
display(quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Analyze Quarantine with Metadata

# COMMAND ----------

# View quarantine metadata columns
print("Quarantine Metadata:")
quarantined_df.select(
    "order_id", 
    "dqx_quarantine_timestamp",
    "dqx_quarantine_reason",
    "dqx_failed_checks"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Advanced Quarantine Patterns
# MAGIC
# MAGIC ### 5.1 Quarantine with Enhanced Metadata

# COMMAND ----------

# Configure advanced quarantine with metadata enrichment
advanced_quarantine_config = QuarantineConfig(
    target_table="dqx_demo.quarantine_advanced",
    partition_by=["date", "quarantine_date"],
    add_metadata=True,
    metadata_columns={
        "quarantine_timestamp": "current_timestamp()",
        "data_source": "'orders_pipeline'",
        "environment": "'production'",
        "quarantine_date": "current_date()"
    }
)

# Create validator with enhanced quarantine
advanced_validator = Validator()

advanced_validator.add_check(
    name="customer_id_required",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=advanced_quarantine_config
)

# Run validation
adv_clean_df, adv_summary = advanced_validator.validate(orders_df)

print(f"✅ Advanced quarantine with metadata enrichment completed")
print(f"Quarantined: {adv_summary.get('quarantined_records', 0)} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Query Quarantine by Failed Check

# COMMAND ----------

# Group quarantined records by failed check
quarantine_by_check = (
    quarantined_df
    .groupBy("dqx_failed_checks")
    .agg(F.count("*").alias("record_count"))
    .orderBy(F.desc("record_count"))
)

print("Quarantined records by failed check:")
display(quarantine_by_check)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Remediation Workflow
# MAGIC
# MAGIC ### 6.1 Review Quarantined Data

# COMMAND ----------

# Load quarantined records for review
quarantine_review = spark.table("dqx_demo.quarantined_orders")

print(f"Total quarantined records to review: {quarantine_review.count()}")
display(quarantine_review)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Apply Fixes

# COMMAND ----------

# Simulate fixing quarantined records
def fix_quarantined_records(df):
    """Apply remediation logic to quarantined records"""
    
    # Fix null customer_ids by assigning default
    fixed_df = df.withColumn(
        "customer_id",
        F.when(F.col("customer_id").isNull(), F.lit("CUST_UNKNOWN"))
        .otherwise(F.col("customer_id"))
    )
    
    # Fix negative amounts by taking absolute value (example fix)
    fixed_df = fixed_df.withColumn(
        "amount",
        F.when(F.col("amount") < 0, F.abs(F.col("amount")))
        .otherwise(F.col("amount"))
    )
    
    # Fix invalid status
    fixed_df = fixed_df.withColumn(
        "status",
        F.when(~F.col("status").isin(["pending", "processing", "completed", "cancelled"]), 
               F.lit("pending"))
        .otherwise(F.col("status"))
    )
    
    # Cap discount at 50% of amount
    fixed_df = fixed_df.withColumn(
        "discount",
        F.when(F.col("discount") > F.col("amount") * 0.5, F.col("amount") * 0.5)
        .otherwise(F.col("discount"))
    )
    
    return fixed_df

# Apply fixes
remediated_df = fix_quarantined_records(quarantine_review)

print("✅ Remediation applied")
display(remediated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Re-validate Fixed Records

# COMMAND ----------

# Re-validate remediated records
revalidated_df, revalidation_summary = quarantine_validator.validate(remediated_df)

print(f"\nRe-validation Summary:")
print(f"  Total Records: {remediated_df.count()}")
print(f"  Valid Records: {revalidated_df.count()}")
print(f"  Still Failed: {remediated_df.count() - revalidated_df.count()}")
print(f"  Pass Rate: {revalidation_summary.get('pass_rate', 0):.2%}")

display(revalidated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Merge Back to Main Table

# COMMAND ----------

# Combine clean and remediated records
final_df = clean_orders_df.unionByName(revalidated_df)

print(f"Final dataset count: {final_df.count()}")
print(f"Original count: {orders_df.count()}")
print(f"Recovery rate: {(final_df.count() / orders_df.count()) * 100:.1f}%")

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Combining Reactions
# MAGIC
# MAGIC ### 7.1 Multi-Tier Reaction Strategy

# COMMAND ----------

# Create comprehensive dataset
comprehensive_data = [
    (1, "REC001", "CUST001", "john@example.com", 1000.00, "completed"),
    (2, None, "CUST002", "jane@example.com", 500.00, "completed"),           # DROP: null record_id
    (3, "REC003", None, "bob@example.com", 750.00, "completed"),             # QUARANTINE: null customer
    (4, "REC004", "CUST004", "invalid-email", 1500.00, "completed"),         # MARK: bad email
    (5, "REC005", "CUST005", "charlie@example.com", -200.00, "completed"),   # DROP: negative amount
    (6, "REC006", "CUST006", "diana@test", 2000.00, "invalid_status"),       # MARK + QUARANTINE
]

comprehensive_df = spark.createDataFrame(
    comprehensive_data,
    ["id", "record_id", "customer_id", "email", "amount", "status"]
)

print("Comprehensive dataset:")
display(comprehensive_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Configure Multi-Tier Reactions

# COMMAND ----------

# Create validator with multiple reaction types
multi_validator = Validator()

# Critical checks - DROP
multi_validator.add_check(
    name="record_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="record_id",
    level="error",
    reaction=Reaction.DROP
)

multi_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error",
    reaction=Reaction.DROP
)

# Important checks - QUARANTINE
quarantine_cfg = QuarantineConfig(
    target_table="dqx_demo.multi_quarantine",
    add_metadata=True
)

multi_validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_cfg
)

multi_validator.add_check(
    name="valid_status",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "completed", "cancelled"],
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_cfg
)

# Quality warnings - MARK
multi_validator.add_check(
    name="email_format",
    check_type=CheckType.REGEX_MATCH,
    column="email",
    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    level="warning",
    reaction=Reaction.MARK,
    mark_column="email_valid"
)

print("✅ Configured multi-tier reaction strategy")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Execute Multi-Tier Validation

# COMMAND ----------

# Run validation with multiple reactions
multi_result_df, multi_summary = multi_validator.validate(comprehensive_df)

print(f"\nMulti-Tier Validation Summary:")
print(f"  Original Records: {comprehensive_df.count()}")
print(f"  Clean Records: {multi_result_df.count()}")
print(f"  Dropped Records: {comprehensive_df.count() - multi_result_df.count() - multi_summary.get('quarantined_records', 0)}")
print(f"  Quarantined Records: {multi_summary.get('quarantined_records', 0)}")
print(f"  Marked Records: {multi_result_df.filter('email_valid = false').count()}")

print("\n✅ Final clean dataset:")
display(multi_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **DROP removes records** completely - use for critical failures
# MAGIC
# MAGIC ✅ **MARK adds quality flags** - use for warnings and tracking
# MAGIC
# MAGIC ✅ **QUARANTINE isolates data** - use for review and remediation
# MAGIC
# MAGIC ✅ **QuarantineConfig controls** where and how data is quarantined
# MAGIC
# MAGIC ✅ **Metadata enrichment** provides audit trail and context
# MAGIC
# MAGIC ✅ **Remediation workflows** can fix and reprocess quarantined data
# MAGIC
# MAGIC ✅ **Combine reactions** for comprehensive quality management

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Use DROP sparingly** - Data loss is permanent
# MAGIC 2. **QUARANTINE for recovery** - Always prefer over DROP when possible
# MAGIC 3. **MARK for transparency** - Let downstream know about quality
# MAGIC 4. **Add metadata** - Enrich quarantine with context
# MAGIC 5. **Monitor quarantine** - Set up alerts for high quarantine rates
# MAGIC 6. **Build remediation** - Have processes to fix quarantined data
# MAGIC 7. **Track metrics** - Monitor recovery rates and quality trends

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Streaming data validation** with real-time quality checks
# MAGIC 2. **Streaming quarantine** patterns
# MAGIC 3. **Real-time quality monitoring** and metrics
# MAGIC 4. **Handling late-arriving data** in streams
# MAGIC
# MAGIC **Continue to**: `05_Demo_Working_with_Spark_Structured_Streaming`
