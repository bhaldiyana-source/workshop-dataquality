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

# DBTITLE 1,Cell 3
# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

print("✅ Libraries imported successfully!")
print("Using PySpark for data quality validation with custom reactions")

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

# DBTITLE 1,Cell 8
# Create validator function with DROP reaction
# DROP reaction: removes invalid records from the result

def create_drop_validator():
    """
    Creates a validator that DROPS (removes) invalid records.
    Returns a function that validates and filters out bad data.
    """
    def validate(df):
        result_df = df
        
        # Check 1: transaction_id NOT NULL
        result_df = result_df.withColumn(
            "check_transaction_id_not_null",
            F.when(F.col("transaction_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
        )
        
        # Check 2: customer_id NOT NULL
        result_df = result_df.withColumn(
            "check_customer_id_not_null",
            F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
        )
        
        # Check 3: amount > 0
        result_df = result_df.withColumn(
            "check_amount_positive",
            F.when(F.col("amount") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
        )
        
        # Add overall quality status
        result_df = result_df.withColumn(
            "dqx_quality_status",
            F.when(
                (F.col("check_transaction_id_not_null") == "FAILED") | 
                (F.col("check_customer_id_not_null") == "FAILED") |
                (F.col("check_amount_positive") == "FAILED"),
                F.lit("FAILED")
            ).otherwise(F.lit("PASSED"))
        )
        
        # Calculate summary before dropping
        total_records = result_df.count()
        failed_records = result_df.filter(F.col("dqx_quality_status") == "FAILED").count()
        
        # DROP reaction: filter out failed records
        clean_df = result_df.filter(F.col("dqx_quality_status") == "PASSED")
        
        # Remove check columns from final output
        check_columns = [c for c in clean_df.columns if c.startswith('check_') or c == 'dqx_quality_status']
        for col in check_columns:
            clean_df = clean_df.drop(col)
        
        valid_records = clean_df.count()
        pass_rate = valid_records / total_records if total_records > 0 else 0
        
        summary = {
            'total_records': total_records,
            'valid_records': valid_records,
            'failed_records': failed_records,
            'pass_rate': pass_rate
        }
        
        return clean_df, summary
    
    return validate

# Create the DROP validator
drop_validator = create_drop_validator()

print("✅ Configured DROP reactions for critical validations")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Execute DROP Validation

# COMMAND ----------

# DBTITLE 1,Cell 10
# Run validation with DROP reactions
clean_df, summary = drop_validator(transactions_df)

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

# DBTITLE 1,Cell 14
# Create validator function with MARK reaction
# MARK reaction: keeps all records but adds quality flag columns

def create_mark_validator():
    """
    Creates a validator that MARKS records with quality flags.
    Returns a function that validates and adds flag columns.
    """
    def validate(df):
        result_df = df
        
        # Email format pattern
        email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
        
        # Check 1: email format - add email_quality_flag column
        result_df = result_df.withColumn(
            "email_quality_flag",
            F.when(F.col("email").rlike(email_pattern), F.lit(True)).otherwise(F.lit(False))
        )
        
        # Phone format pattern
        phone_pattern = r"^\d{3}-\d{3}-\d{4}$"
        
        # Check 2: phone format - add phone_quality_flag column
        result_df = result_df.withColumn(
            "phone_quality_flag",
            F.when(F.col("phone").rlike(phone_pattern), F.lit(True)).otherwise(F.lit(False))
        )
        
        # Check 3: lifetime value outlier - add value_quality_flag column
        result_df = result_df.withColumn(
            "value_quality_flag",
            F.when(F.col("lifetime_value") < 100000, F.lit(True)).otherwise(F.lit(False))
        )
        
        # Add overall quality status
        result_df = result_df.withColumn(
            "dqx_quality_status",
            F.when(
                (F.col("email_quality_flag") == False) |
                (F.col("phone_quality_flag") == False) |
                (F.col("value_quality_flag") == False),
                F.lit("WARNING")
            ).otherwise(F.lit("PASSED"))
        )
        
        # Calculate summary
        total_records = result_df.count()
        valid_records = result_df.filter(F.col("dqx_quality_status") == "PASSED").count()
        warning_records = result_df.filter(F.col("dqx_quality_status") == "WARNING").count()
        pass_rate = valid_records / total_records if total_records > 0 else 0
        
        summary = {
            'total_records': total_records,
            'valid_records': valid_records,
            'warning_records': warning_records,
            'pass_rate': pass_rate
        }
        
        return result_df, summary
    
    return validate

# Create the MARK validator
mark_validator = create_mark_validator()

print("✅ Configured MARK reactions for quality flags")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Execute MARK Validation

# COMMAND ----------

# DBTITLE 1,Cell 16
# Run validation with MARK reactions
marked_df, mark_summary = mark_validator(customers_df)

print(f"\nValidation Summary:")
print(f"  Total Records: {marked_df.count()}")
print(f"  Records with Warnings: {marked_df.filter(F.col('dqx_quality_status') == 'WARNING').count()}")
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

# DBTITLE 1,Cell 24
# Configure quarantine settings
# QuarantineConfig specifies where and how to store quarantined records

quarantine_config = {
    'target_table': 'dqx_demo.quarantined_orders',
    'partition_by': ['date'],
    'add_metadata': True
}

print("✅ Quarantine configuration created")
print(f"  Target table: {quarantine_config['target_table']}")
print(f"  Partition by: {quarantine_config['partition_by']}")
print(f"  Add metadata: {quarantine_config['add_metadata']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Create Validator with QUARANTINE Reactions

# COMMAND ----------

# DBTITLE 1,Cell 26
# Create validator function with QUARANTINE reaction
# QUARANTINE reaction: separates invalid records to a quarantine table

def create_quarantine_validator(quarantine_config):
    """
    Creates a validator that QUARANTINES invalid records.
    Returns a function that validates and separates bad data.
    """
    def validate(df):
        result_df = df
        
        # Check 1: customer_id NOT NULL
        result_df = result_df.withColumn(
            "check_customer_id_not_null",
            F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
        )
        
        # Check 2: amount > 0
        result_df = result_df.withColumn(
            "check_amount_positive",
            F.when(F.col("amount") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
        )
        
        # Check 3: discount <= amount * 0.5 (row-level rule)
        result_df = result_df.withColumn(
            "check_discount_cap",
            F.when(F.col("discount") > F.col("amount") * 0.5, F.lit("FAILED")).otherwise(F.lit("PASSED"))
        )
        
        # Check 4: status IN valid set
        valid_statuses = ["pending", "processing", "completed", "cancelled"]
        result_df = result_df.withColumn(
            "check_valid_status",
            F.when(F.col("status").isin(valid_statuses), F.lit("PASSED")).otherwise(F.lit("FAILED"))
        )
        
        # Collect failed check names for each record
        result_df = result_df.withColumn(
            "dqx_failed_checks",
            F.concat_ws(", ",
                F.when(F.col("check_customer_id_not_null") == "FAILED", F.lit("customer_id_not_null")),
                F.when(F.col("check_amount_positive") == "FAILED", F.lit("amount_positive")),
                F.when(F.col("check_discount_cap") == "FAILED", F.lit("discount_cap")),
                F.when(F.col("check_valid_status") == "FAILED", F.lit("valid_status"))
            )
        )
        
        # Add overall quality status
        result_df = result_df.withColumn(
            "dqx_quality_status",
            F.when(
                (F.col("check_customer_id_not_null") == "FAILED") | 
                (F.col("check_amount_positive") == "FAILED") |
                (F.col("check_discount_cap") == "FAILED") |
                (F.col("check_valid_status") == "FAILED"),
                F.lit("FAILED")
            ).otherwise(F.lit("PASSED"))
        )
        
        # Add quarantine metadata if configured
        if quarantine_config.get('add_metadata', False):
            result_df = result_df.withColumn(
                "dqx_quarantine_timestamp",
                F.lit(datetime.now().isoformat())
            ).withColumn(
                "dqx_quarantine_reason",
                F.when(F.col("dqx_quality_status") == "FAILED", F.col("dqx_failed_checks")).otherwise(F.lit(None))
            )
        
        # Separate clean and quarantined records
        clean_df = result_df.filter(F.col("dqx_quality_status") == "PASSED")
        quarantined_df = result_df.filter(F.col("dqx_quality_status") == "FAILED")
        
        # Remove check columns from clean output
        check_columns = [c for c in clean_df.columns if c.startswith('check_') or c.startswith('dqx_')]
        for col in check_columns:
            clean_df = clean_df.drop(col)
        
        # Calculate summary
        total_records = result_df.count()
        valid_records = clean_df.count()
        quarantined_records = quarantined_df.count()
        pass_rate = valid_records / total_records if total_records > 0 else 0
        
        summary = {
            'total_records': total_records,
            'valid_records': valid_records,
            'quarantined_records': quarantined_records,
            'pass_rate': pass_rate
        }
        
        return clean_df, quarantined_df, summary
    
    return validate

# Create the QUARANTINE validator
quarantine_validator = create_quarantine_validator(quarantine_config)

print("✅ Configured QUARANTINE reactions for data isolation")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Execute QUARANTINE Validation

# COMMAND ----------

# DBTITLE 1,Cell 28
# Run validation with QUARANTINE reactions
clean_orders_df, quarantined_orders_df, quarantine_summary = quarantine_validator(orders_df)

# Save quarantined records to table
spark.sql("CREATE DATABASE IF NOT EXISTS dqx_demo")
quarantined_orders_df.write.mode("overwrite").saveAsTable(quarantine_config['target_table'])

print(f"\nValidation Summary:")
print(f"  Original Records: {orders_df.count()}")
print(f"  Clean Records: {clean_orders_df.count()}")
print(f"  Quarantined Records: {quarantine_summary.get('quarantined_records', 0)}")
print(f"  Pass Rate: {quarantine_summary.get('pass_rate', 0):.2%}")

print(f"\n✅ Quarantined {quarantine_summary.get('quarantined_records', 0)} records to {quarantine_config['target_table']}")
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

# DBTITLE 1,Cell 34
# Configure advanced quarantine with metadata enrichment
# This demonstrates how additional metadata can be added to quarantined records

advanced_quarantine_config = {
    'target_table': 'dqx_demo.quarantine_advanced',
    'partition_by': ['date'],
    'add_metadata': True,
    'metadata_columns': {
        'data_source': 'orders_pipeline',
        'environment': 'production'
    }
}

print("✅ Advanced quarantine configuration created")
print(f"  Target table: {advanced_quarantine_config['target_table']}")
print(f"  Metadata columns: {list(advanced_quarantine_config['metadata_columns'].keys())}")
print("\nNote: The quarantine validator already adds standard metadata:")
print("  - dqx_quarantine_timestamp")
print("  - dqx_quarantine_reason")
print("  - dqx_failed_checks")

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

# DBTITLE 1,Cell 42
# Re-validate remediated records
# Remove metadata columns before re-validation
remediated_clean = remediated_df.select(
    "id", "order_id", "customer_id", "amount", "discount", "status", "date"
)

revalidated_df, revalidated_quarantine, revalidation_summary = quarantine_validator(remediated_clean)

print(f"\nRe-validation Summary:")
print(f"  Total Records: {remediated_clean.count()}")
print(f"  Valid Records: {revalidated_df.count()}")
print(f"  Still Failed: {revalidated_quarantine.count()}")
print(f"  Pass Rate: {revalidation_summary.get('pass_rate', 0):.2%}")

print("\n✅ Successfully remediated records:")
display(revalidated_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Merge Back to Main Table

# COMMAND ----------

# DBTITLE 1,Cell 44
# Combine clean and remediated records
final_df = clean_orders_df.unionByName(revalidated_df)

print(f"Final dataset count: {final_df.count()}")
print(f"Original count: {orders_df.count()}")
print(f"Recovery rate: {(final_df.count() / orders_df.count()) * 100:.1f}%")

print("\n✅ Final merged dataset:")
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

# DBTITLE 1,Cell 48
# Create validator with multiple reaction types
# This demonstrates a comprehensive multi-tier strategy

def create_multi_tier_validator():
    """
    Creates a validator with DROP, QUARANTINE, and MARK reactions.
    """
    multi_quarantine_config = {
        'target_table': 'dqx_demo.multi_quarantine',
        'add_metadata': True
    }
    
    def validate(df):
        result_df = df
        
        # Critical checks - DROP reaction
        # Check 1: record_id NOT NULL (DROP if failed)
        drop_check_1 = F.col("record_id").isNull()
        
        # Check 2: amount > 0 (DROP if failed)
        drop_check_2 = F.col("amount") <= 0
        
        # Filter out records that fail DROP checks
        result_df = result_df.filter(~(drop_check_1 | drop_check_2))
        
        # Important checks - QUARANTINE reaction
        # Check 3: customer_id NOT NULL
        result_df = result_df.withColumn(
            "check_customer_id_not_null",
            F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
        )
        
        # Check 4: status IN valid set
        valid_statuses = ["pending", "completed", "cancelled"]
        result_df = result_df.withColumn(
            "check_valid_status",
            F.when(F.col("status").isin(valid_statuses), F.lit("PASSED")).otherwise(F.lit("FAILED"))
        )
        
        # Quality warnings - MARK reaction
        # Check 5: email format (MARK with flag)
        email_pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"
        result_df = result_df.withColumn(
            "email_valid",
            F.when(F.col("email").rlike(email_pattern), F.lit(True)).otherwise(F.lit(False))
        )
        
        # Collect failed checks for quarantine
        result_df = result_df.withColumn(
            "dqx_failed_checks",
            F.concat_ws(", ",
                F.when(F.col("check_customer_id_not_null") == "FAILED", F.lit("customer_id_not_null")),
                F.when(F.col("check_valid_status") == "FAILED", F.lit("valid_status"))
            )
        )
        
        # Add overall quality status
        result_df = result_df.withColumn(
            "dqx_quality_status",
            F.when(
                (F.col("check_customer_id_not_null") == "FAILED") | 
                (F.col("check_valid_status") == "FAILED"),
                F.lit("FAILED")
            ).when(
                F.col("email_valid") == False,
                F.lit("WARNING")
            ).otherwise(F.lit("PASSED"))
        )
        
        # Add quarantine metadata
        result_df = result_df.withColumn(
            "dqx_quarantine_timestamp",
            F.lit(datetime.now().isoformat())
        ).withColumn(
            "dqx_quarantine_reason",
            F.when(F.col("dqx_quality_status") == "FAILED", F.col("dqx_failed_checks")).otherwise(F.lit(None))
        )
        
        # Separate clean and quarantined records
        clean_df = result_df.filter(F.col("dqx_quality_status") != "FAILED")
        quarantined_df = result_df.filter(F.col("dqx_quality_status") == "FAILED")
        
        # Save quarantined records
        if quarantined_df.count() > 0:
            quarantined_df.write.mode("overwrite").saveAsTable(multi_quarantine_config['target_table'])
        
        # Remove check columns from clean output (keep email_valid for MARK)
        check_columns = [c for c in clean_df.columns if c.startswith('check_') or c.startswith('dqx_')]
        for col in check_columns:
            clean_df = clean_df.drop(col)
        
        # Calculate summary
        total_records = df.count()
        clean_records = clean_df.count()
        quarantined_records = quarantined_df.count()
        dropped_records = total_records - result_df.count()
        pass_rate = clean_records / total_records if total_records > 0 else 0
        
        summary = {
            'total_records': total_records,
            'clean_records': clean_records,
            'quarantined_records': quarantined_records,
            'dropped_records': dropped_records,
            'pass_rate': pass_rate
        }
        
        return clean_df, summary
    
    return validate

# Create the multi-tier validator
multi_validator = create_multi_tier_validator()

print("✅ Configured multi-tier reaction strategy")
print("  DROP: record_id_not_null, amount_positive")
print("  QUARANTINE: customer_id_not_null, valid_status")
print("  MARK: email_format")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Execute Multi-Tier Validation

# COMMAND ----------

# DBTITLE 1,Cell 50
# Run validation with multiple reactions
multi_result_df, multi_summary = multi_validator(comprehensive_df)

print(f"\nMulti-Tier Validation Summary:")
print(f"  Original Records: {comprehensive_df.count()}")
print(f"  Clean Records: {multi_result_df.count()}")
print(f"  Dropped Records: {multi_summary.get('dropped_records', 0)}")
print(f"  Quarantined Records: {multi_summary.get('quarantined_records', 0)}")
print(f"  Marked Records (warnings): {multi_result_df.filter(F.col('email_valid') == False).count()}")

print("\n✅ Final clean dataset with quality flags:")
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
# MAGIC **Continue to**: `05_Working_with_Spark_Structured_Streaming`
