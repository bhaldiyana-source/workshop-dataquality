# Databricks notebook source
# MAGIC %md
# MAGIC # Delta Lake Quality Features
# MAGIC
# MAGIC **Module 3: Built-in Quality Capabilities**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 40 minutes    |
# MAGIC | Level           | 200/300       |
# MAGIC | Type            | Lecture       |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will understand:
# MAGIC
# MAGIC 1. **Table Constraints** - NOT NULL and CHECK constraints for data quality
# MAGIC 2. **Schema Evolution** - Managing schema changes safely
# MAGIC 3. **Change Data Feed** - Tracking data quality changes over time
# MAGIC 4. **Delta Live Tables Expectations** - Quality enforcement in pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake Quality Features Overview
# MAGIC
# MAGIC Delta Lake provides built-in features for data quality:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │              DELTA LAKE QUALITY FEATURES                     │
# MAGIC ├─────────────────────────────────────────────────────────────┤
# MAGIC │                                                              │
# MAGIC │  1. TABLE CONSTRAINTS                                        │
# MAGIC │     • NOT NULL constraints                                   │
# MAGIC │     • CHECK constraints (custom validation)                  │
# MAGIC │     • Enforced at write time                                 │
# MAGIC │                                                              │
# MAGIC │  2. SCHEMA ENFORCEMENT & EVOLUTION                           │
# MAGIC │     • Automatic schema validation                            │
# MAGIC │     • Controlled schema changes                              │
# MAGIC │     • Type safety                                            │
# MAGIC │                                                              │
# MAGIC │  3. CHANGE DATA FEED (CDF)                                   │
# MAGIC │     • Track all data changes                                 │
# MAGIC │     • Quality audit trail                                    │
# MAGIC │     • Historical analysis                                    │
# MAGIC │                                                              │
# MAGIC │  4. DELTA LIVE TABLES EXPECTATIONS                           │
# MAGIC │     • expect() - track violations                            │
# MAGIC │     • expect_or_drop() - remove bad rows                     │
# MAGIC │     • expect_or_fail() - stop pipeline                       │
# MAGIC │                                                              │
# MAGIC └─────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Table Constraints
# MAGIC
# MAGIC Delta Lake supports two types of constraints:
# MAGIC - **NOT NULL constraints**: Ensure columns always have values
# MAGIC - **CHECK constraints**: Custom validation logic using SQL expressions

# COMMAND ----------

# MAGIC %md
# MAGIC ### NOT NULL Constraints
# MAGIC
# MAGIC Prevent NULL values in critical columns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with NOT NULL constraint
# MAGIC CREATE TABLE IF NOT EXISTS customers_demo (
# MAGIC   customer_id INT NOT NULL,
# MAGIC   email STRING NOT NULL,
# MAGIC   name STRING,
# MAGIC   created_date DATE NOT NULL
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- View table properties
# MAGIC DESCRIBE EXTENDED customers_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### CHECK Constraints
# MAGIC
# MAGIC Enforce business rules at the table level

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table for constraint demo
# MAGIC CREATE TABLE IF NOT EXISTS orders_demo (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   order_date DATE,
# MAGIC   ship_date DATE,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING
# MAGIC ) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add CHECK constraints
# MAGIC
# MAGIC -- Constraint 1: Order ID must not be null
# MAGIC ALTER TABLE orders_demo 
# MAGIC ADD CONSTRAINT order_id_not_null CHECK (order_id IS NOT NULL);
# MAGIC
# MAGIC -- Constraint 2: Amount must be positive
# MAGIC ALTER TABLE orders_demo 
# MAGIC ADD CONSTRAINT amount_positive CHECK (amount > 0);
# MAGIC
# MAGIC -- Constraint 3: Status must be valid
# MAGIC ALTER TABLE orders_demo 
# MAGIC ADD CONSTRAINT status_valid CHECK (status IN ('pending', 'completed', 'cancelled'));
# MAGIC
# MAGIC -- Constraint 4: Ship date after order date
# MAGIC ALTER TABLE orders_demo 
# MAGIC ADD CONSTRAINT ship_after_order CHECK (ship_date >= order_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all constraints on the table
# MAGIC SHOW TBLPROPERTIES orders_demo;

# COMMAND ----------

# Example: Test constraints (this will fail)
try:
    # This should fail the amount_positive constraint
    spark.sql("""
        INSERT INTO orders_demo VALUES 
        (1, 100, '2024-01-01', '2024-01-05', -50.00, 'pending')
    """)
except Exception as e:
    print(f"❌ Constraint violation (expected): {str(e)}")

# COMMAND ----------

# Example: Valid insert
try:
    spark.sql("""
        INSERT INTO orders_demo VALUES 
        (1, 100, '2024-01-01', '2024-01-05', 150.00, 'completed')
    """)
    print("✅ Valid record inserted successfully")
except Exception as e:
    print(f"❌ Unexpected error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common CHECK Constraint Patterns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Pattern 1: Value ranges
# MAGIC ALTER TABLE orders_demo 
# MAGIC ADD CONSTRAINT amount_range CHECK (amount BETWEEN 0.01 AND 1000000);
# MAGIC
# MAGIC -- Pattern 2: Email format validation
# MAGIC ALTER TABLE customers_demo 
# MAGIC ADD CONSTRAINT email_format 
# MAGIC CHECK (email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$');
# MAGIC
# MAGIC -- Pattern 3: Date logic
# MAGIC ALTER TABLE orders_demo 
# MAGIC ADD CONSTRAINT valid_order_date CHECK (order_date >= '2020-01-01');
# MAGIC
# MAGIC -- Pattern 4: Cross-column validation
# MAGIC ALTER TABLE orders_demo 
# MAGIC ADD CONSTRAINT status_amount_check 
# MAGIC CHECK (status != 'completed' OR amount IS NOT NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC ### Managing Constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop a constraint if needed
# MAGIC ALTER TABLE orders_demo DROP CONSTRAINT IF EXISTS amount_range;
# MAGIC
# MAGIC -- Note: You cannot modify a constraint, you must drop and recreate

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema Evolution and Enforcement
# MAGIC
# MAGIC Delta Lake protects data quality through schema management

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Enforcement (Default Behavior)
# MAGIC
# MAGIC Delta Lake prevents incompatible schema changes by default

# COMMAND ----------

# Create sample data with correct schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

correct_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("price", DoubleType(), False)
])

correct_data = [(1, "Widget", 29.99), (2, "Gadget", 49.99)]
df_correct = spark.createDataFrame(correct_data, correct_schema)

# Write to Delta table
df_correct.write.format("delta").mode("overwrite").saveAsTable("products_demo")

display(spark.table("products_demo"))

# COMMAND ----------

# Try to append data with incompatible schema (this will fail)
incompatible_schema = StructType([
    StructField("product_id", StringType(), False),  # Changed to String!
    StructField("product_name", StringType(), False),
    StructField("price", DoubleType(), False)
])

incompatible_data = [("3", "Doohickey", 39.99)]
df_incompatible = spark.createDataFrame(incompatible_data, incompatible_schema)

try:
    df_incompatible.write.format("delta").mode("append").saveAsTable("products_demo")
    print("❌ This should not print")
except Exception as e:
    print(f"✅ Schema enforcement prevented bad write: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution (Opt-in)
# MAGIC
# MAGIC Allow additive schema changes when needed

# COMMAND ----------

# Add a new column with schema evolution
new_schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), True)  # New column
])

new_data = [(3, "Thingamajig", 59.99, "Electronics")]
df_evolved = spark.createDataFrame(new_data, new_schema)

# Enable schema evolution
df_evolved.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("products_demo")

print("✅ Schema evolution successful")
display(spark.table("products_demo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Validation Strategy

# COMMAND ----------

def validate_schema_compatibility(source_df, target_table):
    """
    Validate schema compatibility before writing to Delta table
    """
    
    target_df = spark.table(target_table)
    target_schema = target_df.schema
    source_schema = source_df.schema
    
    issues = []
    
    # Check for missing required columns
    target_cols = {field.name: field for field in target_schema}
    source_cols = {field.name: field for field in source_schema}
    
    missing_cols = set(target_cols.keys()) - set(source_cols.keys())
    if missing_cols:
        issues.append(f"Missing required columns: {missing_cols}")
    
    # Check for data type mismatches
    for col_name in set(target_cols.keys()) & set(source_cols.keys()):
        target_type = target_cols[col_name].dataType
        source_type = source_cols[col_name].dataType
        
        if target_type != source_type:
            issues.append(f"Type mismatch for {col_name}: {source_type} vs {target_type}")
    
    if issues:
        return {
            'compatible': False,
            'issues': issues
        }
    else:
        return {
            'compatible': True,
            'issues': []
        }

# Test schema validation
validation_result = validate_schema_compatibility(df_correct, "products_demo")
print(f"Schema compatible: {validation_result['compatible']}")
if not validation_result['compatible']:
    print(f"Issues: {validation_result['issues']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Change Data Feed (CDF)
# MAGIC
# MAGIC Track all changes to Delta tables for quality auditing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Enable Change Data Feed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table with CDF enabled
# MAGIC CREATE TABLE IF NOT EXISTS orders_cdf_demo (
# MAGIC   order_id INT,
# MAGIC   customer_id INT,
# MAGIC   amount DECIMAL(10,2),
# MAGIC   status STRING,
# MAGIC   updated_at TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Or enable on existing table
# MAGIC ALTER TABLE orders_cdf_demo 
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# Insert initial data
from pyspark.sql.functions import current_timestamp

initial_data = [
    (1, 100, 150.00, "pending"),
    (2, 101, 200.00, "pending"),
    (3, 102, 175.00, "completed")
]

df_initial = spark.createDataFrame(initial_data, ["order_id", "customer_id", "amount", "status"]) \
    .withColumn("updated_at", current_timestamp())

df_initial.write.format("delta").mode("append").saveAsTable("orders_cdf_demo")

display(spark.table("orders_cdf_demo"))

# COMMAND ----------

# Update some records
spark.sql("""
    UPDATE orders_cdf_demo 
    SET status = 'completed', updated_at = current_timestamp()
    WHERE order_id = 1
""")

# Delete a record
spark.sql("""
    DELETE FROM orders_cdf_demo WHERE order_id = 3
""")

# Insert new record
new_order = [(4, 103, 300.00, "pending")]
df_new = spark.createDataFrame(new_order, ["order_id", "customer_id", "amount", "status"]) \
    .withColumn("updated_at", current_timestamp())
df_new.write.format("delta").mode("append").saveAsTable("orders_cdf_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Change Data Feed

# COMMAND ----------

# Read all changes from a specific version
changes_df = spark.read \
    .format("delta") \
    .option("readChangeData", "true") \
    .option("startingVersion", 0) \
    .table("orders_cdf_demo")

display(changes_df)

# COMMAND ----------

# Analyze changes for quality audit
from pyspark.sql.functions import col

# Count changes by type
change_summary = changes_df.groupBy("_change_type", "_commit_version").count()
display(change_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use CDF for Quality Tracking

# COMMAND ----------

def audit_data_quality_changes(table_name, start_version=0):
    """
    Audit data quality using Change Data Feed
    """
    
    changes = spark.read \
        .format("delta") \
        .option("readChangeData", "true") \
        .option("startingVersion", start_version) \
        .table(table_name)
    
    # Track quality-impacting changes
    quality_audit = changes.groupBy("_commit_version", "_change_type").agg(
        count("*").alias("records_changed")
    ).orderBy("_commit_version")
    
    print(f"\nQuality Audit for {table_name}")
    print("=" * 60)
    display(quality_audit)
    
    # Identify deleted records (potential quality issues)
    deleted_records = changes.filter(col("_change_type") == "delete")
    print(f"\nDeleted records: {deleted_records.count()}")
    
    # Identify updated records (corrections?)
    updated_records = changes.filter(col("_change_type").isin("update_preimage", "update_postimage"))
    print(f"Updated records: {updated_records.count() // 2}")  # Divided by 2 (pre/post images)
    
    return quality_audit

# Run quality audit
audit_data_quality_changes("orders_cdf_demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta Live Tables Expectations
# MAGIC
# MAGIC DLT provides declarative quality enforcement in data pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Expectation Types
# MAGIC
# MAGIC | Expectation | Behavior | Use Case |
# MAGIC |-------------|----------|----------|
# MAGIC | `expect()` | Track violations in metrics | Monitor quality issues |
# MAGIC | `expect_or_drop()` | Drop rows that fail | Remove invalid data |
# MAGIC | `expect_or_fail()` | Stop pipeline on failure | Critical validation |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: DLT Quality Patterns
# MAGIC
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC # Pattern 1: Track violations (expect)
# MAGIC @dlt.table
# MAGIC @dlt.expect("valid_order_id", "order_id IS NOT NULL")
# MAGIC @dlt.expect("valid_amount", "amount > 0")
# MAGIC def bronze_orders():
# MAGIC     return spark.readStream.format("cloudFiles") \
# MAGIC         .option("cloudFiles.format", "json") \
# MAGIC         .load("/path/to/source")
# MAGIC
# MAGIC # Pattern 2: Drop invalid rows (expect_or_drop)
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_drop("valid_email", "email RLIKE '^[^@]+@[^@]+\\\\.[^@]+$'")
# MAGIC @dlt.expect_or_drop("positive_amount", "amount > 0")
# MAGIC def silver_orders():
# MAGIC     return dlt.read_stream("bronze_orders")
# MAGIC
# MAGIC # Pattern 3: Fail on critical violations (expect_or_fail)
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_fail("required_customer_id", "customer_id IS NOT NULL")
# MAGIC @dlt.expect_or_fail("valid_order_date", "order_date IS NOT NULL")
# MAGIC def gold_orders():
# MAGIC     return dlt.read("silver_orders").groupBy("order_date").agg(...)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### DLT Quality Monitoring
# MAGIC
# MAGIC DLT automatically tracks expectation metrics:
# MAGIC
# MAGIC ```sql
# MAGIC -- Query expectation metrics
# MAGIC SELECT 
# MAGIC   dataset,
# MAGIC   name as expectation,
# MAGIC   passed_records,
# MAGIC   failed_records,
# MAGIC   passed_records / (passed_records + failed_records) * 100 as pass_rate
# MAGIC FROM event_log
# MAGIC WHERE event_type = 'flow_progress'
# MAGIC   AND expectations IS NOT NULL
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combining DLT with Custom Quality

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC import dlt
# MAGIC from pyspark.sql.functions import col, when, current_timestamp
# MAGIC
# MAGIC @dlt.table
# MAGIC @dlt.expect_or_drop("valid_order", "order_id IS NOT NULL")
# MAGIC def silver_orders_with_quality():
# MAGIC     """
# MAGIC     Silver table with DLT expectations and custom quality scoring
# MAGIC     """
# MAGIC     
# MAGIC     df = dlt.read_stream("bronze_orders")
# MAGIC     
# MAGIC     # Add custom quality score
# MAGIC     df_quality = df \
# MAGIC         .withColumn("quality_score", lit(100)) \
# MAGIC         .withColumn("quality_score",
# MAGIC             when(col("amount").isNull(), col("quality_score") - 20)
# MAGIC             .otherwise(col("quality_score"))
# MAGIC         ) \
# MAGIC         .withColumn("quality_score",
# MAGIC             when(~col("status").isin("pending", "completed", "cancelled"), 
# MAGIC                  col("quality_score") - 20)
# MAGIC             .otherwise(col("quality_score"))
# MAGIC         ) \
# MAGIC         .withColumn("quality_timestamp", current_timestamp())
# MAGIC     
# MAGIC     return df_quality
# MAGIC
# MAGIC # Create quarantine table for low-quality records
# MAGIC @dlt.table
# MAGIC def silver_orders_quarantine():
# MAGIC     return dlt.read_stream("silver_orders_with_quality") \
# MAGIC         .filter(col("quality_score") < 70)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### Table Constraints
# MAGIC 1. **Start simple** - Add constraints incrementally
# MAGIC 2. **Test first** - Validate constraints on sample data
# MAGIC 3. **Document** - Comment why each constraint exists
# MAGIC 4. **Monitor** - Track constraint violations over time
# MAGIC 5. **Maintain** - Review and update constraints regularly
# MAGIC
# MAGIC ### Schema Management
# MAGIC 1. **Enforce by default** - Only enable evolution when needed
# MAGIC 2. **Validate explicitly** - Check compatibility before writes
# MAGIC 3. **Version schemas** - Track schema changes in metadata
# MAGIC 4. **Test evolution** - Validate backward compatibility
# MAGIC 5. **Communicate changes** - Notify downstream consumers
# MAGIC
# MAGIC ### Change Data Feed
# MAGIC 1. **Enable early** - Turn on CDF for quality-critical tables
# MAGIC 2. **Audit regularly** - Review changes for quality patterns
# MAGIC 3. **Retention** - Set appropriate CDF retention policies
# MAGIC 4. **Performance** - Be aware of storage impact
# MAGIC 5. **Integration** - Use CDF for downstream quality checks
# MAGIC
# MAGIC ### DLT Expectations
# MAGIC 1. **Progressive validation** - expect → expect_or_drop → expect_or_fail
# MAGIC 2. **Clear names** - Use descriptive expectation names
# MAGIC 3. **Monitor metrics** - Track expectation pass rates
# MAGIC 4. **Combine approaches** - Use with custom quality logic
# MAGIC 5. **Alert on failures** - Set up notifications for critical expectations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete Example: Quality-First Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a production-ready table with all quality features
# MAGIC CREATE TABLE IF NOT EXISTS orders_production (
# MAGIC   order_id BIGINT NOT NULL,
# MAGIC   customer_id BIGINT NOT NULL,
# MAGIC   order_date DATE NOT NULL,
# MAGIC   ship_date DATE,
# MAGIC   amount DECIMAL(10,2) NOT NULL,
# MAGIC   status STRING NOT NULL,
# MAGIC   quality_score INT,
# MAGIC   created_timestamp TIMESTAMP NOT NULL,
# MAGIC   updated_timestamp TIMESTAMP
# MAGIC ) USING DELTA
# MAGIC PARTITIONED BY (order_date)
# MAGIC TBLPROPERTIES (
# MAGIC   delta.enableChangeDataFeed = true,
# MAGIC   delta.autoOptimize.optimizeWrite = true,
# MAGIC   delta.autoOptimize.autoCompact = true
# MAGIC );
# MAGIC
# MAGIC -- Add constraints
# MAGIC ALTER TABLE orders_production ADD CONSTRAINT amount_positive CHECK (amount > 0);
# MAGIC ALTER TABLE orders_production ADD CONSTRAINT status_valid CHECK (status IN ('pending', 'completed', 'cancelled'));
# MAGIC ALTER TABLE orders_production ADD CONSTRAINT ship_after_order CHECK (ship_date IS NULL OR ship_date >= order_date);
# MAGIC ALTER TABLE orders_production ADD CONSTRAINT quality_range CHECK (quality_score BETWEEN 0 AND 100);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Delta Lake Provides
# MAGIC - **Constraints**: Enforce rules at write time
# MAGIC - **Schema Management**: Control data structure evolution
# MAGIC - **Change Tracking**: Audit quality over time
# MAGIC - **Pipeline Quality**: DLT expectations for workflows
# MAGIC
# MAGIC ### Use These Features To
# MAGIC 1. Prevent bad data from entering tables
# MAGIC 2. Maintain data structure integrity
# MAGIC 3. Track quality changes over time
# MAGIC 4. Build quality into pipelines
# MAGIC
# MAGIC ### Remember
# MAGIC - Constraints are enforced at write time
# MAGIC - Schema enforcement is the default (and that's good!)
# MAGIC - CDF adds storage overhead but enables powerful auditing
# MAGIC - DLT expectations integrate quality into pipeline definitions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Module 4: Quality Monitoring & Alerting** to learn:
# MAGIC - Building quality dashboards
# MAGIC - Setting up quality SLAs
# MAGIC - Creating alerting workflows
# MAGIC - Tracking quality trends over time
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 40 minutes | **Level**: 200/300 | **Type**: Lecture
