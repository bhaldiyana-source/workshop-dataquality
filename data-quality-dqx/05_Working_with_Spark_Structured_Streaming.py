# Databricks notebook source
# MAGIC %md
# MAGIC # Working with Spark Structured Streaming
# MAGIC
# MAGIC **Module 6: Real-Time Data Quality**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 30 minutes    |
# MAGIC | Level           | 400           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Validate streaming DataFrames** with DQX
# MAGIC 2. **Configure streaming quarantine** patterns
# MAGIC 3. **Monitor quality metrics** in real-time
# MAGIC 4. **Handle checkpoints** properly
# MAGIC 5. **Manage late-arriving data** quality checks

# COMMAND ----------

# DBTITLE 1,Cell 3
# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import time
import random

print("✅ Libraries imported successfully!")
print("Using PySpark Structured Streaming for data quality validation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Understanding Streaming Quality Validation
# MAGIC
# MAGIC Key differences from batch validation:
# MAGIC
# MAGIC | Aspect | Batch | Streaming |
# MAGIC |--------|-------|-----------|
# MAGIC | **Data Processing** | All at once | Micro-batches |
# MAGIC | **Validation** | One-time | Continuous |
# MAGIC | **Quarantine** | Immediate | Per micro-batch |
# MAGIC | **Metrics** | Final summary | Continuous updates |
# MAGIC | **Checkpoints** | Not needed | Required |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Setting Up Streaming Source
# MAGIC
# MAGIC ### 2.1 Create Sample Streaming Data Directory

# COMMAND ----------

# DBTITLE 1,Setup Streaming Directories
# Create directory for streaming source using workspace path
streaming_path = "/Workspace/Users/ajit.kalura@databricks.com/dqx_streaming_demo"
checkpoint_path = "/Workspace/Users/ajit.kalura@databricks.com/dqx_streaming_checkpoint"

# Create directories
dbutils.fs.mkdirs(streaming_path)
dbutils.fs.mkdirs(checkpoint_path)

print(f"✅ Streaming directories ready:")
print(f"   Source: {streaming_path}")
print(f"   Checkpoint: {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Define Streaming Schema

# COMMAND ----------

# Define schema for streaming transactions
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

print("✅ Transaction schema defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Create Sample Streaming Data Generator

# COMMAND ----------

# DBTITLE 1,Cell 10
def generate_transaction_batch(batch_id, num_records=10):
    """Generate a batch of transaction records with some quality issues"""
    
    transactions = []
    base_time = datetime.now()
    
    for i in range(num_records):
        record_id = batch_id * num_records + i + 1
        
        # Introduce quality issues in ~20% of records
        if random.random() < 0.2:
            # Create records with issues
            issue_type = random.choice(['null_customer', 'negative_amount', 'invalid_status', 'excessive_discount'])
            
            if issue_type == 'null_customer':
                transaction = (
                    f"TXN{record_id:06d}",
                    None,  # Null customer_id
                    round(random.uniform(100, 5000), 2),
                    round(random.uniform(10, 500), 2),
                    random.choice(['pending', 'completed']),
                    random.choice(['credit_card', 'paypal', 'debit_card']),
                    base_time + timedelta(seconds=i)
                )
            elif issue_type == 'negative_amount':
                transaction = (
                    f"TXN{record_id:06d}",
                    f"CUST{random.randint(1, 100):04d}",
                    round(random.uniform(-500, -10), 2),  # Negative amount
                    round(random.uniform(10, 50), 2),
                    random.choice(['pending', 'completed']),
                    random.choice(['credit_card', 'paypal']),
                    base_time + timedelta(seconds=i)
                )
            elif issue_type == 'invalid_status':
                transaction = (
                    f"TXN{record_id:06d}",
                    f"CUST{random.randint(1, 100):04d}",
                    round(random.uniform(100, 5000), 2),
                    round(random.uniform(10, 500), 2),
                    'invalid_status',  # Invalid status
                    random.choice(['credit_card', 'paypal']),
                    base_time + timedelta(seconds=i)
                )
            else:  # excessive_discount
                amount = round(random.uniform(100, 1000), 2)
                transaction = (
                    f"TXN{record_id:06d}",
                    f"CUST{random.randint(1, 100):04d}",
                    amount,
                    amount * 0.7,  # 70% discount (excessive)
                    random.choice(['pending', 'completed']),
                    random.choice(['credit_card', 'paypal']),
                    base_time + timedelta(seconds=i)
                )
        else:
            # Create valid record
            amount = round(random.uniform(100, 5000), 2)
            transaction = (
                f"TXN{record_id:06d}",
                f"CUST{random.randint(1, 100):04d}",
                amount,
                round(random.uniform(10, amount * 0.3), 2),  # Valid discount
                random.choice(['pending', 'processing', 'completed', 'cancelled']),
                random.choice(['credit_card', 'paypal', 'debit_card', 'bank_transfer']),
                base_time + timedelta(seconds=i)
            )
        
        transactions.append(transaction)
    
    # Create DataFrame
    df = spark.createDataFrame(transactions, transaction_schema)
    
    # Write as JSON to simulate streaming source (append mode to avoid overwrite)
    output_path = f"{streaming_path}/batch_{batch_id:04d}.json"
    df.coalesce(1).write.mode("append").json(output_path)
    
    return df

# Generate first batch
first_batch = generate_transaction_batch(1, 20)
print("✅ Generated first batch of streaming data")
display(first_batch)

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ### Alternative: Use Rate Source for Streaming Demo
# MAGIC
# MAGIC Since file-based streaming has path restrictions, we'll use Spark's built-in rate source to generate streaming data.

# COMMAND ----------

# DBTITLE 1,Generate Sample Data
# Create sample transaction data in memory
sample_transactions = [
    ("TXN000001", "CUST0001", 1000.00, 100.00, "completed", "credit_card", datetime.now()),
    ("TXN000002", None, 500.00, 50.00, "completed", "paypal", datetime.now()),  # Issue: null customer
    ("TXN000003", "CUST0003", -200.00, 20.00, "completed", "credit_card", datetime.now()),  # Issue: negative amount
    ("TXN000004", "CUST0004", 1500.00, 800.00, "pending", "credit_card", datetime.now()),  # Issue: excessive discount
    ("TXN000005", "CUST0005", 750.00, 75.00, "invalid_status", "debit_card", datetime.now()),  # Issue: invalid status
    ("TXN000006", "CUST0006", 2000.00, 200.00, "completed", "bank_transfer", datetime.now()),
    ("TXN000007", "CUST0007", 3000.00, 300.00, "processing", "credit_card", datetime.now()),
    ("TXN000008", "CUST0008", 1200.00, 120.00, "completed", "paypal", datetime.now()),
    ("TXN000009", "CUST0009", 800.00, 80.00, "cancelled", "debit_card", datetime.now()),
    ("TXN000010", "CUST0010", 1800.00, 180.00, "completed", "credit_card", datetime.now()),
]

transactions_batch_df = spark.createDataFrame(sample_transactions, transaction_schema)

print("✅ Sample transaction data created")
print(f"Total records: {transactions_batch_df.count()}")
display(transactions_batch_df)

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ### Demonstrate Validation Logic
# MAGIC
# MAGIC We'll apply the same validation logic that would be used in streaming to our batch data.

# COMMAND ----------

# DBTITLE 1,Apply Validation to Batch
# Apply validation (same logic as would be used in streaming)
validated_batch = apply_streaming_validation(transactions_batch_df)

print("✅ Validation applied")
print(f"Total records: {validated_batch.count()}")
print(f"Passed: {validated_batch.filter(F.col('dqx_quality_status') == 'PASSED').count()}")
print(f"Failed: {validated_batch.filter(F.col('dqx_quality_status') == 'FAILED').count()}")
print(f"Warnings: {validated_batch.filter(F.col('dqx_quality_status') == 'WARNING').count()}")

display(validated_batch)

# COMMAND ----------

# DBTITLE 1,Show Clean Records
# Filter clean records (what would go to clean stream)
clean_records = validated_batch.filter(F.col("dqx_quality_status") == "PASSED").drop(
    "check_transaction_id_not_null", "check_customer_id_not_null", 
    "check_amount_positive", "check_valid_status", "check_discount_validation",
    "dqx_failed_checks", "dqx_quality_status"
)

print(f"✅ Clean records: {clean_records.count()}")
display(clean_records)

# COMMAND ----------

# DBTITLE 1,Show Quarantined Records
# Show records that would be quarantined
quarantined_records = validated_batch.filter(F.col("dqx_quality_status") == "FAILED")

print(f"✅ Quarantined records: {quarantined_records.count()}")
print("\nFailed checks by record:")
display(quarantined_records.select(
    "transaction_id", "customer_id", "amount", "discount", "status",
    "dqx_failed_checks", "dqx_quality_status"
))

# COMMAND ----------

# DBTITLE 1,Calculate Quality Metrics
# Calculate quality metrics (what would be written to metrics stream)
total_records = validated_batch.count()
passed_records = validated_batch.filter(F.col("dqx_quality_status") == "PASSED").count()
failed_records = validated_batch.filter(F.col("dqx_quality_status") == "FAILED").count()
warning_records = validated_batch.filter(F.col("dqx_quality_status") == "WARNING").count()
pass_rate = passed_records / total_records if total_records > 0 else 0

print("\n=== Quality Metrics ===")
print(f"Total Records: {total_records}")
print(f"Passed: {passed_records}")
print(f"Failed: {failed_records}")
print(f"Warnings: {warning_records}")
print(f"Pass Rate: {pass_rate:.2%}")

# Analyze failures by check type
print("\n=== Failures by Check ===")
for check_col in [c for c in validated_batch.columns if c.startswith('check_')]:
    failed_count = validated_batch.filter(F.col(check_col) == "FAILED").count()
    if failed_count > 0:
        print(f"  {check_col}: {failed_count} failures")

# COMMAND ----------

# DBTITLE 1,Cell 18
# MAGIC %md
# MAGIC %undefined
# MAGIC ## Streaming Data Quality Concepts Demonstrated
# MAGIC
# MAGIC While we used batch processing for this demo due to path restrictions, the **same validation logic applies to streaming**:
# MAGIC
# MAGIC ### Key Streaming Patterns:
# MAGIC
# MAGIC 1. **Continuous Validation**: Each micro-batch would be validated using `apply_streaming_validation()`
# MAGIC
# MAGIC 2. **Clean Stream**: Records with `dqx_quality_status == 'PASSED'` flow to the clean table
# MAGIC
# MAGIC 3. **Quarantine Stream**: Records with `dqx_quality_status == 'FAILED'` go to quarantine table
# MAGIC
# MAGIC 4. **Metrics Stream**: Quality metrics calculated per micro-batch for monitoring
# MAGIC
# MAGIC 5. **Checkpoints**: Required for fault tolerance in production streaming
# MAGIC
# MAGIC ### In Production Streaming:
# MAGIC
# MAGIC ```python
# MAGIC # This is how it would work with real streaming:
# MAGIC validated_stream = apply_streaming_validation(streaming_df)
# MAGIC
# MAGIC # Write clean records
# MAGIC clean_query = (
# MAGIC     validated_stream
# MAGIC     .filter(F.col("dqx_quality_status") == "PASSED")
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", checkpoint_path)
# MAGIC     .table("clean_table")
# MAGIC )
# MAGIC
# MAGIC # Write quarantined records using foreachBatch
# MAGIC def quarantine_batch(batch_df, batch_id):
# MAGIC     quarantined = batch_df.filter(F.col("dqx_quality_status") == "FAILED")
# MAGIC     if quarantined.count() > 0:
# MAGIC         quarantined.write.mode("append").saveAsTable("quarantine_table")
# MAGIC
# MAGIC quarantine_query = validated_stream.writeStream.foreachBatch(quarantine_batch).start()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Streaming Validation with DQX
# MAGIC
# MAGIC ### 3.1 Create Streaming Source

# COMMAND ----------

# Create streaming DataFrame
streaming_df = (
    spark.readStream
    .schema(transaction_schema)
    .option("maxFilesPerTrigger", 1)
    .json(streaming_path)
)

print("✅ Streaming DataFrame created")
print(f"Is streaming: {streaming_df.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Create StreamingValidator

# COMMAND ----------

# DBTITLE 1,Cell 14
# Create streaming validation configuration
# We'll use PySpark's foreachBatch to apply validation to each micro-batch

streaming_validation_config = {
    'checks': [],
    'quarantine_table': 'dqx_demo.streaming_quarantine',
    'metrics_table': 'dqx_demo.streaming_quality_metrics'
}

print("✅ Streaming validation configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Add Quality Checks

# COMMAND ----------

# DBTITLE 1,Cell 16
# Define quality checks for streaming data
# These will be applied to each micro-batch

def apply_streaming_validation(df):
    """
    Apply quality checks to a streaming micro-batch.
    Returns DataFrame with validation columns.
    """
    result_df = df
    
    # Check 1: transaction_id NOT NULL
    result_df = result_df.withColumn(
        "check_transaction_id_not_null",
        F.when(F.col("transaction_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
    )
    
    # Check 2: customer_id NOT NULL (will quarantine if failed)
    result_df = result_df.withColumn(
        "check_customer_id_not_null",
        F.when(F.col("customer_id").isNull(), F.lit("FAILED")).otherwise(F.lit("PASSED"))
    )
    
    # Check 3: amount > 0
    result_df = result_df.withColumn(
        "check_amount_positive",
        F.when(F.col("amount") <= 0, F.lit("FAILED")).otherwise(F.lit("PASSED"))
    )
    
    # Check 4: valid status
    valid_statuses = ["pending", "processing", "completed", "cancelled", "refunded"]
    result_df = result_df.withColumn(
        "check_valid_status",
        F.when(F.col("status").isin(valid_statuses), F.lit("PASSED")).otherwise(F.lit("WARNING"))
    )
    
    # Check 5: discount validation (row-level rule)
    result_df = result_df.withColumn(
        "check_discount_validation",
        F.when(F.col("discount") > F.col("amount") * 0.5, F.lit("FAILED")).otherwise(F.lit("PASSED"))
    )
    
    # Collect failed checks
    result_df = result_df.withColumn(
        "dqx_failed_checks",
        F.concat_ws(", ",
            F.when(F.col("check_transaction_id_not_null") == "FAILED", F.lit("transaction_id_not_null")),
            F.when(F.col("check_customer_id_not_null") == "FAILED", F.lit("customer_id_not_null")),
            F.when(F.col("check_amount_positive") == "FAILED", F.lit("amount_positive")),
            F.when(F.col("check_valid_status") == "WARNING", F.lit("valid_status")),
            F.when(F.col("check_discount_validation") == "FAILED", F.lit("discount_validation"))
        )
    )
    
    # Add overall quality status
    result_df = result_df.withColumn(
        "dqx_quality_status",
        F.when(
            (F.col("check_transaction_id_not_null") == "FAILED") | 
            (F.col("check_customer_id_not_null") == "FAILED") |
            (F.col("check_amount_positive") == "FAILED") |
            (F.col("check_discount_validation") == "FAILED"),
            F.lit("FAILED")
        ).when(
            F.col("check_valid_status") == "WARNING",
            F.lit("WARNING")
        ).otherwise(F.lit("PASSED"))
    )
    
    return result_df

print("✅ Quality checks defined for streaming validation")
print("Checks configured:")
print("  - transaction_id NOT NULL (error)")
print("  - customer_id NOT NULL (error, quarantine)")
print("  - amount > 0 (error)")
print("  - valid status (warning)")
print("  - discount <= 50% of amount (error)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Apply Streaming Validation

# COMMAND ----------

# DBTITLE 1,Cell 18
# Apply validation to streaming DataFrame
validated_stream = apply_streaming_validation(streaming_df)

print("✅ Validation applied to stream")
print(f"Is streaming: {validated_stream.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Write Clean Data Stream

# COMMAND ----------

# DBTITLE 1,Cell 20
# Create database if it doesn't exist
spark.sql("CREATE DATABASE IF NOT EXISTS dqx_demo")

# Write validated stream to Delta table (only clean records)
clean_stream_query = (
    validated_stream
    .filter(F.col("dqx_quality_status") == "PASSED")
    .drop("check_transaction_id_not_null", "check_customer_id_not_null", 
          "check_amount_positive", "check_valid_status", "check_discount_validation",
          "dqx_failed_checks", "dqx_quality_status")
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/clean_transactions")
    .trigger(processingTime="5 seconds")
    .table("dqx_demo.clean_transactions_stream")
)

print("✅ Clean transaction stream started")
print(f"   Stream ID: {clean_stream_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Streaming Quality Metrics
# MAGIC
# MAGIC ### 4.1 Write Quality Metrics Stream

# COMMAND ----------

# DBTITLE 1,Cell 22
# Create quality metrics stream using foreachBatch
def write_quality_metrics(batch_df, batch_id):
    """
    Calculate and write quality metrics for each micro-batch.
    """
    if batch_df.count() == 0:
        return
    
    # Calculate metrics
    total_records = batch_df.count()
    passed_records = batch_df.filter(F.col("dqx_quality_status") == "PASSED").count()
    failed_records = batch_df.filter(F.col("dqx_quality_status") == "FAILED").count()
    warning_records = batch_df.filter(F.col("dqx_quality_status") == "WARNING").count()
    pass_rate = passed_records / total_records if total_records > 0 else 0
    
    # Count failures by check
    check_columns = [c for c in batch_df.columns if c.startswith('check_')]
    failed_checks = {}
    for check_col in check_columns:
        failed_count = batch_df.filter(F.col(check_col) == "FAILED").count()
        if failed_count > 0:
            failed_checks[check_col] = failed_count
    
    # Create metrics record
    metrics_data = [(
        batch_id,
        datetime.now(),
        total_records,
        passed_records,
        failed_records,
        warning_records,
        pass_rate,
        str(failed_checks)
    )]
    
    metrics_df = spark.createDataFrame(metrics_data, [
        "batch_id", "timestamp", "total_records", "passed_records", 
        "failed_records", "warning_records", "pass_rate", "failed_checks_detail"
    ])
    
    # Write metrics to table
    metrics_df.write.mode("append").saveAsTable("dqx_demo.streaming_quality_metrics")

# Start metrics stream
metrics_query = (
    validated_stream
    .writeStream
    .foreachBatch(write_quality_metrics)
    .option("checkpointLocation", f"{checkpoint_path}/quality_metrics")
    .trigger(processingTime="5 seconds")
    .start()
)

print("✅ Quality metrics stream started")
print(f"   Stream ID: {metrics_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Generate More Streaming Data

# COMMAND ----------

# DBTITLE 1,Cell 32
# Note: File-based streaming has path restrictions in this environment
# We'll use the batch data already created to demonstrate the validation concepts

print("✅ Using sample batch data to demonstrate streaming validation concepts")
print("\nIn production, you would:")
print("  1. Use cloud storage (S3, ADLS, GCS) for streaming sources")
print("  2. Or use Kafka, Kinesis, Event Hubs for real-time ingestion")
print("  3. Apply the same validation logic shown here to each micro-batch")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Query Clean Transactions

# COMMAND ----------

# DBTITLE 1,Cell 34
# Show clean transactions (what would be in the clean stream)
print(f"Total clean transactions: {clean_records.count()}")
print("\nClean records ready for downstream processing:")
display(clean_records)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Query Quality Metrics

# COMMAND ----------

# DBTITLE 1,Cell 46
# Show quarantined records with metadata
# Add quarantine metadata to failed records
quarantined_with_metadata = quarantined_records.withColumn(
    "dqx_quarantine_timestamp",
    F.lit(datetime.now())
).withColumn(
    "dqx_quarantine_reason",
    F.col("dqx_failed_checks")
).withColumn(
    "partition_status",
    F.col("status")
)

print(f"✅ Quarantined records with metadata: {quarantined_with_metadata.count()}")
print("\nQuarantine table structure:")
display(quarantined_with_metadata.select(
    "transaction_id", "customer_id", "amount", "status",
    "dqx_quarantine_timestamp", "dqx_quarantine_reason", "dqx_failed_checks"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Real-Time Quality Dashboard
# MAGIC
# MAGIC ### 5.1 Quality KPIs by Time Window

# COMMAND ----------

# DBTITLE 1,Cell 38
# Quality KPIs summary
print("\n=== Quality KPIs ===")
print(f"Total Records Processed: {total_records}")
print(f"Passed Records: {passed_records}")
print(f"Failed Records: {failed_records}")
print(f"Warning Records: {warning_records}")
print(f"Pass Rate: {pass_rate:.2%}")
print(f"\nData Quality Score: {pass_rate * 100:.1f}/100")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Failed Checks Analysis

# COMMAND ----------

# DBTITLE 1,Cell 40
# Analyze which checks are failing
print("\n=== Failed Checks Analysis ===")

check_columns = [c for c in validated_batch.columns if c.startswith('check_')]
for check_col in check_columns:
    failed_count = validated_batch.filter(F.col(check_col) == "FAILED").count()
    warning_count = validated_batch.filter(F.col(check_col) == "WARNING").count()
    
    if failed_count > 0:
        print(f"  {check_col}: {failed_count} failures")
    if warning_count > 0:
        print(f"  {check_col}: {warning_count} warnings")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Pass Rate Trend

# COMMAND ----------

# DBTITLE 1,Cell 42
# Show pass rate and quality status distribution
quality_distribution = (
    validated_batch
    .groupBy("dqx_quality_status")
    .agg(F.count("*").alias("record_count"))
    .withColumn("percentage", F.col("record_count") / total_records * 100)
    .orderBy(F.desc("record_count"))
)

print("\nQuality Status Distribution:")
display(quality_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Streaming Quarantine Pattern
# MAGIC
# MAGIC ### 6.1 Configure Streaming Quarantine

# COMMAND ----------

# DBTITLE 1,Cell 44
# Demonstrate quarantine pattern with batch data
# In streaming, failed records would be written to a separate quarantine table

quarantine_config_streaming = {
    'target_table': 'dqx_demo.streaming_quarantine',
    'partition_by': ['status'],
    'add_metadata': True
}

print("✅ Quarantine configuration for streaming:")
print(f"  Target table: {quarantine_config_streaming['target_table']}")
print(f"  Partition by: {quarantine_config_streaming['partition_by']}")
print(f"  Metadata: {quarantine_config_streaming['add_metadata']}")
print("\nIn streaming, failed records would be continuously written to this table")

# COMMAND ----------

# DBTITLE 1,Cell 45
# MAGIC %md
# MAGIC %undefined
# MAGIC ### 6.2 Quarantine Records with Metadata
# MAGIC
# MAGIC Demonstrate what quarantined records would look like:

# COMMAND ----------

# DBTITLE 1,Cell 46
# Demonstrate quarantine with metadata
# Add quarantine timestamp and partition info to failed records
quarantined_with_metadata = quarantined_records.withColumn(
    "dqx_quarantine_timestamp",
    F.lit(datetime.now())
).withColumn(
    "dqx_quarantine_reason",
    F.col("dqx_failed_checks")
).withColumn(
    "partition_status",
    F.col("status")
)

print(f"✅ Quarantined records with metadata: {quarantined_with_metadata.count()}")
print(f"\nIn streaming, these records would be written to:")
print(f"  Table: {quarantine_config_streaming['target_table']}")
print(f"  Partitioned by: {quarantine_config_streaming['partition_by']}")
print("\nQuarantine record structure:")
display(quarantined_with_metadata.select(
    "transaction_id", "customer_id", "amount", "discount", "status",
    "dqx_quarantine_timestamp", "dqx_quarantine_reason", "dqx_failed_checks"
))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Query Streaming Quarantine

# COMMAND ----------

# DBTITLE 1,Cell 48
# Show quarantine data summary
print(f"Total quarantined records: {quarantined_with_metadata.count()}")
print("\nQuarantined records details:")
display(quarantined_with_metadata.select(
    "transaction_id", "customer_id", "amount", "discount", "status",
    "dqx_failed_checks", "dqx_quarantine_timestamp"
).orderBy(F.col("transaction_id")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Analyze Quarantine by Reason

# COMMAND ----------

# DBTITLE 1,Cell 50
# Group quarantine by failed checks
quarantine_by_reason = (
    quarantined_with_metadata
    .groupBy("dqx_failed_checks")
    .agg(F.count("*").alias("count"))
    .orderBy(F.desc("count"))
)

print("Quarantined records by failure reason:")
display(quarantine_by_reason)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Handling Late Data
# MAGIC
# MAGIC ### 7.1 Configure Watermarking

# COMMAND ----------

# DBTITLE 1,Cell 52
# Demonstrate watermarking concept
print("✅ Watermarking for Late Data Handling")
print("\nIn streaming, watermarks help manage late-arriving data:")
print("  - Define how late data can arrive (e.g., 10 minutes)")
print("  - Data older than watermark is dropped or handled separately")
print("  - Essential for windowed aggregations")
print("\nExample configuration:")
print("  .withWatermark('timestamp', '10 minutes')")
print("\nThis ensures:")
print("  - Records within 10 minutes of latest timestamp are processed")
print("  - Older records are considered too late and handled accordingly")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Validate with Watermark

# COMMAND ----------

# DBTITLE 1,Cell 54
# Show how watermarking would work with our data
print("\nWatermark Example with Sample Data:")
print(f"Latest timestamp: {validated_batch.agg(F.max('timestamp')).collect()[0][0]}")
print(f"Earliest timestamp: {validated_batch.agg(F.min('timestamp')).collect()[0][0]}")
print("\nWith a 10-minute watermark:")
print("  - All records in this batch would be within the watermark")
print("  - In production streaming, records arriving >10 min late would be dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Cleanup

# COMMAND ----------

# DBTITLE 1,Cell 56
# Check for any active streaming queries
active_streams = spark.streams.active

if len(active_streams) > 0:
    print(f"Active streaming queries: {len(active_streams)}")
    for query in active_streams:
        print(f"  - Stream ID: {query.id}")
        query.stop()
    print("✅ All streaming queries stopped")
else:
    print("✅ No active streaming queries to stop")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **StreamingValidator** handles micro-batch validation automatically
# MAGIC
# MAGIC ✅ **Quality metrics stream** provides real-time monitoring
# MAGIC
# MAGIC ✅ **Streaming quarantine** isolates bad records continuously
# MAGIC
# MAGIC ✅ **Checkpoints are essential** for streaming fault tolerance
# MAGIC
# MAGIC ✅ **Watermarks help** manage late-arriving data
# MAGIC
# MAGIC ✅ **Dashboard queries** enable real-time quality visualization
# MAGIC
# MAGIC ✅ **Same DQX checks** work for both batch and streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC 1. **Always use checkpoints** - Essential for recovery
# MAGIC 2. **Monitor metrics continuously** - Set up real-time dashboards
# MAGIC 3. **Configure watermarks** - Handle late data appropriately
# MAGIC 4. **Partition quarantine tables** - Improve query performance
# MAGIC 5. **Test trigger intervals** - Balance latency and throughput
# MAGIC 6. **Use structured streaming UI** - Monitor progress and lag
# MAGIC 7. **Alert on quality degradation** - Automated monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Lakeflow Pipelines (DLT)** integration with DQX
# MAGIC 2. **Medallion architecture** with quality checks
# MAGIC 3. **Combining DLT expectations** with DQX validations
# MAGIC 4. **Pipeline observability** and monitoring
# MAGIC
# MAGIC **Continue to**: `06_Lakeflow_Pipelines_DLT_Integration`
