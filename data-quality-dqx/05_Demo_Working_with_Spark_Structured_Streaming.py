# Databricks notebook source
# MAGIC %md
# MAGIC # Working with Spark Structured Streaming
# MAGIC
# MAGIC **Module 6: Real-Time Data Quality**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
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

# Import required libraries
from dqx import StreamingValidator, CheckType, Reaction, QuarantineConfig
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time

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

# Create directory for streaming source
streaming_path = "/tmp/dqx_streaming_demo"
checkpoint_path = "/tmp/dqx_streaming_checkpoint"

# Clean up if exists
dbutils.fs.rm(streaming_path, recurse=True)
dbutils.fs.rm(checkpoint_path, recurse=True)

# Create directory
dbutils.fs.mkdirs(streaming_path)
dbutils.fs.mkdirs(checkpoint_path)

print(f"✅ Created streaming directories:")
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

import random
from datetime import datetime, timedelta

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
    
    # Write as JSON to simulate streaming source
    output_path = f"{streaming_path}/batch_{batch_id:04d}.json"
    df.coalesce(1).write.mode("overwrite").json(output_path)
    
    return df

# Generate first batch
first_batch = generate_transaction_batch(1, 20)
print("✅ Generated first batch of streaming data")
display(first_batch)

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

# Create streaming validator
streaming_validator = StreamingValidator()

print("✅ StreamingValidator created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Add Quality Checks

# COMMAND ----------

# Add quality checks for streaming data
streaming_validator.add_check(
    name="transaction_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="transaction_id",
    level="error"
)

streaming_validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error",
    reaction=Reaction.QUARANTINE
)

streaming_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error"
)

streaming_validator.add_check(
    name="valid_status",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "processing", "completed", "cancelled", "refunded"],
    level="warning"
)

# Add row-level check
from dqx import Rule

streaming_validator.add_rule(
    Rule(
        name="discount_validation",
        expression="discount <= amount * 0.5",
        level="error",
        description="Discount cannot exceed 50% of amount"
    )
)

print("✅ Added quality checks to streaming validator")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Apply Streaming Validation

# COMMAND ----------

# Apply validation to streaming DataFrame
validated_stream = streaming_validator.validate_stream(streaming_df)

print("✅ Validation applied to stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Write Clean Data Stream

# COMMAND ----------

# Write validated stream to Delta table
clean_stream_query = (
    validated_stream
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

# Get quality metrics stream
metrics_stream = streaming_validator.get_metrics_stream()

# Write metrics to Delta table
metrics_query = (
    metrics_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/quality_metrics")
    .trigger(processingTime="5 seconds")
    .table("dqx_demo.streaming_quality_metrics")
)

print("✅ Quality metrics stream started")
print(f"   Stream ID: {metrics_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Generate More Streaming Data

# COMMAND ----------

# Generate additional batches to simulate streaming
for batch_id in range(2, 6):
    print(f"Generating batch {batch_id}...")
    generate_transaction_batch(batch_id, 15)
    time.sleep(2)  # Wait between batches

print("✅ Generated additional streaming batches")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Query Clean Transactions

# COMMAND ----------

# Wait for processing
time.sleep(10)

# Query clean transactions
clean_transactions = spark.table("dqx_demo.clean_transactions_stream")

print(f"Total clean transactions: {clean_transactions.count()}")
display(clean_transactions.orderBy(F.desc("timestamp")).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Query Quality Metrics

# COMMAND ----------

# Query streaming quality metrics
quality_metrics = spark.table("dqx_demo.streaming_quality_metrics")

print(f"Total metric records: {quality_metrics.count()}")
display(quality_metrics.orderBy(F.desc("timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Real-Time Quality Dashboard
# MAGIC
# MAGIC ### 5.1 Quality KPIs by Time Window

# COMMAND ----------

# Calculate quality KPIs over time
quality_over_time = (
    quality_metrics
    .groupBy(F.window("timestamp", "30 seconds"))
    .agg(
        F.sum("records_processed").alias("total_records"),
        F.sum("records_failed").alias("failed_records"),
        F.avg("pass_rate").alias("avg_pass_rate")
    )
    .orderBy("window")
)

print("Quality metrics over time:")
display(quality_over_time)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Failed Checks Analysis

# COMMAND ----------

# Analyze which checks are failing
failed_checks_summary = (
    quality_metrics
    .filter("records_failed > 0")
    .groupBy("check_name", "check_type")
    .agg(
        F.sum("records_failed").alias("total_failures"),
        F.count("*").alias("failure_occurrences")
    )
    .orderBy(F.desc("total_failures"))
)

print("Failed checks summary:")
display(failed_checks_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Pass Rate Trend

# COMMAND ----------

# Visualize pass rate trend
pass_rate_trend = (
    quality_metrics
    .select(
        F.col("timestamp"),
        F.col("pass_rate"),
        F.col("records_processed"),
        F.col("records_failed")
    )
    .orderBy("timestamp")
)

print("Pass rate trend:")
display(pass_rate_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Streaming Quarantine Pattern
# MAGIC
# MAGIC ### 6.1 Configure Streaming Quarantine

# COMMAND ----------

# Stop previous streams
clean_stream_query.stop()
metrics_query.stop()

# Wait for streams to stop
time.sleep(5)

# Create new streaming source
streaming_df_2 = (
    spark.readStream
    .schema(transaction_schema)
    .option("maxFilesPerTrigger", 1)
    .json(streaming_path)
)

# Create streaming validator with quarantine
quarantine_validator = StreamingValidator()

# Configure quarantine
quarantine_config = QuarantineConfig(
    target_table="dqx_demo.streaming_quarantine",
    partition_by=["status"],
    add_metadata=True
)

# Add checks with quarantine
quarantine_validator.add_check(
    name="customer_id_required",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_config
)

quarantine_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_config
)

print("✅ Streaming validator with quarantine configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Start Streaming with Quarantine

# COMMAND ----------

# Apply validation with quarantine
validated_with_quarantine = quarantine_validator.validate_stream(streaming_df_2)

# Write clean stream
clean_query_2 = (
    validated_with_quarantine
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/clean_with_quarantine")
    .trigger(processingTime="5 seconds")
    .table("dqx_demo.clean_with_quarantine")
)

print("✅ Streaming with quarantine started")

# Generate more data
for batch_id in range(6, 9):
    print(f"Generating batch {batch_id}...")
    generate_transaction_batch(batch_id, 15)
    time.sleep(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Query Streaming Quarantine

# COMMAND ----------

# Wait for processing
time.sleep(10)

# Query quarantine table
quarantine_data = spark.table("dqx_demo.streaming_quarantine")

print(f"Total quarantined records: {quarantine_data.count()}")
display(quarantine_data.orderBy(F.desc("dqx_quarantine_timestamp")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 Analyze Quarantine by Reason

# COMMAND ----------

# Group quarantine by failed checks
quarantine_by_reason = (
    quarantine_data
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

# Create streaming source with watermark
streaming_with_watermark = (
    spark.readStream
    .schema(transaction_schema)
    .option("maxFilesPerTrigger", 1)
    .json(streaming_path)
    .withWatermark("timestamp", "10 minutes")
)

print("✅ Streaming DataFrame with watermark created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Validate with Watermark

# COMMAND ----------

# Create validator
watermark_validator = StreamingValidator()

watermark_validator.add_check(
    name="amount_positive",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error"
)

# Apply validation
validated_with_watermark = watermark_validator.validate_stream(streaming_with_watermark)

# Write with watermark
watermark_query = (
    validated_with_watermark
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/with_watermark")
    .trigger(processingTime="5 seconds")
    .table("dqx_demo.transactions_with_watermark")
)

print("✅ Streaming with watermark started")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 8: Cleanup

# COMMAND ----------

# Stop all streaming queries
for query in spark.streams.active:
    print(f"Stopping stream: {query.id}")
    query.stop()

print("✅ All streaming queries stopped")

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
# MAGIC **Continue to**: `06_Demo_Lakeflow_Pipelines_DLT_Integration`
