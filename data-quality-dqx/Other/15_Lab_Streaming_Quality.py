# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Streaming Quality
# MAGIC
# MAGIC **Hands-On Lab Exercise 5**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 90 minutes    |
# MAGIC | Level           | 400           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Objectives
# MAGIC
# MAGIC In this lab, you will:
# MAGIC 1. Set up streaming data sources
# MAGIC 2. Apply real-time quality validation
# MAGIC 3. Implement streaming quarantine
# MAGIC 4. Monitor quality metrics in real-time
# MAGIC 5. Handle late-arriving data

# COMMAND ----------

# Import required libraries
from dqx import StreamingValidator, CheckType, Reaction, QuarantineConfig
from pyspark.sql import functions as F
from pyspark.sql.types import *
import time
import random
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Streaming Environment

# COMMAND ----------

# TODO: Create directories for streaming source and checkpoints
streaming_path = "/tmp/dqx_lab_streaming/source"
checkpoint_path = "/tmp/dqx_lab_streaming/checkpoints"

# Clean up if exists
dbutils.fs.rm(streaming_path, recurse=True)
dbutils.fs.rm(checkpoint_path, recurse=True)

# Create directories
dbutils.fs.mkdirs(streaming_path)
dbutils.fs.mkdirs(checkpoint_path)

print(f"✅ Streaming directories created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Streaming Data Source Setup (20 minutes)
# MAGIC
# MAGIC ### Task 1.1: Create Data Generator

# COMMAND ----------

# TODO: Create a function to generate IoT sensor data
# Schema: sensor_id, temperature, humidity, pressure, battery_level, timestamp, location
# Include quality issues:
# - Some sensors with null IDs
# - Temperature out of range (-50 to 150)
# - Humidity out of range (0 to 100)
# - Battery level (0 to 100)
# - Invalid locations

def generate_sensor_batch(batch_id, num_records=50, error_rate=0.15):
    """
    Generate IoT sensor data batch with controlled errors
    """
    # Your code here:
    pass

# Test the generator
test_batch = generate_sensor_batch(1, 10)
display(test_batch)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Write Initial Data Batches

# COMMAND ----------

# TODO: Generate and write 5 batches of sensor data to streaming source

for batch_id in range(1, 6):
    # Generate batch
    batch_df = generate_sensor_batch(batch_id, 50)
    
    # Write to streaming source
    output_path = f"{streaming_path}/batch_{batch_id:04d}.json"
    batch_df.coalesce(1).write.mode("overwrite").json(output_path)
    
    print(f"✅ Generated batch {batch_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.3: Create Streaming DataFrame

# COMMAND ----------

# TODO: Create streaming DataFrame from the source path
# Define appropriate schema
sensor_schema = StructType([
    # Your schema here
])

streaming_df = (
    spark.readStream
    .schema(sensor_schema)
    .option("maxFilesPerTrigger", 1)
    .json(streaming_path)
)

print(f"✅ Streaming DataFrame created")
print(f"Is streaming: {streaming_df.isStreaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Real-Time Quality Validation (25 minutes)
# MAGIC
# MAGIC ### Task 2.1: Create Streaming Validator

# COMMAND ----------

# TODO: Create StreamingValidator with quality checks:
# 1. sensor_id NOT NULL (error)
# 2. temperature BETWEEN -50 and 150 (error)
# 3. humidity BETWEEN 0 and 100 (warning)
# 4. battery_level BETWEEN 0 and 100 (warning)
# 5. location IN_SET of valid locations (warning)

streaming_validator = StreamingValidator()

# Your code here:


print("✅ Streaming validator configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Apply Validation to Stream

# COMMAND ----------

# TODO: Apply validation to the streaming DataFrame

validated_stream = streaming_validator.validate_stream(streaming_df)

print("✅ Validation applied to stream")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Write Clean Data Stream

# COMMAND ----------

# TODO: Write validated stream to Delta table
# Table: dqx_lab.clean_sensor_data

clean_query = (
    validated_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/clean_data")
    .trigger(processingTime="5 seconds")
    .table("dqx_lab.clean_sensor_data")
)

print(f"✅ Clean data stream started")
print(f"Stream ID: {clean_query.id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Streaming Quality Metrics (20 minutes)
# MAGIC
# MAGIC ### Task 3.1: Write Metrics Stream

# COMMAND ----------

# TODO: Get metrics stream from validator and write to table

metrics_stream = streaming_validator.get_metrics_stream()

metrics_query = (
    metrics_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/metrics")
    .trigger(processingTime="5 seconds")
    .table("dqx_lab.sensor_quality_metrics")
)

print(f"✅ Metrics stream started")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Generate More Streaming Data

# COMMAND ----------

# TODO: Generate additional batches to simulate continuous stream

for batch_id in range(6, 11):
    print(f"Generating batch {batch_id}...")
    batch_df = generate_sensor_batch(batch_id, 50)
    output_path = f"{streaming_path}/batch_{batch_id:04d}.json"
    batch_df.coalesce(1).write.mode("overwrite").json(output_path)
    time.sleep(3)  # Wait between batches

print("✅ Additional batches generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Query and Analyze Metrics

# COMMAND ----------

# TODO: Wait for processing, then query metrics
time.sleep(15)

# Query metrics
metrics_df = spark.table("dqx_lab.sensor_quality_metrics")

# Analyze:
# 1. Overall pass rate over time
# 2. Which checks fail most frequently
# 3. Quality trends

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Streaming Quarantine (25 minutes)
# MAGIC
# MAGIC ### Task 4.1: Stop Previous Streams

# COMMAND ----------

# TODO: Stop all active streaming queries

for query in spark.streams.active:
    print(f"Stopping stream: {query.id}")
    query.stop()

time.sleep(5)
print("✅ All streams stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Configure Streaming Quarantine

# COMMAND ----------

# TODO: Create new streaming source
streaming_df_2 = (
    spark.readStream
    .schema(sensor_schema)
    .option("maxFilesPerTrigger", 1)
    .json(streaming_path)
)

# Create validator with quarantine
quarantine_validator = StreamingValidator()

# Configure quarantine
quarantine_config = QuarantineConfig(
    target_table="dqx_lab.quarantine_sensor_data",
    partition_by=["location"],
    add_metadata=True
)

# Add checks with quarantine reaction
# Your code here:


print("✅ Streaming validator with quarantine configured")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.3: Start Streaming with Quarantine

# COMMAND ----------

# TODO: Apply validation and start streams

validated_with_quarantine = quarantine_validator.validate_stream(streaming_df_2)

# Write clean data
clean_query_2 = (
    validated_with_quarantine
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/clean_with_quar")
    .trigger(processingTime="5 seconds")
    .table("dqx_lab.clean_sensor_data_quar")
)

# Generate more data with higher error rate
for batch_id in range(11, 16):
    print(f"Generating batch {batch_id}...")
    batch_df = generate_sensor_batch(batch_id, 50, error_rate=0.30)  # Higher errors
    output_path = f"{streaming_path}/batch_{batch_id:04d}.json"
    batch_df.coalesce(1).write.mode("overwrite").json(output_path)
    time.sleep(3)

print("✅ More data generated")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.4: Analyze Quarantine

# COMMAND ----------

# TODO: Wait for processing then analyze quarantine
time.sleep(15)

quarantine_df = spark.table("dqx_lab.quarantine_sensor_data")

# Analyze:
# 1. Total quarantined records
# 2. Quarantine reasons
# 3. Patterns in quarantined data

# Your code here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Real-Time Quality Dashboard (20 minutes)
# MAGIC
# MAGIC ### Task 5.1: Create Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Write queries for real-time dashboard
# MAGIC
# MAGIC -- 1. Quality metrics by time window
# MAGIC SELECT 
# MAGIC   window(timestamp, '1 minute') as time_window,
# MAGIC   AVG(pass_rate) as avg_pass_rate,
# MAGIC   SUM(records_processed) as total_records,
# MAGIC   SUM(records_failed) as total_failed
# MAGIC FROM dqx_lab.sensor_quality_metrics
# MAGIC GROUP BY window(timestamp, '1 minute')
# MAGIC ORDER BY time_window DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Top failing sensors
# MAGIC -- Your query here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Create Quality Alert Logic

# COMMAND ----------

# TODO: Implement alert logic:
# - Alert if pass rate < 85%
# - Alert if more than 10 records quarantined in 1 minute
# - Alert on specific sensor failures

def check_quality_alerts():
    """
    Check for quality issues requiring alerts
    """
    # Your code here:
    pass

# Test alert logic
check_quality_alerts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Handling Late Data (Bonus - 30 minutes)
# MAGIC
# MAGIC ### Task 6.1: Create Data with Watermarks

# COMMAND ----------

# TODO: Stop existing streams
for query in spark.streams.active:
    query.stop()
time.sleep(5)

# Create streaming source with watermark
streaming_with_watermark = (
    spark.readStream
    .schema(sensor_schema)
    .option("maxFilesPerTrigger", 1)
    .json(streaming_path)
    .withWatermark("timestamp", "10 minutes")
)

print("✅ Streaming DataFrame with watermark created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2: Test Late Arriving Data

# COMMAND ----------

# TODO: Generate data with timestamps in the past
def generate_late_data(batch_id, delay_minutes=15):
    """
    Generate data with old timestamps (simulating late arrival)
    """
    # Your code here:
    pass

# Generate and test with late data
late_batch = generate_late_data(20, delay_minutes=15)
# Process and observe behavior

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.3: Analyze Watermark Behavior

# COMMAND ----------

# TODO: Answer:
# 1. What happens to records older than watermark?
# 2. How does watermark affect quality validation?
# 3. What's the right watermark setting for your use case?

# Your analysis here:


# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# TODO: Stop all streaming queries
for query in spark.streams.active:
    print(f"Stopping stream: {query.id}")
    query.stop()

print("✅ All streams stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC ✅ StreamingValidator handles micro-batch validation
# MAGIC
# MAGIC ✅ Quality metrics stream enables real-time monitoring
# MAGIC
# MAGIC ✅ Streaming quarantine isolates bad records continuously
# MAGIC
# MAGIC ✅ Checkpoints are essential for fault tolerance
# MAGIC
# MAGIC ✅ Watermarks help manage late-arriving data
# MAGIC
# MAGIC ✅ Real-time dashboards support proactive quality management
# MAGIC
# MAGIC ### Challenge Questions
# MAGIC 1. How do you balance throughput and quality check latency?
# MAGIC 2. What's appropriate trigger interval for your use case?
# MAGIC 3. How do you handle stream restarts and replays?
# MAGIC 4. When should you use watermarks vs accept all data?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Lab 6: DLT Integration** to learn about:
# MAGIC - Building DLT pipelines with DQX
# MAGIC - Implementing medallion architecture
# MAGIC - Combining DLT expectations and DQX
# MAGIC - Pipeline observability
