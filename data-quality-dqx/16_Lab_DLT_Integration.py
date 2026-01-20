# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: DLT Integration
# MAGIC
# MAGIC **Hands-On Lab Exercise 6**
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
# MAGIC 1. Build DLT pipelines with DQX integration
# MAGIC 2. Implement medallion architecture with quality checks
# MAGIC 3. Combine DLT expectations and DQX validations
# MAGIC 4. Track quality metrics in DLT
# MAGIC 5. Monitor pipeline health

# COMMAND ----------

# MAGIC %md
# MAGIC ## Important Note
# MAGIC
# MAGIC This lab requires creating a DLT pipeline. You'll need to:
# MAGIC 1. Save this notebook
# MAGIC 2. Create a DLT pipeline in the Databricks UI
# MAGIC 3. Configure the pipeline to use this notebook
# MAGIC 4. Run the pipeline

# COMMAND ----------

import dlt
from dqx import DLTValidator, CheckType, Reaction, Rule
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Bronze Layer with Basic Checks (20 minutes)
# MAGIC
# MAGIC ### Task 1.1: Create Bronze Layer Table

# COMMAND ----------

@dlt.table(
    name="bronze_sales",
    comment="Raw sales data from source systems"
)
def bronze_sales():
    """
    TODO: Implement bronze layer that:
    - Reads from /tmp/dqx_lab/sales_source/
    - Uses cloudFiles for incremental loading
    - Loads data as-is with minimal transformation
    """
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Add Basic DLT Expectations

# COMMAND ----------

@dlt.table(
    name="bronze_sales_with_expectations",
    comment="Bronze sales with basic DLT expectations"
)
# TODO: Add DLT expectations:
# - "valid_timestamp": transaction_date IS NOT NULL
# - "positive_amount": amount > 0
@dlt.expect_all({
    # Your expectations here
})
def bronze_sales_with_expectations():
    """
    Bronze layer with DLT expectations
    """
    return dlt.read_stream("bronze_sales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Silver Layer with DQX Validation (30 minutes)
# MAGIC
# MAGIC ### Task 2.1: Implement Silver Layer with Comprehensive Validation

# COMMAND ----------

@dlt.table(
    name="silver_sales",
    comment="Validated and cleaned sales data"
)
def silver_sales():
    """
    TODO: Implement silver layer with DQX validation:
    1. Read from bronze_sales
    2. Create DLTValidator
    3. Add comprehensive quality checks:
       - NOT NULL checks for critical fields
       - UNIQUE check for transaction_id
       - Range checks for amount and quantity
       - IN_SET checks for status and payment_method
       - Row-level check: total = quantity * unit_price
    4. Return validated data
    """
    # Read from bronze
    bronze_df = dlt.read_stream("bronze_sales")
    
    # Create DQX validator
    validator = DLTValidator()
    
    # Your code here to add checks:
    
    
    # Validate and return
    return validator.validate_dlt(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Create Silver Layer with Enrichment

# COMMAND ----------

@dlt.table(
    name="silver_sales_enriched",
    comment="Silver sales with calculated fields"
)
def silver_sales_enriched():
    """
    TODO: Enrich silver data:
    - Add net_amount (amount - discount)
    - Add discount_percentage
    - Add processing_date
    - Add quality_score based on validation results
    """
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Gold Layer Aggregations (20 minutes)
# MAGIC
# MAGIC ### Task 3.1: Create Daily Summary

# COMMAND ----------

@dlt.table(
    name="gold_daily_sales_summary",
    comment="Daily sales summary metrics"
)
def gold_daily_sales_summary():
    """
    TODO: Create daily aggregations:
    - Total sales by date, store, product_category
    - Transaction counts
    - Average transaction values
    - Discount summaries
    """
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Create Customer Summary

# COMMAND ----------

@dlt.table(
    name="gold_customer_summary",
    comment="Customer sales summary"
)
def gold_customer_summary():
    """
    TODO: Create customer-level aggregations:
    - Total purchases per customer
    - Average order value
    - First and last purchase dates
    - Customer lifetime value
    """
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Quality Metrics Tracking (20 minutes)
# MAGIC
# MAGIC ### Task 4.1: Create Quality Metrics Table

# COMMAND ----------

@dlt.table(
    name="quality_metrics",
    comment="Data quality metrics from DQX validation"
)
def quality_metrics():
    """
    TODO: Capture quality metrics from DLT validation
    """
    return DLTValidator.get_metrics_table()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Create Quality Summary View

# COMMAND ----------

@dlt.view(
    name="quality_summary_view"
)
def quality_summary_view():
    """
    TODO: Create a view that summarizes quality metrics:
    - Latest pass rates by table
    - Failed check counts
    - Quality trends
    """
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Combining DLT and DQX (25 minutes)
# MAGIC
# MAGIC ### Task 5.1: Use Both DLT Expectations and DQX

# COMMAND ----------

@dlt.table(
    name="combined_validation_example"
)
@dlt.expect_all({
    "valid_date": "transaction_date IS NOT NULL",
    "positive_amount": "amount > 0"
})
def combined_validation_example():
    """
    TODO: Combine DLT expectations (simple checks) with DQX (complex checks):
    - Use DLT for simple SQL-based validations
    - Use DQX for:
      - Pattern matching (email, phone formats)
      - Complex business rules
      - Cross-column validations
    """
    df = dlt.read_stream("bronze_sales")
    
    # Apply DQX validations
    validator = DLTValidator()
    
    # Your code here:
    
    
    return validator.validate_dlt(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Create Quarantine Table

# COMMAND ----------

@dlt.table(
    name="quarantined_sales",
    comment="Sales records that failed quality validation"
)
def quarantined_sales():
    """
    TODO: Create table for quarantined records
    This would be populated by DQX quarantine mechanism
    """
    # Note: In production, this would be configured in the validator
    # For this lab, create a placeholder or query actual quarantine table
    # Your code here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Pipeline Configuration and Monitoring (Bonus)
# MAGIC
# MAGIC ### Task 6.1: Document Pipeline Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Document your DLT pipeline configuration (create in UI):
# MAGIC
# MAGIC ```json
# MAGIC {
# MAGIC   "name": "dqx_sales_quality_pipeline",
# MAGIC   "storage": "/mnt/datalake/pipelines/dqx_lab",
# MAGIC   "target": "dqx_lab_dlt",
# MAGIC   "continuous": false,
# MAGIC   "development": true,
# MAGIC   "configuration": {
# MAGIC     "source_path": "/tmp/dqx_lab/sales_source",
# MAGIC     "dqx.metrics.enabled": "true"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.2: Create Sample Source Data

# COMMAND ----------

# Run this in a separate notebook to generate source data
# TODO: Create a separate notebook that generates sample sales data
# and writes it to /tmp/dqx_lab/sales_source/ for the pipeline to process

# Example code (run separately):
# ```python
# # Generate sample sales data
# import random
# from datetime import datetime, timedelta
# 
# sales_data = []
# for i in range(1000):
#     sales_data.append({
#         "transaction_id": f"TXN{i:06d}",
#         "customer_id": f"CUST{random.randint(1,100):04d}",
#         "product_id": f"PROD{random.randint(1,50):03d}",
#         "quantity": random.randint(1, 10),
#         "unit_price": round(random.uniform(10, 500), 2),
#         "amount": 0,  # Will calculate
#         "discount": round(random.uniform(0, 50), 2),
#         "transaction_date": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
#         "store_id": f"STORE{random.randint(1, 10):02d}",
#         "payment_method": random.choice(["credit_card", "debit_card", "cash", "paypal"]),
#         "status": random.choice(["completed", "pending", "cancelled"])
#     })
#     sales_data[-1]["amount"] = sales_data[-1]["quantity"] * sales_data[-1]["unit_price"]
# 
# # Write to source location
# sales_df = spark.createDataFrame(sales_data)
# sales_df.write.mode("overwrite").json("/tmp/dqx_lab/sales_source/batch_001")
# ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.3: Monitor Pipeline Execution

# COMMAND ----------

# MAGIC %md
# MAGIC After running your DLT pipeline, answer these questions:
# MAGIC
# MAGIC 1. What is the overall pass rate for silver layer validation?
# MAGIC 2. Which quality checks failed most frequently?
# MAGIC 3. How many records were quarantined?
# MAGIC 4. What is the data quality trend over time?
# MAGIC 5. Are there any unexpected validation failures?
# MAGIC
# MAGIC Use the quality metrics table and DLT UI to investigate.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 6.4: Quality Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: After pipeline runs, query quality metrics
# MAGIC -- Write queries to:
# MAGIC -- 1. Show overall quality health
# MAGIC -- 2. Identify tables with quality issues
# MAGIC -- 3. Show quality trends
# MAGIC
# MAGIC -- SELECT * FROM dqx_lab_dlt.quality_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC ✅ DLT + DQX provides comprehensive quality validation
# MAGIC
# MAGIC ✅ Use DLT expectations for simple checks, DQX for complex
# MAGIC
# MAGIC ✅ Medallion architecture enables layered quality validation
# MAGIC
# MAGIC ✅ Quality metrics track validation results over time
# MAGIC
# MAGIC ✅ DLT UI provides pipeline observability
# MAGIC
# MAGIC ✅ Quarantine tables isolate bad data for review
# MAGIC
# MAGIC ### Best Practices
# MAGIC 1. **Bronze**: Basic format and schema checks
# MAGIC 2. **Silver**: Comprehensive business logic validation
# MAGIC 3. **Gold**: Aggregate consistency checks
# MAGIC 4. **Monitor**: Track quality metrics continuously
# MAGIC 5. **Alert**: Set up notifications for quality degradation
# MAGIC
# MAGIC ### Challenge Questions
# MAGIC 1. How do you handle schema evolution with quality checks?
# MAGIC 2. What's the right balance between DLT and DQX checks?
# MAGIC 3. How do you version quality rules in DLT pipelines?
# MAGIC 4. What metrics indicate pipeline health vs data quality?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Lab 7: Quality Dashboard** to learn about:
# MAGIC - Collecting comprehensive quality metrics
# MAGIC - Building quality dashboards
# MAGIC - Trend analysis and alerting
# MAGIC - Quality reporting
