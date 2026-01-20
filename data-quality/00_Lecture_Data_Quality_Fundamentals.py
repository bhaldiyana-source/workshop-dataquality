# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Fundamentals
# MAGIC
# MAGIC **Module 1: Foundation of Data Quality**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 30 minutes    |
# MAGIC | Level           | 200           |
# MAGIC | Type            | Lecture       |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will understand:
# MAGIC
# MAGIC 1. **The Six Dimensions of Data Quality** - Key aspects that define quality data
# MAGIC 2. **Data Quality Framework Components** - Building blocks of a quality system
# MAGIC 3. **Quality Metrics and KPIs** - How to measure data quality
# MAGIC 4. **Business Impact** - Why data quality matters to the organization

# COMMAND ----------

# MAGIC %md
# MAGIC ## The Six Dimensions of Data Quality
# MAGIC
# MAGIC Data quality is measured across six fundamental dimensions:

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Accuracy
# MAGIC **Definition**: Correctness of data values
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - Are values correct and truthful?
# MAGIC - Do they match real-world entities?
# MAGIC - Are calculations performed correctly?
# MAGIC
# MAGIC **Examples**:
# MAGIC - Valid email format: `user@example.com` ✅ vs `user@example` ❌
# MAGIC - Correct calculations: `total = sum(line_items)` ✅
# MAGIC - Valid geographic coordinates: `(37.7749, -122.4194)` ✅

# COMMAND ----------

# Example: Checking accuracy
from pyspark.sql.functions import col, regexp_extract

# Sample data with accuracy issues
data = [
    ("john@example.com", 100, "USA"),
    ("invalid-email", 200, "USA"),
    ("jane@test.com", -50, "Canada"),  # Negative amount - accuracy issue
    ("bob@company.com", 300, "XYZ")    # Invalid country code
]

df = spark.createDataFrame(data, ["email", "amount", "country"])

# Check email accuracy
valid_email_pattern = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
df_with_validation = df.withColumn(
    "email_valid",
    col("email").rlike(valid_email_pattern)
)

display(df_with_validation)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Completeness
# MAGIC **Definition**: Presence of required data
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - Are all mandatory fields populated?
# MAGIC - Is data missing randomly or systematically?
# MAGIC - What percentage of records are complete?
# MAGIC
# MAGIC **Examples**:
# MAGIC - Required customer ID populated ✅
# MAGIC - NULL in optional middle name field ✅
# MAGIC - NULL in required order amount ❌

# COMMAND ----------

# Example: Checking completeness
from pyspark.sql.functions import isnan, isnull, when, count, col

data = [
    (1, "John", "Doe", 100),
    (2, "Jane", None, 200),     # Missing last name
    (3, "Bob", "Smith", None),  # Missing amount
    (None, "Alice", "Wong", 150) # Missing ID
]

df = spark.createDataFrame(data, ["id", "first_name", "last_name", "amount"])

# Calculate completeness metrics
completeness = df.select([
    count(when(col(c).isNull(), c)).alias(f"{c}_null_count") 
    for c in df.columns
])

display(completeness)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Consistency
# MAGIC **Definition**: Agreement across datasets and within records
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - Do related values agree?
# MAGIC - Is data consistent across systems?
# MAGIC - Are cross-field rules satisfied?
# MAGIC
# MAGIC **Examples**:
# MAGIC - Ship date >= Order date ✅
# MAGIC - State matches ZIP code ✅
# MAGIC - Total = Sum of line items ✅

# COMMAND ----------

# Example: Checking consistency
from pyspark.sql.functions import datediff

data = [
    (1, "2024-01-01", "2024-01-05", True),   # Consistent
    (2, "2024-01-10", "2024-01-08", False),  # Ship before order!
    (3, "2024-01-15", "2024-01-20", True)
]

df = spark.createDataFrame(data, ["order_id", "order_date", "ship_date", "expected_consistent"])

df_consistency = df.withColumn(
    "is_consistent",
    col("ship_date") >= col("order_date")
).withColumn(
    "days_between",
    datediff(col("ship_date"), col("order_date"))
)

display(df_consistency)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Timeliness
# MAGIC **Definition**: Data recency and availability
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - Is data available when needed?
# MAGIC - How old is the data?
# MAGIC - Are SLAs being met?
# MAGIC
# MAGIC **Examples**:
# MAGIC - Real-time stock prices ✅
# MAGIC - Yesterday's sales report loaded today ✅
# MAGIC - Last month's inventory for today's decisions ❌

# COMMAND ----------

# Example: Checking timeliness
from pyspark.sql.functions import current_timestamp, hour, expr

data = [
    (1, "2024-01-10 08:00:00", "critical"),
    (2, "2024-01-09 14:00:00", "standard"),
    (3, "2024-01-05 10:00:00", "critical")  # Too old for critical data
]

df = spark.createDataFrame(data, ["id", "last_updated", "data_priority"])

df_timeliness = df.withColumn(
    "last_updated",
    col("last_updated").cast("timestamp")
).withColumn(
    "hours_old",
    expr("(unix_timestamp(current_timestamp()) - unix_timestamp(last_updated)) / 3600")
).withColumn(
    "is_timely",
    when((col("data_priority") == "critical") & (col("hours_old") <= 24), True)
    .when((col("data_priority") == "standard") & (col("hours_old") <= 72), True)
    .otherwise(False)
)

display(df_timeliness)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Validity
# MAGIC **Definition**: Data conforms to rules and standards
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - Does data meet defined formats?
# MAGIC - Are values within acceptable ranges?
# MAGIC - Do values match allowed categories?
# MAGIC
# MAGIC **Examples**:
# MAGIC - Date in ISO format: `2024-01-01` ✅
# MAGIC - Age between 0 and 120 ✅
# MAGIC - Status in ['pending', 'completed', 'cancelled'] ✅

# COMMAND ----------

# Example: Checking validity
from pyspark.sql.functions import col, when

data = [
    (1, 25, "completed", "2024-01-01"),
    (2, 150, "pending", "2024-13-45"),     # Invalid age and date
    (3, 35, "invalid_status", "2024-06-15"), # Invalid status
    (4, -5, "cancelled", "2024-03-20")     # Invalid age
]

df = spark.createDataFrame(data, ["id", "age", "status", "date_string"])

valid_statuses = ['pending', 'completed', 'cancelled']

df_validity = df.withColumn(
    "age_valid",
    (col("age") >= 0) & (col("age") <= 120)
).withColumn(
    "status_valid",
    col("status").isin(valid_statuses)
).withColumn(
    "overall_valid",
    col("age_valid") & col("status_valid")
)

display(df_validity)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6. Uniqueness
# MAGIC **Definition**: No unwanted duplicates
# MAGIC
# MAGIC **Key Questions**:
# MAGIC - Are records unique when they should be?
# MAGIC - Is there data redundancy?
# MAGIC - Are primary keys truly unique?
# MAGIC
# MAGIC **Examples**:
# MAGIC - One record per customer ID ✅
# MAGIC - Duplicate transaction records ❌
# MAGIC - Multiple addresses per customer (if allowed) ✅

# COMMAND ----------

# Example: Checking uniqueness
from pyspark.sql import Window
from pyspark.sql.functions import row_number, count

data = [
    (1, "john@example.com", "Order 1"),
    (2, "jane@example.com", "Order 2"),
    (1, "john@example.com", "Order 3"),  # Duplicate customer_id
    (3, "bob@example.com", "Order 4")
]

df = spark.createDataFrame(data, ["customer_id", "email", "order_desc"])

# Find duplicates
window = Window.partitionBy("customer_id").orderBy("order_desc")

df_uniqueness = df.withColumn(
    "row_num",
    row_number().over(window)
).withColumn(
    "is_duplicate",
    col("row_num") > 1
)

display(df_uniqueness)

# Summary of duplicates
duplicate_summary = df.groupBy("customer_id").agg(
    count("*").alias("occurrence_count")
).filter(col("occurrence_count") > 1)

display(duplicate_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Framework Components
# MAGIC
# MAGIC A comprehensive data quality framework consists of five key components:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────┐
# MAGIC │                   DATA QUALITY FRAMEWORK                 │
# MAGIC ├─────────────────────────────────────────────────────────┤
# MAGIC │                                                          │
# MAGIC │  1. PROFILING          → Understand current state       │
# MAGIC │     - Statistical analysis                               │
# MAGIC │     - Pattern detection                                  │
# MAGIC │     - Anomaly identification                             │
# MAGIC │                                                          │
# MAGIC │  2. VALIDATION         → Enforce quality rules          │
# MAGIC │     - Schema validation                                  │
# MAGIC │     - Business rules                                     │
# MAGIC │     - Data type checks                                   │
# MAGIC │                                                          │
# MAGIC │  3. MONITORING         → Track quality metrics          │
# MAGIC │     - Real-time dashboards                               │
# MAGIC │     - Quality scorecards                                 │
# MAGIC │     - Trend analysis                                     │
# MAGIC │                                                          │
# MAGIC │  4. REMEDIATION        → Fix quality issues             │
# MAGIC │     - Quarantine workflows                               │
# MAGIC │     - Data cleansing                                     │
# MAGIC │     - Automated fixes                                    │
# MAGIC │                                                          │
# MAGIC │  5. PREVENTION         → Stop issues at source          │
# MAGIC │     - Input validation                                   │
# MAGIC │     - Schema enforcement                                 │
# MAGIC │     - Quality gates                                      │
# MAGIC │                                                          │
# MAGIC └─────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Component 1: Profiling
# MAGIC Understanding your data's current state through statistical analysis

# COMMAND ----------

# Example: Basic data profiling
def profile_column(df, column_name):
    """Generate basic profile statistics for a column"""
    
    from pyspark.sql.functions import count, countDistinct, min, max, avg, stddev
    
    stats = df.select(
        count(column_name).alias("count"),
        countDistinct(column_name).alias("distinct_count"),
        count(when(col(column_name).isNull(), 1)).alias("null_count")
    ).collect()[0]
    
    total_count = df.count()
    
    print(f"Profile for column: {column_name}")
    print(f"  Total Records: {total_count}")
    print(f"  Non-Null Count: {stats['count']}")
    print(f"  Null Count: {stats['null_count']}")
    print(f"  Null %: {(stats['null_count']/total_count)*100:.2f}%")
    print(f"  Distinct Values: {stats['distinct_count']}")
    print(f"  Cardinality: {(stats['distinct_count']/stats['count'])*100:.2f}%")

# Test with sample data
sample_df = spark.createDataFrame([
    (1, 100, "A"),
    (2, 200, "B"),
    (3, None, "A"),
    (4, 400, "C"),
    (5, 500, "A")
], ["id", "amount", "category"])

profile_column(sample_df, "amount")
profile_column(sample_df, "category")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Component 2: Validation
# MAGIC Enforcing quality rules and business constraints

# COMMAND ----------

# Example: Simple validation framework
class SimpleValidator:
    """Basic validation engine"""
    
    def __init__(self, df):
        self.df = df
        self.results = []
    
    def check_not_null(self, column_name):
        """Validate that column has no nulls"""
        null_count = self.df.filter(col(column_name).isNull()).count()
        total = self.df.count()
        
        result = {
            'rule': f'NOT_NULL_{column_name}',
            'passed': null_count == 0,
            'failed_count': null_count,
            'failure_rate': null_count / total if total > 0 else 0
        }
        self.results.append(result)
        return self
    
    def check_range(self, column_name, min_val, max_val):
        """Validate that values are within range"""
        out_of_range = self.df.filter(
            (col(column_name) < min_val) | (col(column_name) > max_val)
        ).count()
        total = self.df.count()
        
        result = {
            'rule': f'RANGE_{column_name}_{min_val}_to_{max_val}',
            'passed': out_of_range == 0,
            'failed_count': out_of_range,
            'failure_rate': out_of_range / total if total > 0 else 0
        }
        self.results.append(result)
        return self
    
    def get_report(self):
        """Generate validation report"""
        print("\n" + "="*60)
        print("VALIDATION REPORT")
        print("="*60)
        for result in self.results:
            status = "✅ PASS" if result['passed'] else "❌ FAIL"
            print(f"{status} | {result['rule']}")
            if not result['passed']:
                print(f"    Failed: {result['failed_count']} records ({result['failure_rate']*100:.2f}%)")
        print("="*60)

# Test validator
validator = SimpleValidator(sample_df)
validator.check_not_null("id").check_not_null("amount").check_range("amount", 0, 1000)
validator.get_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality Metrics and KPIs
# MAGIC
# MAGIC ### Core Metrics
# MAGIC
# MAGIC | Metric | Description | Formula |
# MAGIC |--------|-------------|---------|
# MAGIC | **Quality Score** | Overall data quality percentage | (Passed Records / Total Records) × 100 |
# MAGIC | **Completeness Rate** | Percentage of non-null required fields | (Non-Null Values / Total Values) × 100 |
# MAGIC | **Accuracy Rate** | Percentage within expected ranges | (Valid Values / Total Values) × 100 |
# MAGIC | **Timeliness** | Average data latency | Avg(Current Time - Record Timestamp) |
# MAGIC | **Duplication Rate** | Percentage of duplicate records | (Duplicates / Total Records) × 100 |
# MAGIC | **Schema Drift** | Count of unexpected schema changes | Count of Schema Violations |

# COMMAND ----------

# Example: Calculate quality metrics
def calculate_quality_metrics(df, required_columns):
    """Calculate comprehensive quality metrics"""
    
    total_records = df.count()
    total_fields = len(required_columns)
    
    metrics = {
        'total_records': total_records,
        'total_fields': total_fields
    }
    
    # Completeness
    null_counts = {}
    for col_name in required_columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_counts[col_name] = null_count
    
    total_nulls = sum(null_counts.values())
    total_values = total_records * total_fields
    
    metrics['completeness_rate'] = ((total_values - total_nulls) / total_values * 100) if total_values > 0 else 0
    
    # Uniqueness (using first column as key)
    if required_columns:
        key_col = required_columns[0]
        distinct_count = df.select(key_col).distinct().count()
        metrics['duplication_rate'] = ((total_records - distinct_count) / total_records * 100) if total_records > 0 else 0
    
    # Overall quality score (simplified)
    metrics['quality_score'] = (metrics['completeness_rate'] + (100 - metrics['duplication_rate'])) / 2
    
    return metrics

# Test metrics calculation
sample_metrics = calculate_quality_metrics(sample_df, ["id", "amount", "category"])

print("\nQUALITY METRICS SUMMARY")
print("-" * 40)
for key, value in sample_metrics.items():
    if isinstance(value, float):
        print(f"{key}: {value:.2f}")
    else:
        print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business Impact Metrics
# MAGIC
# MAGIC These metrics tie data quality to business outcomes:
# MAGIC
# MAGIC 1. **Data-Driven Decision Quality** - Accuracy of insights derived from data
# MAGIC 2. **Downstream System Failures** - Systems impacted by poor data quality
# MAGIC 3. **Manual Remediation Time** - Hours spent fixing data issues
# MAGIC 4. **Customer Impact** - Issues reported due to bad data
# MAGIC 5. **Compliance Violations** - Failed regulatory checks due to quality issues
# MAGIC 6. **Revenue Impact** - Financial loss attributed to data quality problems

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### Six Dimensions Framework
# MAGIC - **Accuracy**: Is the data correct?
# MAGIC - **Completeness**: Is all required data present?
# MAGIC - **Consistency**: Does data agree internally and across systems?
# MAGIC - **Timeliness**: Is data available when needed?
# MAGIC - **Validity**: Does data conform to standards?
# MAGIC - **Uniqueness**: Are there unwanted duplicates?
# MAGIC
# MAGIC ### Quality Framework Components
# MAGIC 1. **Profiling** - Understand current state
# MAGIC 2. **Validation** - Enforce rules
# MAGIC 3. **Monitoring** - Track metrics
# MAGIC 4. **Remediation** - Fix issues
# MAGIC 5. **Prevention** - Stop problems at source
# MAGIC
# MAGIC ### Success Principles
# MAGIC - Start with profiling to understand your data
# MAGIC - Define clear quality rules based on business requirements
# MAGIC - Measure and monitor continuously
# MAGIC - Act on quality issues promptly
# MAGIC - Focus on prevention, not just detection

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **Module 2: Medallion Architecture for Data Quality** to learn:
# MAGIC - Quality patterns for Bronze, Silver, and Gold layers
# MAGIC - Where to implement different quality checks
# MAGIC - How to build quality into your data pipeline architecture
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 30 minutes | **Level**: 200 | **Type**: Lecture
