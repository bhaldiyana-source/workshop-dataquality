# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Data Quality Profiling
# MAGIC
# MAGIC **Module 4: Automated Data Profiling and Statistics**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 200/300       |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC In this hands-on lab, you will:
# MAGIC
# MAGIC 1. **Build a DataProfiler class** - Automated statistical analysis
# MAGIC 2. **Profile columns** - Type-specific profiling for numeric, string, and date columns
# MAGIC 3. **Detect patterns** - Find common data patterns (emails, phones, URLs)
# MAGIC 4. **Identify outliers** - Statistical outlier detection using IQR method
# MAGIC 5. **Generate reports** - Create comprehensive profiling reports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import json
from datetime import datetime, date

# Set catalog and schema (modify as needed)
catalog = "main"
schema = "default"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"✅ Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Sample Data
# MAGIC
# MAGIC Let's create a realistic dataset with various data quality issues

# COMMAND ----------

# Create sample customer orders dataset with quality issues
sample_data = [
    (1, "john.doe@example.com", "555-123-4567", 25, 150.50, "2024-01-15", "completed", "US"),
    (2, "jane.smith@test.com", "555-234-5678", 32, 275.00, "2024-01-16", "completed", "US"),
    (3, "invalid-email", "555-345-6789", 45, 89.99, "2024-01-17", "pending", "CA"),
    (4, "bob@company.com", None, 28, 450.00, "2024-01-18", "completed", "US"),
    (5, "alice.wong@mail.com", "555-456-7890", None, 125.75, "2024-01-19", "completed", "UK"),
    (6, "charlie@test.com", "555-567-8901", 150, 200.00, "2024-01-20", "cancelled", "US"),  # Age outlier
    (7, None, "555-678-9012", 35, -50.00, "2024-01-21", "pending", "CA"),  # Negative amount
    (8, "diana@example.com", "invalid", 29, 175.50, "2024-01-22", "completed", "US"),
    (9, "eve@test.com", "555-789-0123", 41, 999999.99, "2024-01-23", "completed", "UK"),  # Amount outlier
    (10, "frank@mail.com", "555-890-1234", 33, 225.00, "2024-01-24", "invalid_status", "US"),
    # Add more records for statistical significance
    *[(i, f"user{i}@example.com", f"555-{i:03d}-{i+1:04d}", 25 + (i % 40), 100 + (i * 10) % 400, 
       f"2024-01-{(i % 28) + 1:02d}", ["pending", "completed", "cancelled"][i % 3], 
       ["US", "UK", "CA", "DE"][i % 4]) for i in range(11, 101)]
]

schema_def = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("country", StringType(), True)
])

df_sample = spark.createDataFrame(sample_data, schema_def)

# Write to Delta table
df_sample.write.format("delta").mode("overwrite").saveAsTable("customer_orders_sample")

print(f"✅ Created sample dataset with {df_sample.count()} records")
display(df_sample.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Basic Column Profiling
# MAGIC
# MAGIC Build functions to profile individual columns

# COMMAND ----------

def profile_column_basic(df, column_name):
    """
    Generate basic profile statistics for any column
    
    Args:
        df: DataFrame
        column_name: Name of column to profile
    
    Returns:
        Dictionary with profile statistics
    """
    
    total_count = df.count()
    
    # Basic statistics
    stats = df.select(
        count(column_name).alias("non_null_count"),
        count(when(col(column_name).isNull(), 1)).alias("null_count"),
        countDistinct(column_name).alias("distinct_count")
    ).collect()[0]
    
    non_null_count = stats["non_null_count"]
    null_count = stats["null_count"]
    distinct_count = stats["distinct_count"]
    
    profile = {
        "column_name": column_name,
        "data_type": str(df.schema[column_name].dataType),
        "total_count": total_count,
        "non_null_count": non_null_count,
        "null_count": null_count,
        "null_percentage": round((null_count / total_count * 100), 2) if total_count > 0 else 0,
        "distinct_count": distinct_count,
        "cardinality": round((distinct_count / non_null_count * 100), 2) if non_null_count > 0 else 0
    }
    
    return profile

# Test basic profiling
print("="*60)
print("BASIC COLUMN PROFILE: email")
print("="*60)
email_profile = profile_column_basic(df_sample, "email")
for key, value in email_profile.items():
    print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Numeric Column Profiling
# MAGIC
# MAGIC Add statistical analysis for numeric columns

# COMMAND ----------

def profile_numeric_column(df, column_name):
    """
    Profile numeric column with statistical measures
    """
    
    # Get basic profile
    profile = profile_column_basic(df, column_name)
    
    # Add numeric statistics
    stats = df.select(
        min(column_name).alias("min_value"),
        max(column_name).alias("max_value"),
        avg(column_name).alias("mean"),
        stddev(column_name).alias("stddev"),
        sum(column_name).alias("sum")
    ).collect()[0]
    
    profile.update({
        "min_value": stats["min_value"],
        "max_value": stats["max_value"],
        "mean": round(stats["mean"], 2) if stats["mean"] else None,
        "stddev": round(stats["stddev"], 2) if stats["stddev"] else None,
        "sum": stats["sum"]
    })
    
    # Calculate quartiles
    quantiles = df.stat.approxQuantile(column_name, [0.25, 0.5, 0.75], 0.05)
    if len(quantiles) == 3:
        profile.update({
            "q1": quantiles[0],
            "median": quantiles[1],
            "q3": quantiles[2],
            "iqr": quantiles[2] - quantiles[0]
        })
    
    # Detect outliers using IQR method
    if len(quantiles) == 3:
        q1, q3 = quantiles[0], quantiles[2]
        iqr = q3 - q1
        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)
        
        outlier_count = df.filter(
            (col(column_name) < lower_bound) | (col(column_name) > upper_bound)
        ).count()
        
        profile.update({
            "outlier_lower_bound": round(lower_bound, 2),
            "outlier_upper_bound": round(upper_bound, 2),
            "outlier_count": outlier_count,
            "outlier_percentage": round((outlier_count / df.count() * 100), 2)
        })
    
    return profile

# Test numeric profiling
print("\n" + "="*60)
print("NUMERIC COLUMN PROFILE: age")
print("="*60)
age_profile = profile_numeric_column(df_sample, "age")
for key, value in age_profile.items():
    print(f"{key}: {value}")

print("\n" + "="*60)
print("NUMERIC COLUMN PROFILE: order_amount")
print("="*60)
amount_profile = profile_numeric_column(df_sample, "order_amount")
for key, value in amount_profile.items():
    print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: String Column Profiling
# MAGIC
# MAGIC Profile string columns with length and pattern analysis

# COMMAND ----------

def profile_string_column(df, column_name, show_top_values=5):
    """
    Profile string column with length and pattern statistics
    """
    
    # Get basic profile
    profile = profile_column_basic(df, column_name)
    
    # Add string-specific statistics
    df_with_length = df.filter(col(column_name).isNotNull()).withColumn(
        "str_length", length(col(column_name))
    )
    
    length_stats = df_with_length.select(
        min("str_length").alias("min_length"),
        max("str_length").alias("max_length"),
        avg("str_length").alias("avg_length")
    ).collect()[0]
    
    profile.update({
        "min_length": length_stats["min_length"],
        "max_length": length_stats["max_length"],
        "avg_length": round(length_stats["avg_length"], 2) if length_stats["avg_length"] else None
    })
    
    # Get top values
    top_values = df.groupBy(column_name) \
        .count() \
        .orderBy(desc("count")) \
        .limit(show_top_values) \
        .collect()
    
    profile["top_values"] = [
        {"value": row[column_name], "count": row["count"]} 
        for row in top_values
    ]
    
    # Check for empty strings
    empty_count = df.filter(col(column_name) == "").count()
    profile["empty_string_count"] = empty_count
    
    return profile

# Test string profiling
print("\n" + "="*60)
print("STRING COLUMN PROFILE: email")
print("="*60)
email_profile = profile_string_column(df_sample, "email")
for key, value in email_profile.items():
    if key != "top_values":
        print(f"{key}: {value}")
    else:
        print("top_values:")
        for tv in value:
            print(f"  {tv['value']}: {tv['count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: Pattern Detection
# MAGIC
# MAGIC Detect common data patterns in columns

# COMMAND ----------

def detect_patterns(df, column_name):
    """
    Detect common patterns in a string column
    """
    
    total_non_null = df.filter(col(column_name).isNotNull()).count()
    
    if total_non_null == 0:
        return {}
    
    patterns = {
        "email": r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$',
        "phone_us": r'^\d{3}-\d{3}-\d{4}$',
        "phone_international": r'^\+?\d{1,3}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}$',
        "url": r'^https?://',
        "zipcode_us": r'^\d{5}(-\d{4})?$',
        "ssn": r'^\d{3}-\d{2}-\d{4}$',
        "date_iso": r'^\d{4}-\d{2}-\d{2}$',
        "uuid": r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    }
    
    detected_patterns = {}
    
    for pattern_name, pattern_regex in patterns.items():
        match_count = df.filter(
            col(column_name).isNotNull() & col(column_name).rlike(pattern_regex)
        ).count()
        
        match_percentage = (match_count / total_non_null * 100)
        
        # Consider it a match if > 50% of non-null values match the pattern
        if match_percentage > 50:
            detected_patterns[pattern_name] = {
                "match_count": match_count,
                "match_percentage": round(match_percentage, 2)
            }
    
    return detected_patterns

# Test pattern detection
print("\n" + "="*60)
print("PATTERN DETECTION: email")
print("="*60)
email_patterns = detect_patterns(df_sample, "email")
for pattern, stats in email_patterns.items():
    print(f"{pattern}: {stats['match_percentage']}% ({stats['match_count']} records)")

print("\n" + "="*60)
print("PATTERN DETECTION: phone")
print("="*60)
phone_patterns = detect_patterns(df_sample, "phone")
for pattern, stats in phone_patterns.items():
    print(f"{pattern}: {stats['match_percentage']}% ({stats['match_count']} records)")

print("\n" + "="*60)
print("PATTERN DETECTION: order_date")
print("="*60)
date_patterns = detect_patterns(df_sample, "order_date")
for pattern, stats in date_patterns.items():
    print(f"{pattern}: {stats['match_percentage']}% ({stats['match_count']} records)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Comprehensive DataProfiler Class
# MAGIC
# MAGIC Combine all profiling functions into a reusable class

# COMMAND ----------

class DataProfiler:
    """
    Comprehensive data profiling for quality assessment
    """
    
    def __init__(self, df):
        self.df = df
        self.profile_results = {}
    
    def profile_table(self):
        """
        Generate complete table profile
        """
        
        print("Starting table profiling...")
        
        table_profile = {
            "row_count": self.df.count(),
            "column_count": len(self.df.columns),
            "columns": {}
        }
        
        # Profile each column
        for column_name in self.df.columns:
            print(f"  Profiling column: {column_name}")
            table_profile["columns"][column_name] = self.profile_column(column_name)
        
        # Add table-level insights
        table_profile["quality_summary"] = self._generate_quality_summary(table_profile)
        
        self.profile_results = table_profile
        return table_profile
    
    def profile_column(self, column_name):
        """
        Profile individual column based on data type
        """
        
        col_type = self.df.schema[column_name].dataType
        
        # Start with basic profile
        profile = profile_column_basic(self.df, column_name)
        
        # Add type-specific profiling
        if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
            numeric_profile = profile_numeric_column(self.df, column_name)
            profile.update(numeric_profile)
        
        elif isinstance(col_type, StringType):
            string_profile = profile_string_column(self.df, column_name)
            profile.update(string_profile)
            
            # Add pattern detection
            patterns = detect_patterns(self.df, column_name)
            if patterns:
                profile["detected_patterns"] = patterns
        
        return profile
    
    def _generate_quality_summary(self, table_profile):
        """
        Generate quality summary from profile
        """
        
        summary = {
            "columns_with_nulls": [],
            "columns_with_high_nulls": [],  # > 20%
            "columns_with_outliers": [],
            "columns_with_low_cardinality": [],  # < 5%
            "columns_with_pattern_issues": []
        }
        
        for col_name, col_profile in table_profile["columns"].items():
            # Check nulls
            if col_profile.get("null_count", 0) > 0:
                summary["columns_with_nulls"].append(col_name)
                
                if col_profile.get("null_percentage", 0) > 20:
                    summary["columns_with_high_nulls"].append({
                        "column": col_name,
                        "null_percentage": col_profile["null_percentage"]
                    })
            
            # Check outliers
            if col_profile.get("outlier_count", 0) > 0:
                summary["columns_with_outliers"].append({
                    "column": col_name,
                    "outlier_count": col_profile["outlier_count"],
                    "outlier_percentage": col_profile.get("outlier_percentage", 0)
                })
            
            # Check cardinality
            if col_profile.get("cardinality", 100) < 5:
                summary["columns_with_low_cardinality"].append({
                    "column": col_name,
                    "cardinality": col_profile.get("cardinality", 0),
                    "distinct_count": col_profile.get("distinct_count", 0)
                })
            
            # Check pattern detection
            detected_patterns = col_profile.get("detected_patterns", {})
            for pattern_name, pattern_stats in detected_patterns.items():
                if pattern_stats["match_percentage"] < 95:  # Less than 95% match
                    summary["columns_with_pattern_issues"].append({
                        "column": col_name,
                        "pattern": pattern_name,
                        "match_percentage": pattern_stats["match_percentage"]
                    })
        
        return summary
    
    def generate_report(self):
        """
        Generate human-readable profiling report
        """
        
        if not self.profile_results:
            print("No profile results. Run profile_table() first.")
            return
        
        profile = self.profile_results
        
        print("\n" + "="*80)
        print("DATA QUALITY PROFILE REPORT")
        print("="*80)
        print(f"Total Rows: {profile['row_count']:,}")
        print(f"Total Columns: {profile['column_count']}")
        print()
        
        # Quality summary
        summary = profile.get("quality_summary", {})
        
        print("QUALITY SUMMARY")
        print("-"*80)
        print(f"Columns with NULL values: {len(summary.get('columns_with_nulls', []))}")
        print(f"Columns with high NULL % (>20%): {len(summary.get('columns_with_high_nulls', []))}")
        print(f"Columns with outliers: {len(summary.get('columns_with_outliers', []))}")
        print(f"Columns with low cardinality (<5%): {len(summary.get('columns_with_low_cardinality', []))}")
        print()
        
        # High null columns
        if summary.get("columns_with_high_nulls"):
            print("⚠️  COLUMNS WITH HIGH NULL PERCENTAGE:")
            for item in summary["columns_with_high_nulls"]:
                print(f"  - {item['column']}: {item['null_percentage']}%")
            print()
        
        # Outlier columns
        if summary.get("columns_with_outliers"):
            print("⚠️  COLUMNS WITH OUTLIERS:")
            for item in summary["columns_with_outliers"]:
                print(f"  - {item['column']}: {item['outlier_count']} outliers ({item['outlier_percentage']}%)")
            print()
        
        # Pattern issues
        if summary.get("columns_with_pattern_issues"):
            print("⚠️  COLUMNS WITH PATTERN ISSUES:")
            for item in summary["columns_with_pattern_issues"]:
                print(f"  - {item['column']} ({item['pattern']}): {item['match_percentage']}% match")
            print()
        
        print("="*80)
        
        return profile
    
    def save_profile(self, output_table):
        """
        Save profile results to Delta table
        """
        
        if not self.profile_results:
            print("No profile results. Run profile_table() first.")
            return
        
        # Convert to DataFrame
        profile_records = []
        
        for col_name, col_profile in self.profile_results["columns"].items():
            record = {
                "profile_timestamp": datetime.now(),
                "column_name": col_name,
                "profile_data": json.dumps(col_profile)
            }
            profile_records.append(record)
        
        df_profile = spark.createDataFrame(profile_records)
        df_profile.write.format("delta").mode("append").saveAsTable(output_table)
        
        print(f"✅ Profile saved to {output_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: Run Complete Profiling
# MAGIC
# MAGIC Use the DataProfiler class to profile our sample data

# COMMAND ----------

# Create profiler instance
profiler = DataProfiler(df_sample)

# Run profiling
profile = profiler.profile_table()

# Generate report
profiler.generate_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: View Detailed Column Profiles

# COMMAND ----------

# View detailed profile for specific columns
print("\n" + "="*80)
print("DETAILED PROFILE: age")
print("="*80)
age_detail = profile["columns"]["age"]
for key, value in age_detail.items():
    if key not in ["top_values", "detected_patterns"]:
        print(f"{key}: {value}")

print("\n" + "="*80)
print("DETAILED PROFILE: email")
print("="*80)
email_detail = profile["columns"]["email"]
for key, value in email_detail.items():
    if key == "detected_patterns":
        print(f"{key}:")
        for pattern, stats in value.items():
            print(f"  {pattern}: {stats}")
    elif key != "top_values":
        print(f"{key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: Save Profile Results

# COMMAND ----------

# Create profile storage table
spark.sql("""
    CREATE TABLE IF NOT EXISTS data_profiles (
        profile_timestamp TIMESTAMP,
        column_name STRING,
        profile_data STRING
    ) USING DELTA
""")

# Save profile
profiler.save_profile("data_profiles")

# View saved profiles
display(spark.table("data_profiles"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: Compare Profiles Over Time
# MAGIC
# MAGIC Profile the same table after making changes

# COMMAND ----------

# Simulate data changes
df_modified = df_sample.filter(col("age").isNotNull())  # Remove NULL ages
df_modified = df_modified.filter(col("order_amount") > 0)  # Remove negative amounts

df_modified.write.format("delta").mode("overwrite").saveAsTable("customer_orders_modified")

# Profile modified data
df_modified_table = spark.table("customer_orders_modified")
profiler_modified = DataProfiler(df_modified_table)
profile_modified = profiler_modified.profile_table()

print("\nCOMPARISON: Original vs Modified")
print("="*80)
print(f"Original row count: {profile['row_count']}")
print(f"Modified row count: {profile_modified['row_count']}")
print()
print("Null percentages - age:")
print(f"  Original: {profile['columns']['age']['null_percentage']}%")
print(f"  Modified: {profile_modified['columns']['age']['null_percentage']}%")
print()
print("Outliers - order_amount:")
print(f"  Original: {profile['columns']['order_amount']['outlier_count']}")
print(f"  Modified: {profile_modified['columns']['order_amount']['outlier_count']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### What You Learned
# MAGIC 1. **Basic Profiling** - Count, nulls, cardinality for all columns
# MAGIC 2. **Type-Specific Profiling** - Statistical analysis for numeric, length analysis for strings
# MAGIC 3. **Pattern Detection** - Automatically identify data formats
# MAGIC 4. **Outlier Detection** - IQR method for finding anomalies
# MAGIC 5. **Comprehensive Reports** - Quality summaries and detailed profiles
# MAGIC
# MAGIC ### Best Practices
# MAGIC - Profile data before building quality rules
# MAGIC - Save profiles over time to track changes
# MAGIC - Focus on high-null and outlier columns first
# MAGIC - Use pattern detection to validate formats
# MAGIC - Combine profiling with business knowledge
# MAGIC
# MAGIC ### Next Steps
# MAGIC Use these profiles to:
# MAGIC - Define validation rules (Lab 2)
# MAGIC - Set quality thresholds
# MAGIC - Identify data cleansing needs
# MAGIC - Establish quality baselines
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 45 minutes | **Level**: 200/300 | **Type**: Lab
