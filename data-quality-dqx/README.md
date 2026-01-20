# Data Quality with DQX Framework Workshop

Welcome to the **Data Quality with DQX (Data Quality Framework)** workshop! This comprehensive guide will teach you how to use the Databricks Labs DQX framework to define, monitor, and address data quality issues in your Python-based data pipelines.

## 📚 Workshop Overview

DQX is a powerful data quality framework for Apache Spark that enables you to:
- Define quality rules at row and column levels
- Monitor data quality in real-time
- Automatically profile data and generate quality rules
- Handle failed checks with custom reactions
- Track quality metrics and build dashboards
- Support both batch and streaming workloads

**Official Documentation**: [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)

---

## 🎯 Learning Objectives

By the end of this workshop, you will be able to:
1. Install and configure DQX in Databricks
2. Define data quality checks using code and configuration
3. Implement row-level and column-level quality rules
4. Profile data and auto-generate quality rules
5. Handle failed checks with custom reactions (drop, mark, quarantine)
6. Integrate DQX with Spark Structured Streaming
7. Use DQX with Lakeflow Pipelines (DLT)
8. Build quality dashboards and monitoring solutions
9. Implement production-ready data quality pipelines

---

## 📋 Prerequisites

- Basic knowledge of Apache Spark and PySpark
- Familiarity with Databricks workspace
- Understanding of data quality concepts
- Python programming experience

---

## 🛠️ Installation

### Method 1: Install from PyPI (Recommended)

```python
%pip install databricks-labs-dqx
```

### Method 2: Install from Workspace Wheel

```python
%pip install /Workspace/Users/<your-username>/.dqx/wheels/databricks_labs_dqx-*.whl
```

### Restart Python Kernel

After installation, restart the Python kernel:

```python
dbutils.library.restartPython()
```

---

## 📖 Workshop Modules

### Module 1: Foundations of Data Quality with DQX

**Duration**: 30 minutes

#### Topics Covered:
- Introduction to DQX framework
- Core capabilities and features
- DQX architecture and components
- Comparison with other data quality tools
- Use cases and best practices

#### Key Concepts:
- **Data Quality Checks**: Rules that validate data against expected criteria
- **Check Levels**: Warning vs Error severity levels
- **Reactions**: Actions taken when checks fail (drop, mark, quarantine)
- **Profiling**: Automatic analysis of data characteristics
- **Validation Summary**: Aggregated quality metrics

---

### Module 2: Getting Started - Your First Data Quality Check

**Duration**: 45 minutes

#### Learning Goals:
- Set up your first DQX project
- Define simple quality checks
- Run validation on sample data
- Understand validation results

#### Hands-On Exercise:

```python
from dqx import Validator, CheckType

# Create a sample DataFrame
data = [
    (1, "John", 30, "john@example.com"),
    (2, "Jane", -5, "invalid-email"),
    (3, "Bob", 45, "bob@example.com"),
    (4, None, 35, "alice@example.com")
]
df = spark.createDataFrame(data, ["id", "name", "age", "email"])

# Define quality checks
validator = Validator()

# Add column-level checks
validator.add_check(
    check_type=CheckType.NOT_NULL,
    column="name",
    level="error"
)

validator.add_check(
    check_type=CheckType.GREATER_THAN,
    column="age",
    threshold=0,
    level="error"
)

validator.add_check(
    check_type=CheckType.REGEX_MATCH,
    column="email",
    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    level="warning"
)

# Run validation
result_df, summary = validator.validate(df)

# Display results
display(result_df)
display(summary)
```

#### Expected Outcomes:
- Understand how to create validators
- Learn different check types
- Interpret validation results
- Differentiate between error and warning levels

---

### Module 3: Row-Level and Column-Level Quality Rules

**Duration**: 60 minutes

#### Topics Covered:

##### A. Column-Level Rules
- Not null checks
- Data type validations
- Range checks (min/max)
- Pattern matching (regex)
- Uniqueness constraints
- Set membership checks

##### B. Row-Level Rules
- Cross-column validations
- Business logic rules
- Conditional checks
- Complex expressions

#### Hands-On Lab:

```python
from dqx import Validator, CheckType, Rule

# Create validator
validator = Validator()

# Column-level checks
validator.add_check(
    name="customer_id_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error"
)

validator.add_check(
    name="amount_range_check",
    check_type=CheckType.BETWEEN,
    column="amount",
    min_value=0,
    max_value=1000000,
    level="error"
)

validator.add_check(
    name="status_valid_values",
    check_type=CheckType.IN_SET,
    column="status",
    valid_values=["pending", "completed", "cancelled"],
    level="warning"
)

# Row-level check using custom rule
validator.add_rule(
    Rule(
        name="discount_validation",
        expression="discount <= amount * 0.5",
        level="error",
        description="Discount cannot exceed 50% of amount"
    )
)

# Run validation
result_df, summary = validator.validate(transactions_df)
```

#### Lab Exercises:
1. Implement null checks on critical columns
2. Add range validations for numeric fields
3. Create pattern matching for email/phone fields
4. Build cross-column validation rules
5. Test with various data scenarios

---

### Module 4: Data Profiling and Auto-Generated Rules

**Duration**: 45 minutes

#### Topics Covered:
- Automatic data profiling
- Statistics generation
- Quality rule recommendations
- Threshold tuning
- Profile-based validation

#### Hands-On Demo:

```python
from dqx import Profiler, RuleGenerator

# Step 1: Profile your data
profiler = Profiler()
profile_results = profiler.profile(df)

# View profiling results
display(profile_results)

# Step 2: Generate quality rules from profile
rule_generator = RuleGenerator()
suggested_rules = rule_generator.generate_rules(
    profile_results,
    confidence_threshold=0.95
)

# Review suggested rules
display(suggested_rules)

# Step 3: Apply generated rules
validator = Validator()
validator.add_rules_from_profile(suggested_rules)

# Step 4: Validate data
result_df, summary = validator.validate(df)
```

#### Key Features:
- **Automatic Profiling**: Analyzes data distributions, nulls, uniqueness
- **Rule Suggestions**: Generates rules based on data patterns
- **Confidence Levels**: Adjustable thresholds for rule generation
- **Iterative Refinement**: Update rules as data evolves

#### Lab Exercise:
1. Profile a customer dataset
2. Review profiling statistics
3. Generate quality rules automatically
4. Fine-tune suggested rules
5. Apply and validate

---

### Module 5: Custom Reactions to Failed Checks

**Duration**: 60 minutes

#### Topics Covered:

##### A. Drop Invalid Records
Remove rows that fail validation

##### B. Mark Invalid Records
Flag bad records with quality indicators

##### C. Quarantine Invalid Records
Move failed records to separate storage

##### D. Alert and Continue
Log issues but continue processing

#### Implementation Patterns:

```python
from dqx import Validator, Reaction, QuarantineConfig

# Create validator with reactions
validator = Validator()

# Add checks with DROP reaction
validator.add_check(
    name="critical_field_not_null",
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error",
    reaction=Reaction.DROP
)

# Add checks with MARK reaction
validator.add_check(
    name="email_format_check",
    check_type=CheckType.REGEX_MATCH,
    column="email",
    pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
    level="warning",
    reaction=Reaction.MARK,
    mark_column="email_quality_flag"
)

# Add checks with QUARANTINE reaction
quarantine_config = QuarantineConfig(
    target_table="data_quality.quarantine_records",
    partition_by=["date", "source_system"]
)

validator.add_check(
    name="amount_validation",
    check_type=CheckType.GREATER_THAN,
    column="amount",
    threshold=0,
    level="error",
    reaction=Reaction.QUARANTINE,
    quarantine_config=quarantine_config
)

# Run validation with reactions
clean_df, summary = validator.validate(df)

# Clean data only contains valid records
display(clean_df)

# Summary shows quarantined record counts
display(summary)
```

#### Advanced Quarantine Strategy:

```python
# Quarantine with metadata enrichment
validator.configure_quarantine(
    table="quality.quarantine",
    add_metadata=True,
    metadata_columns={
        "quarantine_timestamp": "current_timestamp()",
        "pipeline_id": "{{pipeline_id}}",
        "failed_checks": "array of failed check names"
    }
)

# Query quarantined records
quarantined_df = spark.table("quality.quarantine")
display(quarantined_df)

# Remediate and reprocess
remediated_df = apply_fixes(quarantined_df)
revalidated_df, _ = validator.validate(remediated_df)
```

#### Lab Exercise:
1. Configure different reactions for various check types
2. Set up quarantine tables
3. Implement marking strategy for warnings
4. Build remediation workflows
5. Test with real-world scenarios

---

### Module 6: Working with Spark Structured Streaming

**Duration**: 60 minutes

#### Topics Covered:
- DQX with streaming DataFrames
- Real-time quality monitoring
- Streaming quarantine patterns
- Checkpoint management
- Handling late-arriving data

#### Streaming Implementation:

```python
from dqx import StreamingValidator

# Define streaming source
streaming_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("s3://bucket/streaming-data/")
)

# Create streaming validator
validator = StreamingValidator()

# Add quality checks
validator.add_check(
    check_type=CheckType.NOT_NULL,
    column="transaction_id",
    level="error",
    reaction=Reaction.QUARANTINE
)

validator.add_check(
    check_type=CheckType.BETWEEN,
    column="amount",
    min_value=0,
    max_value=1000000,
    level="error"
)

# Apply validation in streaming context
validated_stream = validator.validate_stream(streaming_df)

# Write clean data to target
(
    validated_stream
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/validated_data")
    .trigger(processingTime="1 minute")
    .table("clean_transactions")
)

# Write quality metrics to monitoring table
(
    validator.get_metrics_stream()
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/quality_metrics")
    .trigger(processingTime="1 minute")
    .table("quality_metrics")
)
```

#### Streaming Quality Dashboard:

```python
# Query real-time quality metrics
quality_metrics_df = spark.table("quality_metrics")

# Calculate quality KPIs
display(
    quality_metrics_df
    .groupBy(window("timestamp", "5 minutes"))
    .agg(
        sum("records_processed").alias("total_records"),
        sum("records_failed").alias("failed_records"),
        (1 - sum("records_failed") / sum("records_processed")).alias("pass_rate")
    )
)
```

#### Lab Exercise:
1. Set up streaming data source
2. Configure DQX for streaming
3. Implement streaming quarantine
4. Build real-time quality dashboard
5. Test with simulated streaming data

---

### Module 7: Lakeflow Pipelines (DLT) Integration

**Duration**: 60 minutes

#### Topics Covered:
- DQX with Delta Live Tables
- DLT expectations vs DQX checks
- Combining DLT and DQX
- Medallion architecture with DQX
- Pipeline observability

#### DLT Pipeline with DQX:

```python
import dlt
from dqx import DLTValidator

# Bronze Layer - Raw data ingestion
@dlt.table(
    name="bronze_transactions",
    comment="Raw transaction data"
)
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/source/transactions")
    )

# Silver Layer - Validated and cleaned data
@dlt.table(
    name="silver_transactions",
    comment="Validated transaction data"
)
def silver_transactions():
    # Read from bronze
    bronze_df = dlt.read_stream("bronze_transactions")
    
    # Create DQX validator for DLT
    validator = DLTValidator()
    
    # Add quality checks
    validator.add_check(
        check_type=CheckType.NOT_NULL,
        column="transaction_id",
        level="error"
    )
    
    validator.add_check(
        check_type=CheckType.UNIQUE,
        column="transaction_id",
        level="error"
    )
    
    validator.add_check(
        check_type=CheckType.BETWEEN,
        column="amount",
        min_value=0,
        max_value=1000000,
        level="error"
    )
    
    # Validate and return clean data
    return validator.validate_dlt(bronze_df)

# Gold Layer - Aggregated business metrics
@dlt.table(
    name="gold_daily_summary",
    comment="Daily transaction summary"
)
def gold_daily_summary():
    return (
        dlt.read("silver_transactions")
        .groupBy("date", "customer_id")
        .agg(
            sum("amount").alias("total_amount"),
            count("*").alias("transaction_count")
        )
    )

# Quality metrics table
@dlt.table(
    name="quality_metrics",
    comment="Data quality tracking metrics"
)
def quality_metrics():
    return DLTValidator.get_metrics_table()
```

#### Combining DLT Expectations and DQX:

```python
@dlt.table
@dlt.expect_all({
    "valid_timestamp": "timestamp IS NOT NULL",
    "positive_amount": "amount > 0"
})
def enriched_data():
    df = dlt.read_stream("source_data")
    
    # Apply additional DQX validations
    validator = DLTValidator()
    validator.add_check(
        check_type=CheckType.REGEX_MATCH,
        column="email",
        pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$",
        level="warning",
        reaction=Reaction.MARK
    )
    
    return validator.validate_dlt(df)
```

#### Lab Exercise:
1. Create DLT pipeline with DQX integration
2. Implement medallion architecture
3. Configure quality checks per layer
4. Set up quality metrics tracking
5. Monitor pipeline execution

---

### Module 8: Configuration-Based Quality Checks

**Duration**: 45 minutes

#### Topics Covered:
- YAML-based check definitions
- Configuration management
- Dynamic rule loading
- Environment-specific rules
- Version control for quality rules

#### YAML Configuration:

```yaml
# quality_rules.yaml
version: "1.0"
tables:
  - name: "transactions"
    checks:
      - name: "transaction_id_not_null"
        type: "not_null"
        column: "transaction_id"
        level: "error"
        reaction: "drop"
      
      - name: "amount_range"
        type: "between"
        column: "amount"
        min_value: 0
        max_value: 1000000
        level: "error"
        reaction: "quarantine"
      
      - name: "valid_status"
        type: "in_set"
        column: "status"
        valid_values:
          - "pending"
          - "completed"
          - "cancelled"
        level: "warning"
        reaction: "mark"
      
      - name: "discount_validation"
        type: "expression"
        expression: "discount <= amount * 0.5"
        level: "error"
        description: "Discount cannot exceed 50% of amount"
    
    quarantine:
      table: "quality.quarantined_transactions"
      partition_by:
        - "date"
      add_metadata: true

  - name: "customers"
    checks:
      - name: "customer_id_unique"
        type: "unique"
        column: "customer_id"
        level: "error"
      
      - name: "email_format"
        type: "regex"
        column: "email"
        pattern: "^[\\w\\.-]+@[\\w\\.-]+\\.\\w+$"
        level: "warning"
```

#### Loading Configuration:

```python
from dqx import ConfigValidator
import yaml

# Load configuration from YAML
with open("quality_rules.yaml", "r") as f:
    config = yaml.safe_load(f)

# Create validator from configuration
validator = ConfigValidator.from_config(config)

# Apply to specific table
result_df, summary = validator.validate(
    df=transactions_df,
    table_name="transactions"
)

# Display results
display(result_df)
display(summary)
```

#### Lab Exercise:
1. Create YAML configuration for quality rules
2. Load and apply configuration
3. Test with multiple tables
4. Implement environment-specific configs
5. Version control quality rules

---

### Module 9: Validation Summary and Quality Dashboard

**Duration**: 60 minutes

#### Topics Covered:
- Quality metrics collection
- Summary statistics
- Dashboard design patterns
- Alerting integration
- Trend analysis

#### Collecting Quality Metrics:

```python
from dqx import Validator, MetricsCollector

# Create validator with metrics collection
validator = Validator()
metrics_collector = MetricsCollector(
    target_table="data_quality.validation_metrics"
)

validator.set_metrics_collector(metrics_collector)

# Add checks
validator.add_check(
    check_type=CheckType.NOT_NULL,
    column="customer_id",
    level="error"
)

# Run validation (metrics automatically collected)
result_df, summary = validator.validate(df)

# Summary contains detailed metrics
print(f"Total Records: {summary['total_records']}")
print(f"Valid Records: {summary['valid_records']}")
print(f"Failed Records: {summary['failed_records']}")
print(f"Pass Rate: {summary['pass_rate']:.2%}")
print(f"Failed Checks: {summary['failed_checks']}")
```

#### Building Quality Dashboard:

```python
# Query validation metrics
metrics_df = spark.table("data_quality.validation_metrics")

# Calculate KPIs
quality_kpis = (
    metrics_df
    .filter("timestamp >= current_date() - interval 30 days")
    .groupBy("table_name", "check_name")
    .agg(
        sum("records_processed").alias("total_processed"),
        sum("records_failed").alias("total_failed"),
        avg("pass_rate").alias("avg_pass_rate"),
        max("timestamp").alias("last_run")
    )
)

display(quality_kpis)

# Time-series analysis
quality_trends = (
    metrics_df
    .groupBy(
        "table_name",
        window("timestamp", "1 day").alias("date")
    )
    .agg(
        sum("records_processed").alias("daily_records"),
        avg("pass_rate").alias("daily_pass_rate"),
        count("*").alias("validation_runs")
    )
)

display(quality_trends)

# Failed check details
failed_checks_summary = (
    metrics_df
    .filter("records_failed > 0")
    .groupBy("table_name", "check_name", "check_type")
    .agg(
        sum("records_failed").alias("total_failures"),
        max("timestamp").alias("last_failure")
    )
    .orderBy(desc("total_failures"))
)

display(failed_checks_summary)
```

#### Creating SQL Dashboard:

```sql
-- Create dashboard views

-- Overall Quality Score
CREATE OR REPLACE VIEW data_quality.overall_quality_score AS
SELECT 
  date_trunc('day', timestamp) as date,
  AVG(pass_rate) as overall_pass_rate,
  SUM(records_processed) as total_records,
  SUM(records_failed) as total_failures
FROM data_quality.validation_metrics
WHERE timestamp >= current_date() - interval 30 days
GROUP BY date_trunc('day', timestamp);

-- Table Quality Health
CREATE OR REPLACE VIEW data_quality.table_health AS
SELECT 
  table_name,
  AVG(pass_rate) as avg_pass_rate,
  MAX(timestamp) as last_validated,
  COUNT(DISTINCT check_name) as total_checks,
  SUM(CASE WHEN records_failed > 0 THEN 1 ELSE 0 END) as failing_runs
FROM data_quality.validation_metrics
WHERE timestamp >= current_date() - interval 7 days
GROUP BY table_name;

-- Top Failing Checks
CREATE OR REPLACE VIEW data_quality.top_failing_checks AS
SELECT 
  table_name,
  check_name,
  check_type,
  SUM(records_failed) as total_failures,
  MAX(timestamp) as last_failure,
  AVG(pass_rate) as avg_pass_rate
FROM data_quality.validation_metrics
WHERE records_failed > 0
  AND timestamp >= current_date() - interval 7 days
GROUP BY table_name, check_name, check_type
ORDER BY total_failures DESC
LIMIT 20;
```

#### Lab Exercise:
1. Set up metrics collection
2. Create quality KPI views
3. Build SQL dashboard
4. Implement trend analysis
5. Configure quality alerts

---

### Module 10: Advanced Topics and Best Practices

**Duration**: 60 minutes

#### Topics Covered:

##### A. Performance Optimization
- Partitioning strategies
- Caching considerations
- Check ordering
- Parallel validation

##### B. Custom Check Development
```python
from dqx import CustomCheck

class BusinessRuleCheck(CustomCheck):
    def __init__(self, column_mapping, business_logic):
        self.column_mapping = column_mapping
        self.business_logic = business_logic
    
    def validate(self, df):
        # Implement custom validation logic
        return df.filter(self.business_logic)

# Use custom check
validator = Validator()
validator.add_custom_check(
    BusinessRuleCheck(
        column_mapping={"revenue": "total_amount", "cost": "expense"},
        business_logic="revenue > cost * 1.2"
    )
)
```

##### C. Multi-Table Validation
```python
from dqx import MultiTableValidator

# Validate referential integrity
multi_validator = MultiTableValidator()

multi_validator.add_foreign_key_check(
    source_table="orders",
    source_column="customer_id",
    target_table="customers",
    target_column="customer_id",
    level="error"
)

multi_validator.add_consistency_check(
    tables=["orders", "payments"],
    expression="orders.total_amount = payments.amount",
    join_condition="orders.order_id = payments.order_id",
    level="warning"
)

# Run multi-table validation
results = multi_validator.validate(
    {"orders": orders_df, "customers": customers_df}
)
```

##### D. Data Quality CI/CD
```python
# Quality check test suite
import unittest
from dqx import Validator, CheckType

class TestDataQualityRules(unittest.TestCase):
    def setUp(self):
        self.validator = Validator()
        self.sample_df = create_sample_data()
    
    def test_customer_id_not_null(self):
        self.validator.add_check(
            check_type=CheckType.NOT_NULL,
            column="customer_id",
            level="error"
        )
        result_df, summary = self.validator.validate(self.sample_df)
        self.assertEqual(summary['failed_records'], 0)
    
    def test_amount_range(self):
        self.validator.add_check(
            check_type=CheckType.BETWEEN,
            column="amount",
            min_value=0,
            max_value=1000000,
            level="error"
        )
        result_df, summary = self.validator.validate(self.sample_df)
        self.assertGreater(summary['pass_rate'], 0.95)

# Run tests
unittest.main()
```

##### E. Quality Rule Versioning
```python
from dqx import RuleVersionManager

# Version control for quality rules
version_manager = RuleVersionManager(
    catalog="quality_rules",
    schema="versions"
)

# Save rule set
version_manager.save_rules(
    validator=validator,
    version="v1.0.0",
    description="Initial production rules",
    author="data-team"
)

# Load specific version
validator_v1 = version_manager.load_rules(version="v1.0.0")

# Compare versions
diff = version_manager.compare_versions("v1.0.0", "v2.0.0")
display(diff)
```

---

### Module 11: Production Implementation Patterns

**Duration**: 90 minutes

#### Complete Production Pipeline:

```python
# production_quality_pipeline.py

from dqx import Validator, Profiler, MetricsCollector, QuarantineConfig
from pyspark.sql import DataFrame
import logging

class ProductionQualityPipeline:
    """Production-ready data quality pipeline with DQX"""
    
    def __init__(self, config_path: str, environment: str):
        self.config = self._load_config(config_path, environment)
        self.logger = self._setup_logging()
        self.metrics_collector = MetricsCollector(
            target_table=f"{self.config['catalog']}.{self.config['schema']}.quality_metrics"
        )
    
    def validate_batch(self, df: DataFrame, table_name: str) -> DataFrame:
        """Validate batch data with comprehensive quality checks"""
        self.logger.info(f"Starting validation for {table_name}")
        
        # Create validator
        validator = Validator()
        validator.set_metrics_collector(self.metrics_collector)
        
        # Load rules for table
        rules = self._load_rules(table_name)
        for rule in rules:
            validator.add_check(**rule)
        
        # Configure quarantine
        validator.configure_quarantine(
            QuarantineConfig(
                target_table=f"{self.config['catalog']}.{self.config['schema']}.quarantine_{table_name}",
                partition_by=["date", "validation_timestamp"],
                add_metadata=True
            )
        )
        
        # Run validation
        try:
            clean_df, summary = validator.validate(df)
            
            # Log results
            self.logger.info(f"Validation completed for {table_name}")
            self.logger.info(f"Pass rate: {summary['pass_rate']:.2%}")
            
            # Alert on low pass rate
            if summary['pass_rate'] < self.config['alert_threshold']:
                self._send_alert(table_name, summary)
            
            return clean_df
            
        except Exception as e:
            self.logger.error(f"Validation failed for {table_name}: {str(e)}")
            raise
    
    def validate_stream(self, stream_df: DataFrame, table_name: str):
        """Validate streaming data"""
        from dqx import StreamingValidator
        
        validator = StreamingValidator()
        validator.set_metrics_collector(self.metrics_collector)
        
        # Load rules
        rules = self._load_rules(table_name)
        for rule in rules:
            validator.add_check(**rule)
        
        # Apply streaming validation
        clean_stream = validator.validate_stream(stream_df)
        
        return clean_stream
    
    def profile_and_generate_rules(self, df: DataFrame, table_name: str):
        """Profile data and generate quality rules"""
        profiler = Profiler()
        profile = profiler.profile(df)
        
        # Save profile
        profile.write.mode("overwrite").saveAsTable(
            f"{self.config['catalog']}.{self.config['schema']}.profile_{table_name}"
        )
        
        # Generate rules
        rule_generator = RuleGenerator()
        suggested_rules = rule_generator.generate_rules(
            profile,
            confidence_threshold=0.95
        )
        
        # Save suggested rules for review
        suggested_rules.write.mode("overwrite").saveAsTable(
            f"{self.config['catalog']}.{self.config['schema']}.suggested_rules_{table_name}"
        )
        
        return suggested_rules
    
    def _load_config(self, config_path: str, environment: str):
        """Load environment-specific configuration"""
        # Implementation
        pass
    
    def _load_rules(self, table_name: str):
        """Load quality rules for table"""
        # Implementation
        pass
    
    def _setup_logging(self):
        """Configure logging"""
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        return logger
    
    def _send_alert(self, table_name: str, summary: dict):
        """Send alert on quality issues"""
        # Integration with alerting system
        pass

# Usage
pipeline = ProductionQualityPipeline(
    config_path="/config/quality_rules.yaml",
    environment="production"
)

# Batch validation
raw_df = spark.table("bronze.transactions")
clean_df = pipeline.validate_batch(raw_df, "transactions")
clean_df.write.mode("overwrite").saveAsTable("silver.transactions")

# Streaming validation
stream_df = spark.readStream.table("bronze_streaming.events")
clean_stream = pipeline.validate_stream(stream_df, "events")
clean_stream.writeStream.table("silver_streaming.events")
```

---

## 🎓 Hands-On Lab Exercises

### Lab 1: Basic Quality Checks
- Set up DQX environment
- Create sample datasets
- Implement column-level checks
- Run validation and review results

### Lab 2: Advanced Validation Rules
- Implement row-level rules
- Create cross-column validations
- Test business logic rules
- Handle edge cases

### Lab 3: Data Profiling
- Profile multiple datasets
- Generate quality rules
- Fine-tune thresholds
- Apply auto-generated rules

### Lab 4: Reaction Strategies
- Configure drop reactions
- Implement marking strategy
- Set up quarantine tables
- Build remediation workflow

### Lab 5: Streaming Quality
- Create streaming pipeline
- Apply real-time validation
- Monitor streaming quality
- Handle late data

### Lab 6: DLT Integration
- Build DLT pipeline with DQX
- Implement medallion architecture
- Track quality metrics
- Monitor pipeline health

### Lab 7: Quality Dashboard
- Collect quality metrics
- Create dashboard views
- Build trend analysis
- Configure alerts

### Lab 8: Production Pipeline
- Implement production pattern
- Configure CI/CD
- Set up monitoring
- Deploy to production

---

## 📊 Assessment and Certification

### Knowledge Check Questions:
1. What are the three main reaction types in DQX?
2. How does DQX differ from native DLT expectations?
3. When should you use row-level vs column-level checks?
4. How do you handle quarantined records?
5. What metrics should you track for data quality?

### Practical Assessment:
Build a complete data quality pipeline that:
- Ingests raw data from multiple sources
- Applies comprehensive quality checks
- Handles failed records appropriately
- Tracks quality metrics
- Provides quality dashboard
- Runs in production environment

---

## 🔗 Additional Resources

### Official Documentation
- [DQX Documentation](https://databrickslabs.github.io/dqx/)
- [DQX GitHub Repository](https://github.com/databrickslabs/dqx)
- [Databricks Data Quality Guide](https://docs.databricks.com/data-governance/data-quality.html)

### Related Workshops
- Delta Lake Fundamentals
- Lakeflow Pipelines (DLT) Workshop
- Data Governance with Unity Catalog

### Community
- Databricks Community Forums
- DQX GitHub Discussions
- Databricks Labs Projects

---

## 🤝 Contributing

Have suggestions or improvements? Contributions are welcome!

---

## 📝 Notes

- All code examples are tested on Databricks Runtime 13.3 LTS or higher
- DQX is actively maintained by Databricks Labs
- Check official documentation for latest features and updates

---

## 📧 Support

For questions or issues:
- Open an issue on [DQX GitHub](https://github.com/databrickslabs/dqx/issues)
- Contact Databricks Support
- Reach out to your Databricks account team

---

**Happy Data Quality Engineering with DQX! 🚀**
