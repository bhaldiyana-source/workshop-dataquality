# Data Quality on Databricks: Comprehensive Workshop

| Field           | Details       | Description                                                                 |
|-----------------|---------------|-----------------------------------------------------------------------------|
| Duration        | 360 minutes   | Estimated duration to complete all labs (6 hours). |
| Level           | 200/300       | Target difficulty level (200 = intermediate, 300 = advanced). |
| Lab Status      | Active        | Workshop is actively maintained and updated. |
| Course Location | N/A           | Lab is only available in this repo. |
| Developer       | Ajit Kalura   | Primary developer(s) of the lab. |
| Reviewer        | Ajit Kalura   | Subject matter expert reviewer(s). |
| Product Version | Databricks Runtime 13.3+, Delta Lake 3.0+, Python 3.10+ | Required versions. |

---

## Description

This comprehensive workshop teaches you to build enterprise-grade data quality frameworks on Databricks. You'll master data quality dimensions, validation strategies, profiling techniques, and automated testing across the medallion architecture (Bronze-Silver-Gold layers). Learn to detect, prevent, and remediate data quality issues at scale using Delta Lake features, Great Expectations, PySpark, and custom quality frameworks.

## Learning Objectives

By the end of this workshop, you will be able to:

### Foundation
- Understand the six dimensions of data quality
- Implement data quality checks at each medallion layer
- Design comprehensive validation strategies
- Build quarantine and remediation workflows

### Technical Skills
- Use Delta Lake constraints and expectations
- Integrate Great Expectations with Databricks
- Implement custom data quality frameworks
- Profile data at scale using PySpark
- Create data quality dashboards and alerts

### Production Capabilities
- Build automated quality gates in pipelines
- Implement SLA-based quality monitoring
- Track data lineage and quality metrics
- Handle schema drift and evolution
- Create comprehensive quality reports

### Advanced Techniques
- Implement statistical anomaly detection
- Build ML-based quality scoring
- Create domain-specific validation rules
- Optimize quality checks for performance
- Handle data quality in streaming pipelines

## Requirements & Prerequisites

Before starting this workshop, ensure you have:
- **Databricks workspace** with Unity Catalog enabled
- **Cluster access** (DBR 13.3+ recommended)
- **Unity Catalog permissions** for CREATE TABLE and MODIFY
- **Intermediate SQL and Python/PySpark** knowledge
- **Understanding of data pipelines** and ETL concepts
- **Basic statistics knowledge** for anomaly detection
- **Familiarity with Delta Lake** (recommended but not required)

## Contents

### Lectures
1. **Data Quality Fundamentals** - Dimensions, principles, and frameworks
2. **Medallion Architecture for Quality** - Bronze, Silver, Gold quality patterns
3. **Delta Lake Quality Features** - Constraints, expectations, and CDF
4. **Quality Monitoring & Alerting** - Metrics, dashboards, and SLAs

### Labs
1. **Data Quality Profiling** - Automated profiling and statistics
2. **Validation Framework** - Building reusable validation library
3. **Bronze Layer Quality** - Schema validation and completeness
4. **Silver Layer Quality** - Business rules and transformations
5. **Gold Layer Quality** - Aggregation validation and consistency
6. **Great Expectations Integration** - Enterprise quality testing
7. **Quarantine & Remediation** - Handling bad data
8. **Quality Monitoring Dashboard** - Real-time quality tracking
9. **Anomaly Detection** - Statistical and ML-based detection
10. **Production Pipeline** - End-to-end quality implementation

### Resources
- Data quality assessment templates
- Validation rule library
- Quality metrics catalog
- Dashboard templates
- Troubleshooting guide

---

## Workshop Structure

### Module 1: Data Quality Fundamentals (30 minutes)

**Lecture: Understanding Data Quality**

#### Six Dimensions of Data Quality

1. **Accuracy** - Correctness of data values
   - Are values correct and truthful?
   - Do they match real-world entities?
   - Examples: Valid email format, correct calculations

2. **Completeness** - Presence of required data
   - Are all mandatory fields populated?
   - Is data missing randomly or systematically?
   - Examples: NULL checks, required field validation

3. **Consistency** - Agreement across datasets
   - Do related values agree?
   - Is data consistent across systems?
   - Examples: Referential integrity, cross-field validation

4. **Timeliness** - Data recency and availability
   - Is data available when needed?
   - How old is the data?
   - Examples: Freshness checks, latency monitoring

5. **Validity** - Data conforms to rules/standards
   - Does data meet defined formats?
   - Are values within acceptable ranges?
   - Examples: Data type validation, regex patterns

6. **Uniqueness** - No unwanted duplicates
   - Are records unique when they should be?
   - Is there data redundancy?
   - Examples: Primary key validation, deduplication

#### Data Quality Framework Components

```
┌─────────────────────────────────────────────────────────┐
│                   DATA QUALITY FRAMEWORK                 │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  1. PROFILING          → Understand current state       │
│     - Statistical analysis                               │
│     - Pattern detection                                  │
│     - Anomaly identification                             │
│                                                          │
│  2. VALIDATION         → Enforce quality rules          │
│     - Schema validation                                  │
│     - Business rules                                     │
│     - Data type checks                                   │
│                                                          │
│  3. MONITORING         → Track quality metrics          │
│     - Real-time dashboards                               │
│     - Quality scorecards                                 │
│     - Trend analysis                                     │
│                                                          │
│  4. REMEDIATION        → Fix quality issues             │
│     - Quarantine workflows                               │
│     - Data cleansing                                     │
│     - Automated fixes                                    │
│                                                          │
│  5. PREVENTION         → Stop issues at source          │
│     - Input validation                                   │
│     - Schema enforcement                                 │
│     - Quality gates                                      │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

#### Quality Metrics and KPIs

**Core Metrics:**
- **Quality Score**: Percentage of records passing all validations
- **Completeness Rate**: Percentage of non-null required fields
- **Accuracy Rate**: Percentage of values within expected ranges
- **Timeliness**: Average data latency from source to destination
- **Duplication Rate**: Percentage of duplicate records
- **Schema Drift Incidents**: Count of unexpected schema changes

**Business Impact Metrics:**
- **Data-Driven Decision Quality**: Accuracy of insights
- **Downstream System Failures**: Caused by data quality issues
- **Manual Remediation Time**: Hours spent fixing data
- **Customer Impact**: Issues reported due to bad data
- **Compliance Violations**: Failed regulatory checks

---

### Module 2: Medallion Architecture for Data Quality (45 minutes)

**Lecture: Quality Patterns Across Bronze-Silver-Gold**

#### Bronze Layer: Raw Data Quality

**Purpose**: Capture data exactly as received, minimal transformation

**Quality Focus:**
- ✅ Schema validation
- ✅ Completeness checks (critical fields present)
- ✅ Data capture metadata (source, timestamp)
- ✅ Full data retention (no filtering)
- ✅ Audit trail

**Quality Checks:**
```python
# Bronze Layer Quality Pattern
def validate_bronze_quality(df):
    """Validate raw data ingestion quality"""
    
    checks = {
        'schema_match': check_schema_compatibility(df),
        'record_count': df.count() > 0,
        'critical_fields': validate_critical_fields(df),
        'data_types': validate_data_types(df),
        'source_metadata': check_metadata_present(df)
    }
    
    return checks
```

**Strategies:**
- Append-only writes (preserve everything)
- Minimal rejection (only malformed/unparseable data)
- Schema evolution enabled
- Change Data Feed enabled for tracking
- Partition by ingestion date for lifecycle management

#### Silver Layer: Business Quality

**Purpose**: Cleansed, validated, enriched data ready for analytics

**Quality Focus:**
- ✅ Business rule validation
- ✅ Data standardization and normalization
- ✅ Duplicate detection and resolution
- ✅ Referential integrity
- ✅ Data enrichment and derivation
- ✅ Quality scoring

**Quality Checks:**
```python
# Silver Layer Quality Pattern
def validate_silver_quality(df):
    """Comprehensive business quality validation"""
    
    checks = {
        # Accuracy
        'valid_emails': validate_email_format(df),
        'valid_phone_numbers': validate_phone_format(df),
        'valid_amounts': check_amount_ranges(df),
        
        # Consistency
        'referential_integrity': check_foreign_keys(df),
        'cross_field_rules': validate_business_rules(df),
        
        # Completeness
        'required_fields': check_required_fields(df),
        'null_rates': calculate_null_percentages(df),
        
        # Uniqueness
        'primary_key_uniqueness': check_pk_duplicates(df),
        'duplicate_records': find_duplicates(df),
        
        # Timeliness
        'data_freshness': check_data_age(df),
        
        # Validity
        'value_ranges': validate_value_ranges(df),
        'categorical_values': validate_enums(df)
    }
    
    return checks
```

**Strategies:**
- Quarantine invalid records
- Flag quality issues (don't always reject)
- Add quality score columns
- Implement progressive validation (critical → nice-to-have)
- Track data lineage

#### Gold Layer: Aggregation Quality

**Purpose**: Business-ready metrics and aggregations

**Quality Focus:**
- ✅ Aggregation accuracy
- ✅ Metric consistency
- ✅ Historical comparison validation
- ✅ Business logic correctness
- ✅ Performance optimization

**Quality Checks:**
```python
# Gold Layer Quality Pattern
def validate_gold_quality(df):
    """Validate aggregated metrics quality"""
    
    checks = {
        # Mathematical accuracy
        'sum_checks': validate_aggregation_sums(df),
        'count_accuracy': verify_record_counts(df),
        'avg_reasonableness': check_average_ranges(df),
        
        # Consistency
        'period_over_period': compare_to_previous_period(df),
        'cross_metric_validation': validate_metric_relationships(df),
        
        # Completeness
        'all_dimensions_present': check_dimension_completeness(df),
        'time_series_gaps': detect_missing_periods(df),
        
        # Business rules
        'known_totals': validate_against_known_values(df),
        'trend_anomalies': detect_unusual_changes(df)
    }
    
    return checks
```

**Strategies:**
- Compare to expected totals
- Validate against previous periods
- Check for logical impossibilities
- Implement business-specific validations
- Monitor query performance

---

### Module 3: Delta Lake Quality Features (40 minutes)

**Lecture: Built-in Quality Capabilities**

#### Table Constraints

**NOT NULL Constraints:**
```sql
-- Enforce NOT NULL at table level
ALTER TABLE silver_customers
ADD CONSTRAINT customer_id_not_null CHECK (customer_id IS NOT NULL);

-- Multiple constraints
ALTER TABLE silver_orders
ADD CONSTRAINT order_id_not_null CHECK (order_id IS NOT NULL),
ADD CONSTRAINT amount_positive CHECK (amount > 0),
ADD CONSTRAINT status_valid CHECK (status IN ('pending', 'completed', 'cancelled'));
```

**CHECK Constraints:**
```sql
-- Value range validation
ALTER TABLE silver_transactions
ADD CONSTRAINT amount_range CHECK (amount BETWEEN 0.01 AND 1000000);

-- Date logic validation
ALTER TABLE silver_orders
ADD CONSTRAINT valid_dates CHECK (ship_date >= order_date);

-- Complex business rules
ALTER TABLE silver_customers
ADD CONSTRAINT email_format CHECK (email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$');
```

#### Schema Evolution and Enforcement

**Schema Enforcement (Default):**
```python
# Prevents incompatible schema changes
df_bad_schema.write.format("delta").mode("append").saveAsTable("customers")
# Throws error if schema doesn't match

# Delta Lake protects data quality by default!
```

**Schema Evolution (Opt-in):**
```python
# Allow schema evolution for additive changes
df_with_new_column.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("customers")

# New columns added with NULL for existing records
```

**Schema Validation Strategy:**
```python
def validate_schema_compatibility(source_df, target_table):
    """Validate schema before write"""
    
    target_schema = spark.table(target_table).schema
    source_schema = source_df.schema
    
    # Check for missing required columns
    missing_cols = set(target_schema.fieldNames()) - set(source_schema.fieldNames())
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Check for data type compatibility
    for field in source_schema:
        if field.name in target_schema.fieldNames():
            target_type = [f.dataType for f in target_schema if f.name == field.name][0]
            if field.dataType != target_type:
                raise TypeError(f"Incompatible type for {field.name}")
    
    return True
```

#### Change Data Feed for Quality Tracking

```python
# Enable CDF to track all changes
spark.sql("""
    ALTER TABLE silver_customers
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

# Query changes to audit data quality
changes = spark.read.format("delta") \
    .option("readChangeData", "true") \
    .option("startingVersion", 10) \
    .table("silver_customers")

# Track quality issues over time
quality_audit = changes.filter(col("_change_type").isin("update", "delete")) \
    .groupBy("_commit_version") \
    .agg(
        count("*").alias("records_changed"),
        countDistinct("customer_id").alias("customers_affected")
    )
```

#### Expectations (Delta Live Tables)

```python
# Define expectations in DLT pipelines
@dlt.expect("valid_email", "email IS NOT NULL AND email RLIKE '^[^@]+@[^@]+\\.[^@]+$'")
@dlt.expect("positive_amount", "amount > 0")
@dlt.expect_or_drop("required_customer", "customer_id IS NOT NULL")
@dlt.expect_or_fail("critical_timestamp", "event_timestamp IS NOT NULL")
def silver_orders():
    return spark.readStream.table("bronze_orders")
```

---

### Module 4: Data Profiling (Lab - 45 minutes)

**Build automated data profiling capabilities**

#### Statistical Profiling

```python
class DataProfiler:
    """Comprehensive data profiling for quality assessment"""
    
    def profile_table(self, table_name):
        """Generate complete data profile"""
        df = spark.table(table_name)
        
        profile = {
            'row_count': df.count(),
            'column_count': len(df.columns),
            'columns': self.profile_columns(df),
            'duplicates': self.find_duplicates(df),
            'patterns': self.detect_patterns(df),
            'relationships': self.analyze_relationships(df)
        }
        
        return profile
    
    def profile_columns(self, df):
        """Profile each column"""
        profiles = {}
        
        for col_name in df.columns:
            col_type = df.schema[col_name].dataType
            
            profile = {
                'type': str(col_type),
                'null_count': df.filter(col(col_name).isNull()).count(),
                'null_percentage': self.calculate_null_pct(df, col_name),
                'distinct_count': df.select(col_name).distinct().count(),
                'cardinality': self.calculate_cardinality(df, col_name)
            }
            
            # Numeric column profiling
            if isinstance(col_type, (IntegerType, LongType, FloatType, DoubleType, DecimalType)):
                stats = df.select(
                    min(col_name).alias('min'),
                    max(col_name).alias('max'),
                    avg(col_name).alias('mean'),
                    stddev(col_name).alias('stddev')
                ).collect()[0]
                
                profile.update({
                    'min': stats['min'],
                    'max': stats['max'],
                    'mean': stats['mean'],
                    'stddev': stats['stddev'],
                    'outliers': self.detect_outliers(df, col_name)
                })
            
            # String column profiling
            elif isinstance(col_type, StringType):
                profile.update({
                    'min_length': df.select(length(col_name)).agg(min(length(col_name))).collect()[0][0],
                    'max_length': df.select(length(col_name)).agg(max(length(col_name))).collect()[0][0],
                    'avg_length': df.select(length(col_name)).agg(avg(length(col_name))).collect()[0][0],
                    'patterns': self.extract_patterns(df, col_name),
                    'top_values': self.get_top_values(df, col_name)
                })
            
            profiles[col_name] = profile
        
        return profiles
    
    def detect_outliers(self, df, col_name):
        """Detect statistical outliers using IQR method"""
        quantiles = df.stat.approxQuantile(col_name, [0.25, 0.75], 0.05)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1
        lower_bound = q1 - (1.5 * iqr)
        upper_bound = q3 + (1.5 * iqr)
        
        outliers = df.filter(
            (col(col_name) < lower_bound) | (col(col_name) > upper_bound)
        ).count()
        
        return {
            'count': outliers,
            'lower_bound': lower_bound,
            'upper_bound': upper_bound
        }
```

#### Pattern Detection

```python
def detect_data_patterns(df, column_name):
    """Detect common patterns in data"""
    
    patterns = {
        'email': df.filter(col(column_name).rlike('^[^@]+@[^@]+\\.[^@]+$')).count(),
        'phone_us': df.filter(col(column_name).rlike(r'^\d{3}-\d{3}-\d{4}$')).count(),
        'ssn': df.filter(col(column_name).rlike(r'^\d{3}-\d{2}-\d{4}$')).count(),
        'zipcode': df.filter(col(column_name).rlike(r'^\d{5}(-\d{4})?$')).count(),
        'url': df.filter(col(column_name).rlike(r'^https?://')).count(),
        'date_iso': df.filter(col(column_name).rlike(r'^\d{4}-\d{2}-\d{2}$')).count()
    }
    
    total = df.count()
    pattern_match = {k: v/total for k, v in patterns.items() if v > total * 0.5}
    
    return pattern_match
```

---

### Module 5: Validation Framework (Lab - 50 minutes)

**Build reusable validation library**

#### Core Validation Classes

```python
from abc import ABC, abstractmethod
from typing import Dict, List, Any
from dataclasses import dataclass

@dataclass
class ValidationResult:
    """Standard validation result"""
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    failure_rate: float
    details: Dict[str, Any]
    
    @property
    def success_rate(self):
        return 1 - self.failure_rate

class ValidationRule(ABC):
    """Base class for validation rules"""
    
    def __init__(self, rule_name: str, severity: str = 'ERROR'):
        self.rule_name = rule_name
        self.severity = severity  # ERROR, WARNING, INFO
    
    @abstractmethod
    def validate(self, df) -> ValidationResult:
        """Execute validation and return result"""
        pass

class NotNullRule(ValidationRule):
    """Validate that column has no NULL values"""
    
    def __init__(self, column_name: str, severity: str = 'ERROR'):
        super().__init__(f"not_null_{column_name}", severity)
        self.column_name = column_name
    
    def validate(self, df) -> ValidationResult:
        total = df.count()
        null_count = df.filter(col(self.column_name).isNull()).count()
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=null_count == 0,
            failed_count=null_count,
            total_count=total,
            failure_rate=null_count / total if total > 0 else 0,
            details={'column': self.column_name, 'null_count': null_count}
        )

class ValueRangeRule(ValidationRule):
    """Validate that values are within acceptable range"""
    
    def __init__(self, column_name: str, min_value: float, max_value: float, severity: str = 'ERROR'):
        super().__init__(f"range_{column_name}", severity)
        self.column_name = column_name
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, df) -> ValidationResult:
        total = df.count()
        out_of_range = df.filter(
            (col(self.column_name) < self.min_value) | 
            (col(self.column_name) > self.max_value)
        ).count()
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=out_of_range == 0,
            failed_count=out_of_range,
            total_count=total,
            failure_rate=out_of_range / total if total > 0 else 0,
            details={
                'column': self.column_name,
                'min': self.min_value,
                'max': self.max_value,
                'violations': out_of_range
            }
        )

class UniqueRule(ValidationRule):
    """Validate uniqueness of column or combination of columns"""
    
    def __init__(self, columns: List[str], severity: str = 'ERROR'):
        super().__init__(f"unique_{'_'.join(columns)}", severity)
        self.columns = columns
    
    def validate(self, df) -> ValidationResult:
        total = df.count()
        distinct = df.select(self.columns).distinct().count()
        duplicates = total - distinct
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=duplicates == 0,
            failed_count=duplicates,
            total_count=total,
            failure_rate=duplicates / total if total > 0 else 0,
            details={'columns': self.columns, 'duplicate_count': duplicates}
        )

class RegexRule(ValidationRule):
    """Validate that column values match regex pattern"""
    
    def __init__(self, column_name: str, pattern: str, pattern_name: str, severity: str = 'ERROR'):
        super().__init__(f"regex_{column_name}_{pattern_name}", severity)
        self.column_name = column_name
        self.pattern = pattern
        self.pattern_name = pattern_name
    
    def validate(self, df) -> ValidationResult:
        total = df.filter(col(self.column_name).isNotNull()).count()
        invalid = df.filter(
            col(self.column_name).isNotNull() & 
            ~col(self.column_name).rlike(self.pattern)
        ).count()
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=invalid == 0,
            failed_count=invalid,
            total_count=total,
            failure_rate=invalid / total if total > 0 else 0,
            details={
                'column': self.column_name,
                'pattern': self.pattern_name,
                'invalid_count': invalid
            }
        )

class ReferentialIntegrityRule(ValidationRule):
    """Validate foreign key relationships"""
    
    def __init__(self, source_column: str, reference_table: str, reference_column: str, severity: str = 'ERROR'):
        super().__init__(f"fk_{source_column}_to_{reference_table}", severity)
        self.source_column = source_column
        self.reference_table = reference_table
        self.reference_column = reference_column
    
    def validate(self, df) -> ValidationResult:
        total = df.count()
        
        reference_df = spark.table(self.reference_table)
        
        # Find orphaned records
        orphans = df.join(
            reference_df,
            df[self.source_column] == reference_df[self.reference_column],
            'left_anti'
        ).count()
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=orphans == 0,
            failed_count=orphans,
            total_count=total,
            failure_rate=orphans / total if total > 0 else 0,
            details={
                'source_column': self.source_column,
                'reference_table': self.reference_table,
                'orphaned_records': orphans
            }
        )
```

#### Validation Engine

```python
class ValidationEngine:
    """Execute and manage validation rules"""
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.results: List[ValidationResult] = []
    
    def add_rule(self, rule: ValidationRule):
        """Add validation rule"""
        self.rules.append(rule)
        return self
    
    def add_rules(self, rules: List[ValidationRule]):
        """Add multiple rules"""
        self.rules.extend(rules)
        return self
    
    def validate(self, df) -> Dict[str, Any]:
        """Execute all validation rules"""
        self.results = []
        
        for rule in self.rules:
            try:
                result = rule.validate(df)
                self.results.append(result)
            except Exception as e:
                print(f"Error executing rule {rule.rule_name}: {e}")
        
        return self.get_summary()
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate validation summary"""
        total_rules = len(self.results)
        passed_rules = sum(1 for r in self.results if r.passed)
        
        critical_failures = [r for r in self.results if not r.passed and 
                            self.get_rule_severity(r.rule_name) == 'ERROR']
        
        return {
            'total_rules': total_rules,
            'passed_rules': passed_rules,
            'failed_rules': total_rules - passed_rules,
            'pass_rate': passed_rules / total_rules if total_rules > 0 else 0,
            'critical_failures': len(critical_failures),
            'overall_passed': len(critical_failures) == 0,
            'results': self.results
        }
    
    def get_rule_severity(self, rule_name: str) -> str:
        """Get severity of a rule"""
        for rule in self.rules:
            if rule.rule_name == rule_name:
                return rule.severity
        return 'UNKNOWN'
    
    def generate_report(self) -> str:
        """Generate human-readable validation report"""
        summary = self.get_summary()
        
        report = ["=" * 80]
        report.append("DATA QUALITY VALIDATION REPORT")
        report.append("=" * 80)
        report.append(f"Total Rules Executed: {summary['total_rules']}")
        report.append(f"Passed: {summary['passed_rules']}")
        report.append(f"Failed: {summary['failed_rules']}")
        report.append(f"Pass Rate: {summary['pass_rate']*100:.2f}%")
        report.append(f"Critical Failures: {summary['critical_failures']}")
        report.append(f"Overall Status: {'✅ PASSED' if summary['overall_passed'] else '❌ FAILED'}")
        report.append("")
        
        # Failed rules details
        if summary['failed_rules'] > 0:
            report.append("FAILED RULES:")
            report.append("-" * 80)
            for result in self.results:
                if not result.passed:
                    report.append(f"❌ {result.rule_name}")
                    report.append(f"   Failed Count: {result.failed_count} / {result.total_count}")
                    report.append(f"   Failure Rate: {result.failure_rate*100:.2f}%")
                    report.append(f"   Details: {result.details}")
                    report.append("")
        
        report.append("=" * 80)
        
        return "\n".join(report)
```

---

### Module 6: Bronze Layer Quality (Lab - 40 minutes)

**Implement quality checks for raw data ingestion**

```python
def validate_bronze_layer(bronze_table: str):
    """Complete Bronze layer validation"""
    
    df = spark.table(bronze_table)
    
    # Initialize validation engine
    engine = ValidationEngine()
    
    # Schema validation rules
    engine.add_rules([
        NotNullRule('source_system', 'ERROR'),
        NotNullRule('ingestion_timestamp', 'ERROR'),
        NotNullRule('_ingested_date', 'ERROR')
    ])
    
    # Data completeness rules
    critical_fields = ['order_id', 'customer_id', 'order_date']
    for field in critical_fields:
        engine.add_rule(NotNullRule(field, 'ERROR'))
    
    # Basic data type validation
    engine.add_rules([
        ValueRangeRule('order_id', 0, float('inf'), 'ERROR'),
        ValueRangeRule('customer_id', 0, float('inf'), 'ERROR')
    ])
    
    # Execute validation
    summary = engine.validate(df)
    
    # Generate report
    print(engine.generate_report())
    
    # Store validation results
    store_validation_results(bronze_table, 'bronze', summary)
    
    return summary['overall_passed']
```

---

### Module 7: Silver Layer Quality (Lab - 60 minutes)

**Implement comprehensive Silver layer validation**

```python
def validate_silver_layer(silver_table: str):
    """Comprehensive Silver layer validation"""
    
    df = spark.table(silver_table)
    
    engine = ValidationEngine()
    
    # Accuracy validations
    engine.add_rules([
        RegexRule('email', r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$', 'email_format', 'ERROR'),
        RegexRule('phone', r'^\+?1?\d{9,15}$', 'phone_format', 'WARNING'),
        ValueRangeRule('amount', 0.01, 1000000, 'ERROR'),
        ValueRangeRule('quantity', 1, 10000, 'ERROR')
    ])
    
    # Completeness validations
    required_fields = ['order_id', 'customer_id', 'order_date', 'amount', 'status']
    for field in required_fields:
        engine.add_rule(NotNullRule(field, 'ERROR'))
    
    # Uniqueness validations
    engine.add_rule(UniqueRule(['order_id'], 'ERROR'))
    
    # Referential integrity (if reference tables exist)
    if spark.catalog.tableExists('silver_customers'):
        engine.add_rule(
            ReferentialIntegrityRule('customer_id', 'silver_customers', 'customer_id', 'ERROR')
        )
    
    # Business rule validations
    engine.add_rule(
        CustomRule('ship_after_order', 
                  lambda df: df.filter(col('ship_date') < col('order_date')).count() == 0,
                  'ERROR')
    )
    
    # Execute validation
    summary = engine.validate(df)
    
    # Quality scoring
    quality_score = calculate_quality_score(summary)
    
    # Tag records with quality scores
    df_with_quality = tag_quality_scores(df, engine.results)
    
    return summary, quality_score, df_with_quality
```

**Quality Scoring:**

```python
def calculate_quality_score(validation_summary: Dict) -> float:
    """Calculate overall quality score (0-100)"""
    
    pass_rate = validation_summary['pass_rate']
    
    # Weight by severity
    error_weight = 0.7
    warning_weight = 0.2
    info_weight = 0.1
    
    error_pass_rate = calculate_severity_pass_rate(validation_summary, 'ERROR')
    warning_pass_rate = calculate_severity_pass_rate(validation_summary, 'WARNING')
    info_pass_rate = calculate_severity_pass_rate(validation_summary, 'INFO')
    
    weighted_score = (
        error_pass_rate * error_weight +
        warning_pass_rate * warning_weight +
        info_pass_rate * info_weight
    )
    
    return weighted_score * 100
```

---

### Module 8: Gold Layer Quality (Lab - 45 minutes)

**Validate aggregations and business metrics**

```python
def validate_gold_layer(gold_table: str, silver_table: str):
    """Validate Gold layer aggregations"""
    
    gold_df = spark.table(gold_table)
    silver_df = spark.table(silver_table)
    
    validations = []
    
    # 1. Reconciliation: Gold totals match Silver source
    gold_total = gold_df.agg(sum('total_revenue')).collect()[0][0]
    silver_total = silver_df.filter(col('status') == 'completed').agg(sum('amount')).collect()[0][0]
    
    reconciliation_passed = abs(gold_total - silver_total) / silver_total < 0.001  # 0.1% tolerance
    
    validations.append({
        'check': 'reconciliation',
        'passed': reconciliation_passed,
        'gold_total': gold_total,
        'silver_total': silver_total,
        'difference': gold_total - silver_total
    })
    
    # 2. Completeness: All time periods present
    date_range = silver_df.agg(
        min('order_date').alias('min_date'),
        max('order_date').alias('max_date')
    ).collect()[0]
    
    expected_dates = spark.sql(f"""
        SELECT sequence(
            date'{date_range.min_date}',
            date'{date_range.max_date}',
            interval 1 day
        ) as date_range
    """).selectExpr("explode(date_range) as expected_date")
    
    gold_dates = gold_df.select('order_date').distinct()
    
    missing_dates = expected_dates.join(gold_dates, 
                                        expected_dates.expected_date == gold_dates.order_date,
                                        'left_anti').count()
    
    validations.append({
        'check': 'date_completeness',
        'passed': missing_dates == 0,
        'missing_dates': missing_dates
    })
    
    # 3. Consistency: Metrics match expected relationships
    # Example: avg_order_value should equal total_revenue / order_count
    inconsistent = gold_df.filter(
        abs(col('avg_order_value') - (col('total_revenue') / col('order_count'))) > 0.01
    ).count()
    
    validations.append({
        'check': 'metric_consistency',
        'passed': inconsistent == 0,
        'inconsistent_records': inconsistent
    })
    
    # 4. Trend analysis: Detect anomalies
    anomalies = detect_metric_anomalies(gold_df, 'total_revenue')
    
    validations.append({
        'check': 'trend_anomalies',
        'passed': len(anomalies) == 0,
        'anomalies': anomalies
    })
    
    return validations
```

---

### Module 9: Great Expectations Integration (Lab - 50 minutes)

**Enterprise-grade data quality testing**

```python
# Install Great Expectations
# %pip install great-expectations

import great_expectations as gx
from great_expectations.dataset import SparkDFDataset

def setup_great_expectations():
    """Initialize Great Expectations for Databricks"""
    
    context = gx.get_context()
    
    # Configure datasource
    datasource_config = {
        "name": "my_spark_datasource",
        "class_name": "Datasource",
        "execution_engine": {
            "class_name": "SparkDFExecutionEngine"
        },
        "data_connectors": {
            "default_runtime_data_connector": {
                "class_name": "RuntimeDataConnector",
                "batch_identifiers": ["default_identifier"]
            }
        }
    }
    
    context.add_datasource(**datasource_config)
    
    return context

def create_expectation_suite(suite_name: str):
    """Create comprehensive expectation suite"""
    
    context = setup_great_expectations()
    
    suite = context.create_expectation_suite(
        expectation_suite_name=suite_name,
        overwrite_existing=True
    )
    
    return suite

def validate_with_great_expectations(df, suite_name: str):
    """Validate DataFrame with Great Expectations"""
    
    # Convert to GX dataset
    gx_df = SparkDFDataset(df)
    
    # Add expectations
    gx_df.expect_table_row_count_to_be_between(min_value=1)
    gx_df.expect_column_values_to_not_be_null('order_id')
    gx_df.expect_column_values_to_be_unique('order_id')
    gx_df.expect_column_values_to_be_between('amount', min_value=0, max_value=1000000)
    gx_df.expect_column_values_to_match_regex('email', r'^[^@]+@[^@]+\.[^@]+$')
    gx_df.expect_column_values_to_be_in_set('status', ['pending', 'completed', 'cancelled'])
    
    # Validate
    results = gx_df.validate()
    
    return results

# Example: Silver layer validation with GX
silver_df = spark.table('silver_orders')
results = validate_with_great_expectations(silver_df, 'silver_orders_suite')

if results.success:
    print("✅ All Great Expectations passed!")
else:
    print("❌ Quality issues found:")
    for result in results.results:
        if not result.success:
            print(f"  - {result.expectation_config.expectation_type}: {result.result}")
```

---

### Module 10: Quarantine & Remediation (Lab - 40 minutes)

**Handle data quality failures gracefully**

```python
def quarantine_failed_records(source_table: str, validation_results: List[ValidationResult],
                              quarantine_table: str):
    """Move failed records to quarantine"""
    
    source_df = spark.table(source_table)
    
    # Build filter for failed records
    failed_filters = []
    
    for result in validation_results:
        if not result.passed:
            # Create filter based on validation rule
            filter_expr = create_failure_filter(result)
            failed_filters.append(filter_expr)
    
    if not failed_filters:
        print("No records to quarantine")
        return
    
    # Combine all failure filters
    combined_filter = reduce(lambda a, b: a | b, failed_filters)
    
    # Select failed records
    quarantine_df = source_df.filter(combined_filter) \
        .withColumn("quarantine_timestamp", current_timestamp()) \
        .withColumn("quarantine_reason", lit("validation_failure")) \
        .withColumn("validation_results", to_json(struct(*[lit(r) for r in validation_results])))
    
    # Write to quarantine
    quarantine_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(quarantine_table)
    
    print(f"✅ Quarantined {quarantine_df.count()} records to {quarantine_table}")
    
    # Remove failed records from source
    valid_df = source_df.filter(~combined_filter)
    
    valid_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(source_table)
    
    print(f"✅ {valid_df.count()} valid records remain in {source_table}")

def remediate_quarantine_records(quarantine_table: str, target_table: str):
    """Process and remediate quarantined records"""
    
    quarantine_df = spark.table(quarantine_table)
    
    # Apply remediation logic
    remediated_df = quarantine_df \
        .withColumn("email", fix_email_format(col("email"))) \
        .withColumn("phone", fix_phone_format(col("phone"))) \
        .withColumn("amount", when(col("amount") < 0, abs(col("amount"))).otherwise(col("amount"))) \
        .withColumn("remediation_timestamp", current_timestamp()) \
        .withColumn("remediation_applied", lit(True))
    
    # Re-validate
    engine = ValidationEngine()
    # Add validation rules...
    
    summary = engine.validate(remediated_df)
    
    if summary['overall_passed']:
        # Move to target table
        remediated_df.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(target_table)
        
        # Remove from quarantine
        spark.sql(f"DELETE FROM {quarantine_table} WHERE quarantine_timestamp < current_timestamp()")
        
        print(f"✅ Remediated {remediated_df.count()} records")
    else:
        print("❌ Remediation failed - records remain in quarantine")
```

---

### Module 11: Quality Monitoring Dashboard (Lab - 45 minutes)

**Real-time quality tracking and alerting**

```python
def create_quality_metrics_table():
    """Create table to store quality metrics over time"""
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS quality_metrics (
            table_name STRING,
            layer STRING,
            timestamp TIMESTAMP,
            validation_date DATE,
            total_records BIGINT,
            valid_records BIGINT,
            invalid_records BIGINT,
            quality_score DOUBLE,
            critical_failures INT,
            warning_failures INT,
            pass_rate DOUBLE,
            validation_details STRING
        ) USING DELTA
        PARTITIONED BY (validation_date)
    """)

def log_quality_metrics(table_name: str, layer: str, validation_summary: Dict):
    """Log quality metrics for monitoring"""
    
    metrics = spark.createDataFrame([{
        'table_name': table_name,
        'layer': layer,
        'timestamp': datetime.now(),
        'validation_date': date.today(),
        'total_records': validation_summary.get('total_records', 0),
        'valid_records': validation_summary.get('valid_records', 0),
        'invalid_records': validation_summary.get('invalid_records', 0),
        'quality_score': validation_summary.get('quality_score', 0),
        'critical_failures': validation_summary.get('critical_failures', 0),
        'warning_failures': validation_summary.get('warning_failures', 0),
        'pass_rate': validation_summary.get('pass_rate', 0),
        'validation_details': json.dumps(validation_summary)
    }])
    
    metrics.write.format("delta").mode("append").saveAsTable("quality_metrics")

def create_quality_dashboard():
    """Generate SQL queries for quality dashboard"""
    
    dashboard_queries = {
        'overall_quality_trend': """
            SELECT 
                validation_date,
                layer,
                AVG(quality_score) as avg_quality_score,
                AVG(pass_rate) as avg_pass_rate,
                SUM(critical_failures) as total_critical_failures
            FROM quality_metrics
            WHERE validation_date >= CURRENT_DATE - INTERVAL 30 DAYS
            GROUP BY validation_date, layer
            ORDER BY validation_date, layer
        """,
        
        'table_quality_summary': """
            SELECT 
                table_name,
                layer,
                AVG(quality_score) as avg_quality_score,
                MIN(quality_score) as min_quality_score,
                MAX(quality_score) as max_quality_score,
                SUM(critical_failures) as total_failures
            FROM quality_metrics
            WHERE validation_date >= CURRENT_DATE - INTERVAL 7 DAYS
            GROUP BY table_name, layer
            ORDER BY avg_quality_score ASC
        """,
        
        'quality_alerts': """
            SELECT 
                table_name,
                layer,
                timestamp,
                quality_score,
                critical_failures,
                validation_details
            FROM quality_metrics
            WHERE quality_score < 95
               OR critical_failures > 0
               AND validation_date >= CURRENT_DATE - INTERVAL 1 DAY
            ORDER BY quality_score ASC, critical_failures DESC
        """
    }
    
    return dashboard_queries

# Generate quality alerts
def check_quality_alerts(threshold: float = 95.0):
    """Check for quality issues and generate alerts"""
    
    alerts = spark.sql(f"""
        SELECT 
            table_name,
            layer,
            quality_score,
            critical_failures
        FROM quality_metrics
        WHERE validation_date = CURRENT_DATE
          AND (quality_score < {threshold} OR critical_failures > 0)
    """).collect()
    
    if alerts:
        print(f"⚠️ QUALITY ALERTS ({len(alerts)} issues)")
        for alert in alerts:
            print(f"  - {alert.table_name} ({alert.layer}): Score {alert.quality_score:.2f}%, Failures: {alert.critical_failures}")
        
        # Send notifications (email, Slack, PagerDuty, etc.)
        send_quality_alert_notification(alerts)
    else:
        print("✅ No quality alerts - all tables passing")
```

---

### Module 12: Anomaly Detection (Lab - 50 minutes)

**Statistical and ML-based quality anomaly detection**

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from scipy import stats

def detect_statistical_anomalies(df, numeric_columns: List[str], threshold: float = 3.0):
    """Detect anomalies using Z-score method"""
    
    anomalies = {}
    
    for col_name in numeric_columns:
        # Calculate mean and stddev
        stats_df = df.select(
            mean(col(col_name)).alias('mean'),
            stddev(col(col_name)).alias('stddev')
        ).collect()[0]
        
        mean_val = stats_df['mean']
        stddev_val = stats_df['stddev']
        
        # Calculate Z-scores
        df_with_zscore = df.withColumn(
            f'{col_name}_zscore',
            abs((col(col_name) - mean_val) / stddev_val)
        )
        
        # Find anomalies (|z-score| > threshold)
        anomaly_df = df_with_zscore.filter(col(f'{col_name}_zscore') > threshold)
        anomaly_count = anomaly_df.count()
        
        if anomaly_count > 0:
            anomalies[col_name] = {
                'count': anomaly_count,
                'percentage': anomaly_count / df.count() * 100,
                'mean': mean_val,
                'stddev': stddev_val,
                'threshold': threshold
            }
    
    return anomalies

def detect_ml_anomalies(df, feature_columns: List[str]):
    """Detect anomalies using unsupervised ML (K-Means clustering)"""
    
    # Assemble features
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    df_features = assembler.transform(df)
    
    # Train K-Means
    kmeans = KMeans(k=5, seed=42)
    model = kmeans.fit(df_features)
    
    # Get predictions and distances
    predictions = model.transform(df_features)
    
    # Calculate distance from nearest centroid
    # Records far from all centroids are anomalies
    from pyspark.ml.linalg import Vectors, VectorUDT
    from pyspark.sql.functions import udf
    
    def euclidean_distance(v1, v2):
        return float(Vectors.squared_distance(v1, v2) ** 0.5)
    
    distance_udf = udf(euclidean_distance, DoubleType())
    
    predictions_with_distance = predictions.withColumn(
        'distance_to_centroid',
        distance_udf(col('features'), 
                    array_getitem(model.clusterCenters(), col('prediction')))
    )
    
    # Define anomalies as top 5% by distance
    percentile_95 = predictions_with_distance.stat.approxQuantile(
        'distance_to_centroid', [0.95], 0.01
    )[0]
    
    anomalies = predictions_with_distance.filter(
        col('distance_to_centroid') > percentile_95
    )
    
    return anomalies

def detect_time_series_anomalies(df, date_col: str, metric_col: str):
    """Detect anomalies in time series data"""
    
    # Calculate moving average and standard deviation
    window_spec = Window.orderBy(date_col).rowsBetween(-7, 0)
    
    df_with_stats = df.withColumn(
        'moving_avg',
        avg(metric_col).over(window_spec)
    ).withColumn(
        'moving_stddev',
        stddev(metric_col).over(window_spec)
    )
    
    # Detect points beyond 3 standard deviations
    anomalies = df_with_stats.withColumn(
        'is_anomaly',
        when(
            abs(col(metric_col) - col('moving_avg')) > (3 * col('moving_stddev')),
            True
        ).otherwise(False)
    ).filter(col('is_anomaly'))
    
    return anomalies
```

---

## Key Best Practices

### Data Quality Strategy

✅ **Prevention Over Detection**
- Validate at the source
- Implement input controls
- Use schema enforcement
- Set up quality gates

✅ **Layered Validation**
- Bronze: Structural quality
- Silver: Business quality
- Gold: Analytical quality

✅ **Fail Fast and Gracefully**
- Stop bad data early
- Quarantine, don't discard
- Provide clear error messages
- Enable self-service remediation

✅ **Measure and Monitor**
- Track quality metrics over time
- Set quality SLAs
- Alert on degradation
- Trend analysis

✅ **Continuous Improvement**
- Learn from quality issues
- Update rules based on findings
- Automate common fixes
- Regular quality reviews

### Production Patterns

**Quality Framework Design:**
```python
class ProductionQualityFramework:
    """Complete production quality framework"""
    
    def __init__(self, config):
        self.profiler = DataProfiler()
        self.validator = ValidationEngine()
        self.monitor = QualityMonitor()
        self.remediator = DataRemediator()
    
    def validate_pipeline(self, bronze_table, silver_table, gold_table):
        """End-to-end pipeline validation"""
        
        # 1. Profile data
        bronze_profile = self.profiler.profile_table(bronze_table)
        
        # 2. Validate bronze
        bronze_valid = self.validator.validate_bronze(bronze_table)
        
        # 3. Validate silver
        silver_valid = self.validator.validate_silver(silver_table)
        
        # 4. Validate gold
        gold_valid = self.validator.validate_gold(gold_table, silver_table)
        
        # 5. Log metrics
        self.monitor.log_metrics(bronze_table, 'bronze', bronze_valid)
        self.monitor.log_metrics(silver_table, 'silver', silver_valid)
        self.monitor.log_metrics(gold_table, 'gold', gold_valid)
        
        # 6. Check for alerts
        self.monitor.check_alerts()
        
        # 7. Remediate if needed
        if not silver_valid:
            self.remediator.quarantine_and_remediate(silver_table)
        
        return all([bronze_valid, silver_valid, gold_valid])
```

---

## Additional Resources

### Documentation
- [Delta Lake Constraints](https://docs.delta.io/latest/delta-constraints.html)
- [Great Expectations](https://docs.greatexpectations.io/)
- [Databricks Data Quality](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Delta Live Tables Expectations](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-expectations.html)

### Tools and Libraries
- **Great Expectations**: Enterprise data quality testing
- **Deequ**: Data quality library by AWS
- **PySpark**: Built-in data validation
- **Pandera**: DataFrame schema validation
- **Delta Lake**: Built-in quality features

### Templates
- Validation rule library templates
- Quality dashboard SQL queries
- Remediation workflow patterns
- Quality SLA definitions

---

## Getting Started

1. **Complete Module 1 Lecture** - Understand data quality fundamentals
2. **Work through Labs 1-5** - Build core validation capabilities
3. **Implement Labs 6-10** - Apply to medallion architecture
4. **Build Production Pipeline** - Integrate all patterns
5. **Establish Monitoring** - Set up dashboards and alerts

---

## Support

For questions or issues:
- Open an issue in this repository
- Contact: Ajit Kalura
- Databricks Community Forums

---

**Ready to build world-class data quality into your pipelines?** Start with Module 1! 🚀
