# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Validation Framework
# MAGIC
# MAGIC **Module 5: Building Reusable Validation Library**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 50 minutes    |
# MAGIC | Level           | 200/300       |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC In this hands-on lab, you will:
# MAGIC
# MAGIC 1. **Build ValidationResult class** - Standard result format
# MAGIC 2. **Create ValidationRule base class** - Abstract rule definition
# MAGIC 3. **Implement specific rules** - NotNull, ValueRange, Unique, Regex, Referential Integrity
# MAGIC 4. **Build ValidationEngine** - Execute and manage rules
# MAGIC 5. **Generate reports** - Human-readable validation results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Any
from datetime import datetime
import json

# Set catalog and schema
catalog = "main"
schema = "default"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"USE SCHEMA {schema}")

print(f"✅ Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Test Data

# COMMAND ----------

# Create comprehensive test dataset
test_data = [
    (1, "john@example.com", "555-123-4567", 25, 150.50, "2024-01-15", "2024-01-20", "completed", 100),
    (2, "jane@test.com", "555-234-5678", 32, 275.00, "2024-01-16", "2024-01-22", "completed", 200),
    (3, "invalid-email", "555-345-6789", 45, 89.99, "2024-01-17", "2024-01-23", "pending", 300),
    (None, "bob@test.com", "555-456-7890", 28, 450.00, "2024-01-18", "2024-01-24", "completed", 400),  # NULL ID
    (5, None, "555-567-8901", 35, 125.75, "2024-01-19", "2024-01-25", "completed", 500),  # NULL email
    (6, "alice@example.com", "invalid", 150, 200.00, "2024-01-20", "2024-01-26", "cancelled", 600),  # Age outlier
    (7, "charlie@test.com", "555-678-9012", 29, -50.00, "2024-01-21", "2024-01-27", "pending", 700),  # Negative amount
    (8, "diana@example.com", "555-789-0123", 33, 175.50, "2024-01-25", "2024-01-22", "completed", 800),  # Ship before order
    (1, "eve@test.com", "555-890-1234", 41, 300.00, "2024-01-23", "2024-01-28", "completed", 900),  # Duplicate ID
    (10, "frank@test.com", "555-901-2345", 28, 225.00, "2024-01-24", "2024-01-29", "invalid", 999),  # Invalid status
]

test_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("ship_date", StringType(), True),
    StructField("status", StringType(), True),
    StructField("customer_id", IntegerType(), True)
])

df_test = spark.createDataFrame(test_data, test_schema)
df_test.write.format("delta").mode("overwrite").saveAsTable("orders_validation_test")

print(f"✅ Created test dataset with {df_test.count()} records")
display(df_test)

# COMMAND ----------

# Create reference customer table for referential integrity testing
customer_data = [
    (100, "Customer A"),
    (200, "Customer B"),
    (300, "Customer C"),
    (400, "Customer D"),
    (500, "Customer E"),
    (600, "Customer F"),
    (700, "Customer G"),
    (800, "Customer H"),
    (900, "Customer I"),
    # Note: customer_id 999 is missing (orphan in orders)
]

customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("customer_name", StringType(), False)
])

df_customers = spark.createDataFrame(customer_data, customer_schema)
df_customers.write.format("delta").mode("overwrite").saveAsTable("customers_reference")

print(f"✅ Created reference customer table with {df_customers.count()} records")
display(df_customers)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: ValidationResult Class
# MAGIC
# MAGIC Create a standard format for validation results

# COMMAND ----------

@dataclass
class ValidationResult:
    """Standard validation result"""
    rule_name: str
    passed: bool
    failed_count: int
    total_count: int
    failure_rate: float
    details: Dict[str, Any]
    severity: str = "ERROR"
    
    @property
    def success_rate(self):
        """Calculate success rate"""
        return 1 - self.failure_rate
    
    def to_dict(self):
        """Convert to dictionary"""
        return {
            'rule_name': self.rule_name,
            'passed': self.passed,
            'failed_count': self.failed_count,
            'total_count': self.total_count,
            'failure_rate': self.failure_rate,
            'success_rate': self.success_rate,
            'severity': self.severity,
            'details': self.details
        }

# Test ValidationResult
result = ValidationResult(
    rule_name="test_rule",
    passed=False,
    failed_count=10,
    total_count=100,
    failure_rate=0.10,
    details={"column": "test_col"},
    severity="ERROR"
)

print("ValidationResult example:")
print(f"  Rule: {result.rule_name}")
print(f"  Passed: {result.passed}")
print(f"  Failed: {result.failed_count}/{result.total_count}")
print(f"  Success Rate: {result.success_rate*100:.2f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: ValidationRule Base Class
# MAGIC
# MAGIC Create abstract base class for all validation rules

# COMMAND ----------

class ValidationRule(ABC):
    """Base class for validation rules"""
    
    def __init__(self, rule_name: str, severity: str = 'ERROR'):
        """
        Initialize validation rule
        
        Args:
            rule_name: Unique name for this rule
            severity: ERROR, WARNING, or INFO
        """
        self.rule_name = rule_name
        self.severity = severity
        
        if severity not in ['ERROR', 'WARNING', 'INFO']:
            raise ValueError(f"Invalid severity: {severity}. Must be ERROR, WARNING, or INFO")
    
    @abstractmethod
    def validate(self, df) -> ValidationResult:
        """
        Execute validation and return result
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationResult object
        """
        pass
    
    def __repr__(self):
        return f"{self.__class__.__name__}(rule_name='{self.rule_name}', severity='{self.severity}')"

print("✅ ValidationRule base class created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: NotNullRule
# MAGIC
# MAGIC Validate that a column has no NULL values

# COMMAND ----------

class NotNullRule(ValidationRule):
    """Validate that column has no NULL values"""
    
    def __init__(self, column_name: str, severity: str = 'ERROR'):
        super().__init__(f"not_null_{column_name}", severity)
        self.column_name = column_name
    
    def validate(self, df) -> ValidationResult:
        """Check for NULL values in column"""
        
        total = df.count()
        null_count = df.filter(col(self.column_name).isNull()).count()
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=null_count == 0,
            failed_count=null_count,
            total_count=total,
            failure_rate=null_count / total if total > 0 else 0,
            details={
                'column': self.column_name,
                'null_count': null_count
            },
            severity=self.severity
        )

# Test NotNullRule
rule = NotNullRule("order_id", "ERROR")
result = rule.validate(df_test)

print(f"Rule: {rule.rule_name}")
print(f"Passed: {result.passed}")
print(f"Failed: {result.failed_count}/{result.total_count}")
print(f"Details: {result.details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: ValueRangeRule
# MAGIC
# MAGIC Validate that values are within an acceptable range

# COMMAND ----------

class ValueRangeRule(ValidationRule):
    """Validate that values are within acceptable range"""
    
    def __init__(self, column_name: str, min_value: float, max_value: float, severity: str = 'ERROR'):
        super().__init__(f"range_{column_name}_{min_value}_to_{max_value}", severity)
        self.column_name = column_name
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, df) -> ValidationResult:
        """Check if values are within range"""
        
        total = df.count()
        out_of_range = df.filter(
            col(self.column_name).isNotNull() &
            ((col(self.column_name) < self.min_value) | (col(self.column_name) > self.max_value))
        ).count()
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=out_of_range == 0,
            failed_count=out_of_range,
            total_count=total,
            failure_rate=out_of_range / total if total > 0 else 0,
            details={
                'column': self.column_name,
                'min_value': self.min_value,
                'max_value': self.max_value,
                'violations': out_of_range
            },
            severity=self.severity
        )

# Test ValueRangeRule
rule = ValueRangeRule("amount", 0.01, 1000000, "ERROR")
result = rule.validate(df_test)

print(f"Rule: {rule.rule_name}")
print(f"Passed: {result.passed}")
print(f"Failed: {result.failed_count}/{result.total_count}")
print(f"Details: {result.details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: UniqueRule
# MAGIC
# MAGIC Validate uniqueness of column or combination of columns

# COMMAND ----------

class UniqueRule(ValidationRule):
    """Validate uniqueness of column(s)"""
    
    def __init__(self, columns: List[str], severity: str = 'ERROR'):
        if isinstance(columns, str):
            columns = [columns]
        super().__init__(f"unique_{'_'.join(columns)}", severity)
        self.columns = columns
    
    def validate(self, df) -> ValidationResult:
        """Check for duplicate values"""
        
        total = df.count()
        distinct = df.select(self.columns).distinct().count()
        duplicates = total - distinct
        
        return ValidationResult(
            rule_name=self.rule_name,
            passed=duplicates == 0,
            failed_count=duplicates,
            total_count=total,
            failure_rate=duplicates / total if total > 0 else 0,
            details={
                'columns': self.columns,
                'duplicate_count': duplicates,
                'distinct_count': distinct
            },
            severity=self.severity
        )

# Test UniqueRule
rule = UniqueRule("order_id", "ERROR")
result = rule.validate(df_test)

print(f"Rule: {rule.rule_name}")
print(f"Passed: {result.passed}")
print(f"Failed: {result.failed_count}/{result.total_count}")
print(f"Details: {result.details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 6: RegexRule
# MAGIC
# MAGIC Validate that column values match a regex pattern

# COMMAND ----------

class RegexRule(ValidationRule):
    """Validate that column values match regex pattern"""
    
    def __init__(self, column_name: str, pattern: str, pattern_name: str, severity: str = 'ERROR'):
        super().__init__(f"regex_{column_name}_{pattern_name}", severity)
        self.column_name = column_name
        self.pattern = pattern
        self.pattern_name = pattern_name
    
    def validate(self, df) -> ValidationResult:
        """Check if values match regex pattern"""
        
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
                'pattern_name': self.pattern_name,
                'pattern': self.pattern,
                'invalid_count': invalid
            },
            severity=self.severity
        )

# Test RegexRule - Email validation
rule = RegexRule("email", r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', "email_format", "WARNING")
result = rule.validate(df_test)

print(f"Rule: {rule.rule_name}")
print(f"Passed: {result.passed}")
print(f"Failed: {result.failed_count}/{result.total_count}")
print(f"Details: {result.details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 7: ReferentialIntegrityRule
# MAGIC
# MAGIC Validate foreign key relationships

# COMMAND ----------

class ReferentialIntegrityRule(ValidationRule):
    """Validate foreign key relationships"""
    
    def __init__(self, source_column: str, reference_table: str, reference_column: str, severity: str = 'ERROR'):
        super().__init__(f"fk_{source_column}_to_{reference_table}_{reference_column}", severity)
        self.source_column = source_column
        self.reference_table = reference_table
        self.reference_column = reference_column
    
    def validate(self, df) -> ValidationResult:
        """Check for orphaned records"""
        
        total = df.count()
        
        try:
            reference_df = spark.table(self.reference_table)
            
            # Find orphaned records (records in source but not in reference)
            orphans = df.filter(col(self.source_column).isNotNull()).join(
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
                    'reference_column': self.reference_column,
                    'orphaned_records': orphans
                },
                severity=self.severity
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.rule_name,
                passed=False,
                failed_count=total,
                total_count=total,
                failure_rate=1.0,
                details={
                    'error': str(e),
                    'message': f"Could not validate referential integrity: {str(e)}"
                },
                severity=self.severity
            )

# Test ReferentialIntegrityRule
rule = ReferentialIntegrityRule("customer_id", "customers_reference", "customer_id", "ERROR")
result = rule.validate(df_test)

print(f"Rule: {rule.rule_name}")
print(f"Passed: {result.passed}")
print(f"Failed: {result.failed_count}/{result.total_count}")
print(f"Details: {result.details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 8: CustomRule
# MAGIC
# MAGIC Create a flexible rule for custom validations

# COMMAND ----------

class CustomRule(ValidationRule):
    """Custom validation rule using SQL expression or Python function"""
    
    def __init__(self, rule_name: str, validation_expr: str, severity: str = 'ERROR'):
        """
        Args:
            rule_name: Name of the rule
            validation_expr: SQL expression that returns true for valid records
            severity: ERROR, WARNING, or INFO
        """
        super().__init__(rule_name, severity)
        self.validation_expr = validation_expr
    
    def validate(self, df) -> ValidationResult:
        """Execute custom validation"""
        
        total = df.count()
        
        try:
            # Count records that PASS the validation
            passed = df.filter(expr(self.validation_expr)).count()
            failed = total - passed
            
            return ValidationResult(
                rule_name=self.rule_name,
                passed=failed == 0,
                failed_count=failed,
                total_count=total,
                failure_rate=failed / total if total > 0 else 0,
                details={
                    'validation_expression': self.validation_expr,
                    'passed_count': passed,
                    'failed_count': failed
                },
                severity=self.severity
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.rule_name,
                passed=False,
                failed_count=total,
                total_count=total,
                failure_rate=1.0,
                details={
                    'error': str(e),
                    'validation_expression': self.validation_expr
                },
                severity=self.severity
            )

# Test CustomRule - Ship date must be after order date
rule = CustomRule("ship_after_order", "ship_date >= order_date", "ERROR")
result = rule.validate(df_test)

print(f"Rule: {rule.rule_name}")
print(f"Passed: {result.passed}")
print(f"Failed: {result.failed_count}/{result.total_count}")
print(f"Details: {result.details}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 9: ValidationEngine
# MAGIC
# MAGIC Create an engine to execute and manage multiple validation rules

# COMMAND ----------

class ValidationEngine:
    """Execute and manage validation rules"""
    
    def __init__(self):
        self.rules: List[ValidationRule] = []
        self.results: List[ValidationResult] = []
    
    def add_rule(self, rule: ValidationRule):
        """Add a validation rule"""
        self.rules.append(rule)
        return self
    
    def add_rules(self, rules: List[ValidationRule]):
        """Add multiple rules"""
        self.rules.extend(rules)
        return self
    
    def validate(self, df) -> Dict[str, Any]:
        """Execute all validation rules"""
        self.results = []
        
        print(f"Executing {len(self.rules)} validation rules...")
        
        for rule in self.rules:
            try:
                print(f"  Running: {rule.rule_name}")
                result = rule.validate(df)
                self.results.append(result)
            except Exception as e:
                print(f"  ❌ Error in rule {rule.rule_name}: {str(e)}")
                # Create failed result
                self.results.append(ValidationResult(
                    rule_name=rule.rule_name,
                    passed=False,
                    failed_count=df.count(),
                    total_count=df.count(),
                    failure_rate=1.0,
                    details={'error': str(e)},
                    severity=rule.severity
                ))
        
        return self.get_summary()
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate validation summary"""
        total_rules = len(self.results)
        passed_rules = sum(1 for r in self.results if r.passed)
        failed_rules = total_rules - passed_rules
        
        # Count by severity
        critical_failures = [r for r in self.results if not r.passed and r.severity == 'ERROR']
        warning_failures = [r for r in self.results if not r.passed and r.severity == 'WARNING']
        info_failures = [r for r in self.results if not r.passed and r.severity == 'INFO']
        
        return {
            'total_rules': total_rules,
            'passed_rules': passed_rules,
            'failed_rules': failed_rules,
            'pass_rate': (passed_rules / total_rules * 100) if total_rules > 0 else 0,
            'critical_failures': len(critical_failures),
            'warning_failures': len(warning_failures),
            'info_failures': len(info_failures),
            'overall_passed': len(critical_failures) == 0,
            'results': self.results
        }
    
    def generate_report(self) -> str:
        """Generate human-readable validation report"""
        summary = self.get_summary()
        
        report = []
        report.append("=" * 80)
        report.append("DATA QUALITY VALIDATION REPORT")
        report.append("=" * 80)
        report.append(f"Total Rules Executed: {summary['total_rules']}")
        report.append(f"Passed: {summary['passed_rules']}")
        report.append(f"Failed: {summary['failed_rules']}")
        report.append(f"Pass Rate: {summary['pass_rate']:.2f}%")
        report.append("")
        report.append(f"Critical Failures (ERROR): {summary['critical_failures']}")
        report.append(f"Warning Failures (WARNING): {summary['warning_failures']}")
        report.append(f"Info Failures (INFO): {summary['info_failures']}")
        report.append("")
        report.append(f"Overall Status: {'✅ PASSED' if summary['overall_passed'] else '❌ FAILED'}")
        report.append("")
        
        # Details by severity
        if summary['critical_failures'] > 0:
            report.append("CRITICAL FAILURES (ERROR):")
            report.append("-" * 80)
            for result in self.results:
                if not result.passed and result.severity == 'ERROR':
                    report.append(f"❌ {result.rule_name}")
                    report.append(f"   Failed: {result.failed_count} / {result.total_count} records ({result.failure_rate*100:.2f}%)")
                    report.append(f"   Details: {result.details}")
                    report.append("")
        
        if summary['warning_failures'] > 0:
            report.append("WARNINGS:")
            report.append("-" * 80)
            for result in self.results:
                if not result.passed and result.severity == 'WARNING':
                    report.append(f"⚠️  {result.rule_name}")
                    report.append(f"   Failed: {result.failed_count} / {result.total_count} records ({result.failure_rate*100:.2f}%)")
                    report.append(f"   Details: {result.details}")
                    report.append("")
        
        # Passed rules summary
        if summary['passed_rules'] > 0:
            report.append(f"PASSED RULES ({summary['passed_rules']}):")
            report.append("-" * 80)
            for result in self.results:
                if result.passed:
                    report.append(f"✅ {result.rule_name}")
        
        report.append("=" * 80)
        
        return "\n".join(report)
    
    def save_results(self, table_name: str, source_table: str):
        """Save validation results to Delta table"""
        
        records = []
        for result in self.results:
            record = {
                'validation_timestamp': datetime.now(),
                'source_table': source_table,
                'rule_name': result.rule_name,
                'severity': result.severity,
                'passed': result.passed,
                'failed_count': result.failed_count,
                'total_count': result.total_count,
                'failure_rate': result.failure_rate,
                'details': json.dumps(result.details)
            }
            records.append(record)
        
        if records:
            df_results = spark.createDataFrame(records)
            df_results.write.format("delta").mode("append").saveAsTable(table_name)
            print(f"✅ Saved validation results to {table_name}")

print("✅ ValidationEngine created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 10: Test Complete Validation Framework

# COMMAND ----------

# Create validation engine
engine = ValidationEngine()

# Add comprehensive validation rules
engine.add_rules([
    # Critical validations (ERROR severity)
    NotNullRule("order_id", "ERROR"),
    NotNullRule("customer_id", "ERROR"),
    UniqueRule("order_id", "ERROR"),
    ValueRangeRule("amount", 0.01, 1000000, "ERROR"),
    ValueRangeRule("age", 0, 120, "ERROR"),
    CustomRule("ship_after_order", "ship_date >= order_date", "ERROR"),
    CustomRule("status_valid", "status IN ('pending', 'completed', 'cancelled')", "ERROR"),
    ReferentialIntegrityRule("customer_id", "customers_reference", "customer_id", "ERROR"),
    
    # Warnings (WARNING severity)
    NotNullRule("email", "WARNING"),
    RegexRule("email", r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$', "email_format", "WARNING"),
    RegexRule("phone", r'^\d{3}-\d{3}-\d{4}$', "phone_format", "WARNING"),
])

# Execute validation
summary = engine.validate(df_test)

# Generate and print report
print("\n")
print(engine.generate_report())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 11: Save Validation Results

# COMMAND ----------

# Create results table
spark.sql("""
    CREATE TABLE IF NOT EXISTS validation_results (
        validation_timestamp TIMESTAMP,
        source_table STRING,
        rule_name STRING,
        severity STRING,
        passed BOOLEAN,
        failed_count BIGINT,
        total_count BIGINT,
        failure_rate DOUBLE,
        details STRING
    ) USING DELTA
""")

# Save results
engine.save_results("validation_results", "orders_validation_test")

# View results
display(spark.sql("""
    SELECT 
        validation_timestamp,
        rule_name,
        severity,
        passed,
        failed_count,
        total_count,
        ROUND(failure_rate * 100, 2) as failure_pct
    FROM validation_results
    ORDER BY passed ASC, severity ASC, failure_rate DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 12: Create Quality Score
# MAGIC
# MAGIC Calculate overall quality score based on validation results

# COMMAND ----------

def calculate_quality_score(validation_summary: Dict) -> float:
    """
    Calculate overall quality score (0-100) with severity weighting
    """
    
    # Weight by severity
    error_weight = 0.6
    warning_weight = 0.3
    info_weight = 0.1
    
    # Calculate pass rate by severity
    error_results = [r for r in validation_summary['results'] if r.severity == 'ERROR']
    warning_results = [r for r in validation_summary['results'] if r.severity == 'WARNING']
    info_results = [r for r in validation_summary['results'] if r.severity == 'INFO']
    
    error_pass_rate = sum(1 for r in error_results if r.passed) / len(error_results) if error_results else 1.0
    warning_pass_rate = sum(1 for r in warning_results if r.passed) / len(warning_results) if warning_results else 1.0
    info_pass_rate = sum(1 for r in info_results if r.passed) / len(info_results) if info_results else 1.0
    
    weighted_score = (
        error_pass_rate * error_weight +
        warning_pass_rate * warning_weight +
        info_pass_rate * info_weight
    )
    
    return weighted_score * 100

# Calculate quality score
quality_score = calculate_quality_score(summary)

print("=" * 80)
print(f"OVERALL QUALITY SCORE: {quality_score:.2f} / 100")
print("=" * 80)
print()
print("Score Breakdown:")
print(f"  Total Rules: {summary['total_rules']}")
print(f"  Passed: {summary['passed_rules']}")
print(f"  Failed: {summary['failed_rules']}")
print(f"  Critical Failures: {summary['critical_failures']}")
print(f"  Warnings: {summary['warning_failures']}")
print()

if quality_score >= 95:
    print("✅ EXCELLENT - Data quality exceeds standards")
elif quality_score >= 85:
    print("✅ GOOD - Data quality meets standards")
elif quality_score >= 70:
    print("⚠️  ACCEPTABLE - Data quality needs improvement")
else:
    print("❌ POOR - Data quality requires immediate attention")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ### What You Built
# MAGIC 1. **ValidationResult** - Standard result format with pass/fail status
# MAGIC 2. **ValidationRule** - Abstract base class for all rules
# MAGIC 3. **Specific Rules** - NotNull, ValueRange, Unique, Regex, Referential Integrity, Custom
# MAGIC 4. **ValidationEngine** - Orchestrates multiple rules
# MAGIC 5. **Quality Scoring** - Weighted quality calculation
# MAGIC
# MAGIC ### Best Practices
# MAGIC - Use severity levels appropriately (ERROR for critical, WARNING for important)
# MAGIC - Make rules reusable and composable
# MAGIC - Provide detailed error information in results
# MAGIC - Save validation results for trend analysis
# MAGIC - Calculate quality scores for easy monitoring
# MAGIC
# MAGIC ### Next Steps
# MAGIC Use this framework in:
# MAGIC - Lab 3: Bronze Layer Quality
# MAGIC - Lab 4: Silver Layer Quality
# MAGIC - Lab 5: Gold Layer Quality
# MAGIC - Lab 10: Production Pipeline
# MAGIC
# MAGIC ---
# MAGIC **Duration**: 50 minutes | **Level**: 200/300 | **Type**: Lab
