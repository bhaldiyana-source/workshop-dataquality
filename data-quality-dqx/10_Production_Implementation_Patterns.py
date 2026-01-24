# Databricks notebook source
# MAGIC %md
# MAGIC # Production Implementation Patterns
# MAGIC
# MAGIC **Module 11: Enterprise-Grade Data Quality Pipeline**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 60 minutes    |
# MAGIC | Level           | 400           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC %undefined
# MAGIC ### ⚠️ Implementation Note
# MAGIC
# MAGIC This notebook demonstrates **production-ready data quality pipeline patterns** using PySpark-based implementations.
# MAGIC
# MAGIC **Key Adaptations:**
# MAGIC * Production pipeline class implemented with PySpark
# MAGIC * Configuration-driven validation using Delta tables
# MAGIC * Metrics collection and monitoring with Unity Catalog
# MAGIC * Error handling and logging patterns
# MAGIC * All production concepts remain the same as the conceptual DQX API
# MAGIC
# MAGIC **Why?** The `databricks-labs-dqx` package uses a different API structure than the simplified demo API shown here. This implementation demonstrates production patterns using working PySpark code.
# MAGIC
# MAGIC **For production use**, refer to the [databricks-labs-dqx documentation](https://databrickslabs.github.io/dqx/) for the actual API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Build production-ready** quality pipelines
# MAGIC 2. **Implement comprehensive** error handling
# MAGIC 3. **Set up monitoring** and alerting
# MAGIC 4. **Manage configurations** across environments
# MAGIC 5. **Deploy quality pipelines** to production

# COMMAND ----------

# DBTITLE 1,Install PyYAML
# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Production Pipeline Architecture
# MAGIC
# MAGIC ### Architecture Overview
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                      Data Sources                                │
# MAGIC │  (Kafka, S3, JDBC, APIs, Files)                                 │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                  Configuration Management                        │
# MAGIC │  (Unity Catalog, Delta Tables, Git)                             │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC                          ▼
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │            Production Quality Pipeline                           │
# MAGIC │  ┌───────────┐  ┌──────────────┐  ┌────────────────┐          │
# MAGIC │  │  Profiler │  │   Validator  │  │  Quarantine    │          │
# MAGIC │  └───────────┘  └──────────────┘  └────────────────┘          │
# MAGIC │  ┌───────────┐  ┌──────────────┐  ┌────────────────┐          │
# MAGIC │  │  Metrics  │  │   Alerts     │  │  Remediation   │          │
# MAGIC │  └───────────┘  └──────────────┘  └────────────────┘          │
# MAGIC └────────────────────────┬────────────────────────────────────────┘
# MAGIC                          │
# MAGIC          ┌───────────────┼───────────────┐
# MAGIC          ▼               ▼               ▼
# MAGIC   ┌────────────┐  ┌────────────┐  ┌──────────────┐
# MAGIC   │ Clean Data │  │ Quarantine │  │   Metrics    │
# MAGIC   │   (Delta)  │  │   (Delta)  │  │  Dashboard   │
# MAGIC   └────────────┘  └────────────┘  └──────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Cell 4
# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
import yaml
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List
import random

print("✅ Libraries imported successfully")
print("Using PySpark for production data quality patterns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Production Quality Pipeline Class

# COMMAND ----------

# DBTITLE 1,Production Configuration
# Create production configuration
prod_config = {
    "version": "1.0",
    "environment": "production",
    "catalog": "production",
    "schema": "quality",
    "metrics": {
        "catalog": "production",
        "schema": "metrics",
        "enabled": True
    },
    "alerts": {
        "enabled": True,
        "pass_rate_threshold": 0.95,
        "channels": ["slack", "pagerduty"]
    },
    "tables": [
        {
            "name": "transactions",
            "checks": [
                {
                    "name": "transaction_id_not_null",
                    "type": "not_null",
                    "column": "transaction_id",
                    "level": "error"
                },
                {
                    "name": "customer_id_not_null",
                    "type": "not_null",
                    "column": "customer_id",
                    "level": "error"
                },
                {
                    "name": "amount_range",
                    "type": "between",
                    "column": "amount",
                    "min_value": 0,
                    "max_value": 1000000,
                    "level": "error"
                }
            ],
            "quarantine": {
                "table": "production.quality.quarantine_transactions",
                "partition_by": ["date"],
                "add_metadata": True
            }
        }
    ]
}

print("✅ Production configuration created")
print(f"   Environment: {prod_config['environment']}")
print(f"   Catalog: {prod_config['catalog']}.{prod_config['schema']}")
print(f"   Tables configured: {len(prod_config['tables'])}")

# COMMAND ----------

# DBTITLE 1,Cell 6
class ProductionQualityPipeline:
    """
    Production-ready data quality pipeline with comprehensive features:
    - Configuration-driven validation
    - Metrics collection and monitoring
    - Error handling and logging
    - Quarantine management
    - Alerting integration
    """
    
    def __init__(self, config: Dict[str, Any], environment: str = "production"):
        """
        Initialize production quality pipeline
        
        Args:
            config: Configuration dictionary
            environment: Environment name (development, staging, production)
        """
        self.environment = environment
        self.config = config
        self.logger = self._setup_logging()
        self.metrics_table = f"{config['catalog']}.{config['schema']}.validation_metrics"
        
        # Create metrics table if it doesn't exist
        self._setup_metrics_table()
        
        self.logger.info(f"Production Quality Pipeline initialized for {environment}")
    
    def _setup_logging(self) -> logging.Logger:
        """Configure logging"""
        logger = logging.getLogger(f"DQX-{self.environment}")
        logger.setLevel(logging.INFO)
        
        # Add handler if not exists
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _setup_metrics_table(self):
        """Create metrics table if it doesn't exist"""
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.config['catalog']}.{self.config['schema']}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.metrics_table} (
                    timestamp TIMESTAMP,
                    environment STRING,
                    table_name STRING,
                    check_name STRING,
                    check_type STRING,
                    level STRING,
                    records_processed BIGINT,
                    records_failed BIGINT,
                    pass_rate DOUBLE
                )
                USING DELTA
            """)
        except Exception as e:
            self.logger.warning(f"Could not create metrics table: {str(e)}")
    
    def validate_batch(self, df, table_name: str) -> Tuple:
        """
        Validate batch data with comprehensive quality checks
        
        Args:
            df: Input DataFrame to validate
            table_name: Name of the table being validated
            
        Returns:
            Tuple of (clean_df, quarantine_df, summary_dict)
        """
        self.logger.info(f"Starting validation for {table_name}")
        
        try:
            # Get table configuration
            table_config = self._get_table_config(table_name)
            
            if not table_config:
                raise ValueError(f"No configuration found for table: {table_name}")
            
            # Apply quality checks
            validated_df = df
            check_results = []
            
            for check_config in table_config.get('checks', []):
                validated_df, check_result = self._apply_check(validated_df, check_config)
                check_results.append(check_result)
            
            # Separate clean and quarantine records
            check_cols = [col for col in validated_df.columns if col.startswith('check_')]
            
            # Calculate failures per row
            failure_expr = sum([F.when(F.col(c) == "FAILED", 1).otherwise(0) for c in check_cols])
            validated_df = validated_df.withColumn("_total_failures", failure_expr)
            
            # Split into clean and quarantine
            clean_df = validated_df.filter(F.col("_total_failures") == 0).drop("_total_failures", *check_cols)
            quarantine_df = validated_df.filter(F.col("_total_failures") > 0)
            
            # Calculate summary
            total_records = df.count()
            clean_records = clean_df.count()
            quarantine_records = quarantine_df.count()
            pass_rate = clean_records / total_records if total_records > 0 else 0
            
            summary = {
                'total_records': total_records,
                'valid_records': clean_records,
                'failed_records': quarantine_records,
                'pass_rate': pass_rate,
                'check_results': check_results
            }
            
            # Save metrics
            self._save_metrics(table_name, check_results)
            
            # Log results
            self.logger.info(f"Validation completed for {table_name}")
            self.logger.info(f"Pass rate: {pass_rate:.2%}")
            
            # Check for alerts
            self._check_alerts(table_name, summary)
            
            # Save quarantine if configured
            if quarantine_records > 0 and table_config.get('quarantine'):
                self._save_quarantine(quarantine_df, table_name, table_config['quarantine'])
            
            return clean_df, quarantine_df, summary
            
        except Exception as e:
            self.logger.error(f"Validation failed for {table_name}: {str(e)}")
            raise
    
    def _get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get configuration for specific table"""
        for table in self.config.get('tables', []):
            if table['name'] == table_name:
                return table
        return {}
    
    def _apply_check(self, df, check_config: Dict) -> Tuple:
        """Apply a single quality check"""
        check_name = check_config['name']
        check_type = check_config['type']
        column = check_config.get('column')
        result_col = f"check_{check_name}"
        
        if check_type == 'not_null':
            df = df.withColumn(
                result_col,
                F.when(F.col(column).isNotNull(), F.lit("PASSED")).otherwise(F.lit("FAILED"))
            )
        elif check_type == 'unique':
            # For unique check, use window function
            from pyspark.sql.window import Window
            w = Window.partitionBy(column)
            df = df.withColumn(
                result_col,
                F.when(F.count(column).over(w) == 1, F.lit("PASSED")).otherwise(F.lit("FAILED"))
            )
        elif check_type == 'between':
            min_val = check_config['min_value']
            max_val = check_config['max_value']
            df = df.withColumn(
                result_col,
                F.when(
                    (F.col(column) >= min_val) & (F.col(column) <= max_val),
                    F.lit("PASSED")
                ).otherwise(F.lit("FAILED"))
            )
        
        # Calculate check result
        total = df.count()
        failed = df.filter(F.col(result_col) == "FAILED").count()
        
        check_result = {
            'check_name': check_name,
            'check_type': check_type,
            'level': check_config.get('level', 'error'),
            'records_processed': total,
            'records_failed': failed,
            'pass_rate': (total - failed) / total if total > 0 else 0
        }
        
        return df, check_result
    
    def _save_metrics(self, table_name: str, check_results: List[Dict]):
        """Save metrics to Delta table"""
        try:
            metrics_data = [
                (
                    datetime.now(),
                    self.environment,
                    table_name,
                    result['check_name'],
                    result['check_type'],
                    result['level'],
                    result['records_processed'],
                    result['records_failed'],
                    result['pass_rate']
                )
                for result in check_results
            ]
            
            metrics_df = spark.createDataFrame(
                metrics_data,
                ["timestamp", "environment", "table_name", "check_name", "check_type",
                 "level", "records_processed", "records_failed", "pass_rate"]
            )
            
            metrics_df.write.mode("append").saveAsTable(self.metrics_table)
        except Exception as e:
            self.logger.warning(f"Could not save metrics: {str(e)}")
    
    def _save_quarantine(self, quarantine_df, table_name: str, quarantine_config: Dict):
        """Save quarantined records"""
        try:
            quarantine_table = quarantine_config['table']
            
            # Add metadata
            quarantine_df = quarantine_df.withColumn("_quarantine_timestamp", F.current_timestamp())
            quarantine_df = quarantine_df.withColumn("_source_table", F.lit(table_name))
            
            # Save to quarantine table
            quarantine_df.write.mode("append").saveAsTable(quarantine_table)
            
            self.logger.info(f"Saved {quarantine_df.count()} records to quarantine")
        except Exception as e:
            self.logger.warning(f"Could not save quarantine records: {str(e)}")
    
    def _check_alerts(self, table_name: str, summary: Dict):
        """Check if alerts should be triggered"""
        alert_config = self.config.get('alerts', {})
        pass_rate_threshold = alert_config.get('pass_rate_threshold', 0.95)
        
        pass_rate = summary.get('pass_rate', 1.0)
        
        if pass_rate < pass_rate_threshold:
            self._send_alert(
                severity="HIGH",
                table_name=table_name,
                message=f"Quality degradation detected: {pass_rate:.2%} pass rate",
                details=summary
            )
    
    def _send_alert(self, severity: str, table_name: str, message: str, details: Dict):
        """Send alert (integrate with your alerting system)"""
        alert = {
            "timestamp": datetime.now().isoformat(),
            "severity": severity,
            "environment": self.environment,
            "table": table_name,
            "message": message,
            "details": details
        }
        
        # Log alert
        self.logger.warning(f"ALERT: {json.dumps(alert, default=str)}")
        
        # Here you would integrate with:
        # - PagerDuty
        # - Slack
        # - Email
        # - Databricks Alerts
        # etc.
    
    def get_quality_metrics(self, table_name: str = None, days: int = 7):
        """Retrieve quality metrics for monitoring"""
        try:
            metrics_df = spark.table(self.metrics_table)
            
            # Filter by time window
            metrics_df = metrics_df.filter(
                f"timestamp >= current_timestamp() - INTERVAL {days} DAYS"
            )
            
            # Filter by table if specified
            if table_name:
                metrics_df = metrics_df.filter(f"table_name = '{table_name}'")
            
            return metrics_df
        except Exception as e:
            self.logger.error(f"Could not retrieve metrics: {str(e)}")
            return None

print("✅ ProductionQualityPipeline class defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Configuration Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Initialize Production Pipeline

# COMMAND ----------

# DBTITLE 1,Cell 12
# Initialize pipeline with config dict
pipeline = ProductionQualityPipeline(
    config=prod_config,
    environment="production"
)

print("✅ Production pipeline initialized")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Run Production Validation

# COMMAND ----------

# Create sample production data
production_data = [
    (1, "TXN001", "CUST001", 1000.00, "completed", "2024-01-15"),
    (2, "TXN002", "CUST002", 500.00, "completed", "2024-01-16"),
    (3, "TXN003", "CUST003", 1500.00, "completed", "2024-01-17"),
    (4, "TXN004", "CUST004", 2000.00, "completed", "2024-01-18"),
    (5, "TXN005", "CUST005", 750.00, "completed", "2024-01-19"),
]

transactions_df = spark.createDataFrame(
    production_data,
    ["id", "transaction_id", "customer_id", "amount", "status", "date"]
)

print("Sample production data:")
display(transactions_df)

# COMMAND ----------

# DBTITLE 1,Cell 16
# Run validation
clean_df, quarantine_df, summary = pipeline.validate_batch(transactions_df, "transactions")

print(f"\nProduction Validation Summary:")
print(f"  Total Records: {summary.get('total_records', 0)}")
print(f"  Valid Records: {summary.get('valid_records', 0)}")
print(f"  Failed Records: {summary.get('failed_records', 0)}")
print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")

print(f"\nClean Data:")
display(clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: End-to-End Production Example

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Production Workflow

# COMMAND ----------

# DBTITLE 1,Production Workflow Function
def production_workflow_demo(source_df, table_name: str, pipeline):
    """
    Complete production data quality workflow (demonstration)
    
    Steps:
    1. Validate data
    2. Monitor metrics
    3. Generate report
    
    Note: In production, you would write clean data to target tables
    """
    try:
        # Step 1: Validate
        print(f"✅ Validating data for: {table_name}")
        clean_df, quarantine_df, summary = pipeline.validate_batch(source_df, table_name)
        
        # Step 2: Monitor metrics
        print(f"📊 Checking quality metrics...")
        metrics_df = pipeline.get_quality_metrics(table_name, days=1)
        
        # Step 3: Report
        print(f"\n{'='*60}")
        print(f"PRODUCTION WORKFLOW COMPLETED")
        print(f"{'='*60}")
        print(f"Table Name: {table_name}")
        print(f"Records Processed: {summary.get('total_records', 0):,}")
        print(f"Records Valid: {summary.get('valid_records', 0):,}")
        print(f"Records Failed: {summary.get('failed_records', 0):,}")
        print(f"Pass Rate: {summary.get('pass_rate', 0):.2%}")
        print(f"\nIn production, clean data would be written to target table")
        print(f"{'='*60}\n")
        
        return clean_df, summary
        
    except Exception as e:
        print(f"❌ Production workflow failed: {str(e)}")
        raise

print("✅ Production workflow function defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Operational Playbooks
# MAGIC
# MAGIC ### Playbook 1: Daily Quality Check

# COMMAND ----------

def daily_quality_check():
    """
    Daily quality check playbook
    Run this daily to monitor overall data quality
    """
    print("="*60)
    print("DAILY QUALITY CHECK")
    print("="*60)
    
    # 1. Get metrics for last 24 hours
    metrics_df = pipeline.get_quality_metrics(days=1)
    
    # 2. Calculate daily KPIs
    daily_kpis = metrics_df.agg(
        F.sum("records_processed").alias("total_records"),
        F.sum("records_failed").alias("total_failures"),
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.countDistinct("table_name").alias("tables_validated")
    ).collect()[0]
    
    print(f"\n📊 Daily Quality Summary:")
    print(f"   Total Records: {daily_kpis['total_records']:,}")
    print(f"   Total Failures: {daily_kpis['total_failures']:,}")
    print(f"   Average Pass Rate: {daily_kpis['avg_pass_rate']:.2%}")
    print(f"   Tables Validated: {daily_kpis['tables_validated']}")
    
    # 3. Identify issues
    issues = (
        metrics_df
        .filter("pass_rate < 0.95")
        .select("table_name", "check_name", "pass_rate")
        .distinct()
    )
    
    if issues.count() > 0:
        print(f"\n⚠️  Quality Issues Detected:")
        display(issues)
    else:
        print(f"\n✅ No quality issues detected")
    
    print("="*60)

# Run daily check
daily_quality_check()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Playbook 2: Quarantine Review and Remediation

# COMMAND ----------

def quarantine_review_playbook():
    """
    Review and remediate quarantined records
    Run this weekly or as needed
    """
    print("="*60)
    print("QUARANTINE REVIEW PLAYBOOK")
    print("="*60)
    
    # 1. Load quarantined records
    quarantine_table = "production.quality.quarantine_transactions"
    
    try:
        quarantine_df = spark.table(quarantine_table)
        total_quarantined = quarantine_df.count()
        
        print(f"\n📦 Quarantined Records: {total_quarantined:,}")
        
        if total_quarantined == 0:
            print("✅ No quarantined records to review")
            return
        
        # 2. Analyze by failure reason
        print(f"\n🔍 Failure Analysis:")
        failure_analysis = (
            quarantine_df
            .groupBy("dqx_failed_checks")
            .agg(F.count("*").alias("count"))
            .orderBy(F.desc("count"))
        )
        display(failure_analysis)
        
        # 3. Sample quarantined records
        print(f"\n📋 Sample Quarantined Records:")
        display(quarantine_df.limit(10))
        
        # 4. Remediation suggestions
        print(f"\n💡 Remediation Suggestions:")
        print("   1. Review failure patterns")
        print("   2. Fix data quality issues at source")
        print("   3. Apply manual corrections if needed")
        print("   4. Re-validate and merge back")
        
    except Exception as e:
        print(f"⚠️  Quarantine table not found or empty: {str(e)}")
    
    print("="*60)

# Run quarantine review
quarantine_review_playbook()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Playbook 3: Weekly Quality Report

# COMMAND ----------

def weekly_quality_report():
    """
    Generate weekly quality report
    Run this weekly for stakeholder review
    """
    print("="*60)
    print("WEEKLY QUALITY REPORT")
    print("="*60)
    
    # Get metrics for last 7 days
    metrics_df = pipeline.get_quality_metrics(days=7)
    
    # 1. Overall summary
    weekly_summary = metrics_df.agg(
        F.sum("records_processed").alias("total_records"),
        F.sum("records_failed").alias("total_failures"),
        F.avg("pass_rate").alias("avg_pass_rate"),
        F.min("pass_rate").alias("worst_pass_rate"),
        F.max("pass_rate").alias("best_pass_rate")
    ).collect()[0]
    
    print(f"\n📊 Weekly Summary:")
    print(f"   Total Records: {weekly_summary['total_records']:,}")
    print(f"   Total Failures: {weekly_summary['total_failures']:,}")
    print(f"   Average Pass Rate: {weekly_summary['avg_pass_rate']:.2%}")
    print(f"   Worst Pass Rate: {weekly_summary['worst_pass_rate']:.2%}")
    print(f"   Best Pass Rate: {weekly_summary['best_pass_rate']:.2%}")
    
    # 2. Trend analysis
    print(f"\n📈 Daily Trend:")
    daily_trend = (
        metrics_df
        .withColumn("date", F.to_date("timestamp"))
        .groupBy("date")
        .agg(
            F.avg("pass_rate").alias("daily_pass_rate"),
            F.sum("records_processed").alias("daily_records")
        )
        .orderBy("date")
    )
    display(daily_trend)
    
    # 3. Top issues
    print(f"\n⚠️  Top Issues:")
    top_issues = (
        metrics_df
        .filter("records_failed > 0")
        .groupBy("table_name", "check_name")
        .agg(F.sum("records_failed").alias("total_failures"))
        .orderBy(F.desc("total_failures"))
        .limit(10)
    )
    display(top_issues)
    
    print("="*60)

# Generate weekly report
weekly_quality_report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **Production pipeline** requires comprehensive error handling
# MAGIC
# MAGIC ✅ **Configuration-driven** approach enables flexibility
# MAGIC
# MAGIC ✅ **Metrics and monitoring** are essential for operations
# MAGIC
# MAGIC ✅ **Alerting integration** enables proactive quality management
# MAGIC
# MAGIC ✅ **Operational playbooks** provide standard procedures
# MAGIC
# MAGIC ✅ **End-to-end workflow** covers all quality lifecycle stages
# MAGIC
# MAGIC ✅ **Production-grade code** includes logging, error handling, and monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ## Production Checklist
# MAGIC
# MAGIC ### Pre-Production
# MAGIC - [ ] Quality rules defined and tested
# MAGIC - [ ] Configuration management set up
# MAGIC - [ ] Metrics collection configured
# MAGIC - [ ] Quarantine tables created
# MAGIC - [ ] Alert thresholds defined
# MAGIC - [ ] Documentation completed
# MAGIC
# MAGIC ### Production Deployment
# MAGIC - [ ] Deploy to production environment
# MAGIC - [ ] Configure monitoring dashboards
# MAGIC - [ ] Set up alerting integrations
# MAGIC - [ ] Train operations team
# MAGIC - [ ] Establish on-call rotation
# MAGIC
# MAGIC ### Post-Production
# MAGIC - [ ] Monitor quality metrics daily
# MAGIC - [ ] Review quarantine weekly
# MAGIC - [ ] Generate quality reports
# MAGIC - [ ] Update rules as needed
# MAGIC - [ ] Conduct quarterly reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You have completed the **Data Quality with DQX Framework Workshop**!
# MAGIC
# MAGIC You now have the skills to:
# MAGIC - ✅ Build comprehensive data quality solutions with DQX
# MAGIC - ✅ Implement production-grade quality pipelines
# MAGIC - ✅ Monitor and maintain data quality at scale
# MAGIC - ✅ Integrate DQX with enterprise data platforms
# MAGIC
# MAGIC ### Next Steps:
# MAGIC 1. **Practice** with your own datasets
# MAGIC 2. **Implement** quality checks in your pipelines
# MAGIC 3. **Share** knowledge with your team
# MAGIC 4. **Contribute** to the DQX community
# MAGIC
# MAGIC ### Resources:
# MAGIC - [DQX Documentation](https://databrickslabs.github.io/dqx/)
# MAGIC - [DQX GitHub](https://github.com/databrickslabs/dqx)
# MAGIC - [Databricks Community](https://community.databricks.com/)
# MAGIC
# MAGIC **Thank you for participating! Happy Data Quality Engineering! 🚀**
