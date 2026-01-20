# Databricks notebook source
# MAGIC %md
# MAGIC # Production Implementation Patterns
# MAGIC
# MAGIC **Module 11: Enterprise-Grade Data Quality Pipeline**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 90 minutes    |
# MAGIC | Level           | 400           |
# MAGIC | Type            | Demo          |

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

# Import required libraries
from dqx import Validator, Profiler, MetricsCollector, QuarantineConfig, ConfigValidator
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging
import yaml
import json
from datetime import datetime
from typing import Dict, Any, Tuple

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: Production Quality Pipeline Class

# COMMAND ----------

class ProductionQualityPipeline:
    """
    Production-ready data quality pipeline with comprehensive features:
    - Configuration-driven validation
    - Metrics collection and monitoring
    - Error handling and logging
    - Quarantine management
    - Alerting integration
    """
    
    def __init__(self, config_path: str, environment: str = "production"):
        """
        Initialize production quality pipeline
        
        Args:
            config_path: Path to configuration file or Delta table
            environment: Environment name (development, staging, production)
        """
        self.environment = environment
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.metrics_collector = self._setup_metrics()
        
        self.logger.info(f"Production Quality Pipeline initialized for {environment}")
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load configuration from file or Delta table"""
        try:
            if config_path.startswith("dbfs:") or config_path.startswith("/"):
                # Load from file
                with open(config_path.replace("dbfs:", "/dbfs"), 'r') as f:
                    config = yaml.safe_load(f)
            else:
                # Load from Delta table
                config_df = spark.table(config_path)
                config_row = (
                    config_df
                    .filter(f"environment = '{self.environment}'")
                    .select("config_yaml")
                    .first()
                )
                if config_row:
                    config = yaml.safe_load(config_row['config_yaml'])
                else:
                    raise ValueError(f"No config found for environment: {self.environment}")
            
            return config
        except Exception as e:
            raise RuntimeError(f"Failed to load configuration: {str(e)}")
    
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
    
    def _setup_metrics(self) -> MetricsCollector:
        """Setup metrics collector"""
        catalog = self.config.get('metrics', {}).get('catalog', 'dqx_metrics')
        schema = self.config.get('metrics', {}).get('schema', 'quality')
        table = f"{catalog}.{schema}.validation_metrics"
        
        return MetricsCollector(target_table=table)
    
    def validate_batch(self, df, table_name: str) -> Tuple:
        """
        Validate batch data with comprehensive quality checks
        
        Args:
            df: Input DataFrame to validate
            table_name: Name of the table being validated
            
        Returns:
            Tuple of (clean_df, summary_dict)
        """
        self.logger.info(f"Starting validation for {table_name}")
        
        try:
            # Create validator from configuration
            validator = self._create_validator(table_name)
            
            # Run validation
            clean_df, summary = validator.validate(df)
            
            # Log results
            self.logger.info(f"Validation completed for {table_name}")
            self.logger.info(f"Pass rate: {summary.get('pass_rate', 0):.2%}")
            
            # Check for alerts
            self._check_alerts(table_name, summary)
            
            return clean_df, summary
            
        except Exception as e:
            self.logger.error(f"Validation failed for {table_name}: {str(e)}")
            raise
    
    def _create_validator(self, table_name: str) -> Validator:
        """Create validator from configuration"""
        validator = Validator()
        validator.set_metrics_collector(self.metrics_collector)
        
        # Load rules for table
        table_config = self._get_table_config(table_name)
        
        if not table_config:
            raise ValueError(f"No configuration found for table: {table_name}")
        
        # Add checks from configuration
        for check_config in table_config.get('checks', []):
            self._add_check_from_config(validator, check_config)
        
        # Configure quarantine if specified
        quarantine_config = table_config.get('quarantine')
        if quarantine_config:
            validator.configure_quarantine(
                QuarantineConfig(**quarantine_config)
            )
        
        return validator
    
    def _get_table_config(self, table_name: str) -> Dict[str, Any]:
        """Get configuration for specific table"""
        for table in self.config.get('tables', []):
            if table['name'] == table_name:
                return table
        return {}
    
    def _add_check_from_config(self, validator: Validator, check_config: Dict):
        """Add check to validator from configuration"""
        check_type = check_config['type']
        
        if check_type == 'not_null':
            validator.add_check(
                name=check_config['name'],
                check_type=CheckType.NOT_NULL,
                column=check_config['column'],
                level=check_config['level']
            )
        elif check_type == 'unique':
            validator.add_check(
                name=check_config['name'],
                check_type=CheckType.UNIQUE,
                column=check_config['column'],
                level=check_config['level']
            )
        elif check_type == 'between':
            validator.add_check(
                name=check_config['name'],
                check_type=CheckType.BETWEEN,
                column=check_config['column'],
                min_value=check_config['min_value'],
                max_value=check_config['max_value'],
                level=check_config['level']
            )
        # Add more check types as needed...
    
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
        self.logger.warning(f"ALERT: {json.dumps(alert)}")
        
        # Here you would integrate with:
        # - PagerDuty
        # - Slack
        # - Email
        # - Databricks Alerts
        # etc.
    
    def profile_and_generate_rules(self, df, table_name: str):
        """Profile data and generate quality rule suggestions"""
        self.logger.info(f"Profiling {table_name}")
        
        try:
            # Profile data
            profiler = Profiler()
            profile = profiler.profile(df)
            
            # Save profile
            profile_table = f"{self.config['catalog']}.{self.config['schema']}.profiles"
            profile.write.mode("append").saveAsTable(profile_table)
            
            # Generate rules
            from dqx import RuleGenerator
            rule_generator = RuleGenerator()
            suggested_rules = rule_generator.generate_rules(
                profile,
                confidence_threshold=0.95
            )
            
            # Save suggested rules
            rules_table = f"{self.config['catalog']}.{self.config['schema']}.suggested_rules"
            suggested_rules.write.mode("append").saveAsTable(rules_table)
            
            self.logger.info(f"Profiling completed: {suggested_rules.count()} rules suggested")
            
            return profile, suggested_rules
            
        except Exception as e:
            self.logger.error(f"Profiling failed: {str(e)}")
            raise
    
    def get_quality_metrics(self, table_name: str = None, days: int = 7):
        """Retrieve quality metrics for monitoring"""
        metrics_table = f"{self.config['metrics']['catalog']}.{self.config['metrics']['schema']}.validation_metrics"
        metrics_df = spark.table(metrics_table)
        
        # Filter by time window
        metrics_df = metrics_df.filter(
            f"timestamp >= current_timestamp() - INTERVAL {days} DAYS"
        )
        
        # Filter by table if specified
        if table_name:
            metrics_df = metrics_df.filter(f"table_name = '{table_name}'")
        
        return metrics_df

print("✅ ProductionQualityPipeline class defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Configuration Setup

# COMMAND ----------

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
                "target_table": "production.quality.quarantine_transactions",
                "partition_by": ["date"],
                "add_metadata": True
            }
        }
    ]
}

# Save configuration
config_path = "/tmp/prod_quality_config.yaml"
dbutils.fs.put(config_path, yaml.dump(prod_config), overwrite=True)

print("✅ Production configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Initialize Production Pipeline

# COMMAND ----------

# Initialize pipeline
pipeline = ProductionQualityPipeline(
    config_path=config_path,
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

# Run validation
clean_df, summary = pipeline.validate_batch(transactions_df, "transactions")

print(f"\nProduction Validation Summary:")
print(f"  Total Records: {summary.get('total_records', 0)}")
print(f"  Valid Records: {summary.get('valid_records', 0)}")
print(f"  Failed Records: {summary.get('failed_records', 0)}")
print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")

display(clean_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: End-to-End Production Example

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Production Workflow

# COMMAND ----------

def production_workflow(source_table: str, target_table: str, pipeline):
    """
    Complete production data quality workflow
    
    Steps:
    1. Read source data
    2. Validate with DQX
    3. Write clean data
    4. Monitor metrics
    5. Handle quarantine
    """
    try:
        # Step 1: Read source data
        print(f"📖 Reading source: {source_table}")
        source_df = spark.table(source_table)
        
        # Step 2: Validate
        print(f"✅ Validating data...")
        clean_df, summary = pipeline.validate_batch(source_df, source_table)
        
        # Step 3: Write clean data
        print(f"💾 Writing clean data to: {target_table}")
        clean_df.write.mode("overwrite").saveAsTable(target_table)
        
        # Step 4: Monitor metrics
        print(f"📊 Checking quality metrics...")
        metrics_df = pipeline.get_quality_metrics(source_table, days=1)
        
        # Step 5: Report
        print(f"\n{'='*60}")
        print(f"PRODUCTION WORKFLOW COMPLETED")
        print(f"{'='*60}")
        print(f"Source Table: {source_table}")
        print(f"Target Table: {target_table}")
        print(f"Records Processed: {summary.get('total_records', 0):,}")
        print(f"Records Valid: {summary.get('valid_records', 0):,}")
        print(f"Records Failed: {summary.get('failed_records', 0):,}")
        print(f"Pass Rate: {summary.get('pass_rate', 0):.2%}")
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
