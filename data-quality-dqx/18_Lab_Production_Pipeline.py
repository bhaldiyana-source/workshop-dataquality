# Databricks notebook source
# MAGIC %md
# MAGIC # Lab: Production Pipeline
# MAGIC
# MAGIC **Hands-On Lab Exercise 8 - Final Project**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 120 minutes   |
# MAGIC | Level           | 400           |
# MAGIC | Type            | Lab           |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Objectives
# MAGIC
# MAGIC In this final lab, you will build a complete production-grade data quality pipeline:
# MAGIC 1. Design end-to-end quality architecture
# MAGIC 2. Implement production patterns and best practices
# MAGIC 3. Configure CI/CD integration
# MAGIC 4. Set up monitoring and alerting
# MAGIC 5. Deploy to production environment

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Scenario
# MAGIC
# MAGIC You are building a data quality pipeline for an e-commerce company that processes:
# MAGIC - Customer data from CRM system
# MAGIC - Transaction data from payment processing
# MAGIC - Product inventory from warehouse management
# MAGIC - Order fulfillment from shipping system
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Process 10M+ records daily
# MAGIC - Maintain 99%+ data quality
# MAGIC - Real-time monitoring and alerting
# MAGIC - Automated remediation where possible
# MAGIC - Comprehensive audit trail
# MAGIC - Support for multiple environments (dev, staging, prod)

# COMMAND ----------

# Import required libraries
from dqx import Validator, Profiler, MetricsCollector, QuarantineConfig, ConfigValidator
from pyspark.sql import functions as F
from pyspark.sql.types import *
import yaml
import json
import logging
from typing import Dict, List, Tuple
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 1: Architecture Design (30 minutes)
# MAGIC
# MAGIC ### Task 1.1: Design Data Quality Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Document your architecture design
# MAGIC
# MAGIC Create a document covering:
# MAGIC 1. **Data Flow**: How data moves through the pipeline
# MAGIC 2. **Quality Layers**: Where quality checks are applied
# MAGIC 3. **Storage Strategy**: Where clean/quarantined data is stored
# MAGIC 4. **Metrics Collection**: How quality is measured
# MAGIC 5. **Alert Strategy**: When and how to alert
# MAGIC 6. **Remediation Process**: How bad data is handled
# MAGIC
# MAGIC ```
# MAGIC Architecture Diagram:
# MAGIC
# MAGIC Source Systems → Bronze (Raw) → Silver (Validated) → Gold (Aggregated)
# MAGIC                    ↓               ↓                    ↓
# MAGIC                 Basic Checks   Comprehensive      Consistency
# MAGIC                                Validation          Checks
# MAGIC                    ↓               ↓                    ↓
# MAGIC                Metrics         Quarantine          Metrics
# MAGIC                                    ↓
# MAGIC                                Remediation
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 1.2: Define Quality Requirements

# COMMAND ----------

# TODO: Define quality requirements for each data source
quality_requirements = {
    "customers": {
        "critical_checks": ["customer_id_unique", "email_format"],
        "target_pass_rate": 0.99,
        "alert_threshold": 0.95,
        "max_null_rate": {"customer_id": 0.0, "email": 0.01}
    },
    "transactions": {
        "critical_checks": ["transaction_id_unique", "amount_positive"],
        "target_pass_rate": 0.995,
        "alert_threshold": 0.98,
        "max_null_rate": {"transaction_id": 0.0, "amount": 0.0}
    },
    # Add for products and orders
}

print("Quality Requirements Defined:")
print(json.dumps(quality_requirements, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 2: Production Pipeline Implementation (45 minutes)
# MAGIC
# MAGIC ### Task 2.1: Create Production Pipeline Class

# COMMAND ----------

# TODO: Implement comprehensive production pipeline class
class EnterpriseQualityPipeline:
    """
    Production-grade data quality pipeline with enterprise features
    """
    
    def __init__(self, environment: str, config_path: str):
        """
        Initialize pipeline
        
        Args:
            environment: dev, staging, or production
            config_path: Path to configuration file
        """
        # Your implementation here:
        pass
    
    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from file or Delta table"""
        # Your implementation here:
        pass
    
    def _setup_logging(self, environment: str):
        """Configure comprehensive logging"""
        # Your implementation here:
        pass
    
    def _setup_metrics(self):
        """Configure metrics collection"""
        # Your implementation here:
        pass
    
    def validate_batch(self, df, table_name: str) -> Tuple:
        """
        Validate batch data with error handling and retry logic
        """
        # Your implementation here:
        pass
    
    def validate_streaming(self, stream_df, table_name: str):
        """
        Validate streaming data
        """
        # Your implementation here:
        pass
    
    def profile_data(self, df, table_name: str):
        """
        Profile data and save results
        """
        # Your implementation here:
        pass
    
    def handle_quarantine(self, table_name: str):
        """
        Process quarantined records
        """
        # Your implementation here:
        pass
    
    def generate_report(self, report_type: str = "daily"):
        """
        Generate quality report
        """
        # Your implementation here:
        pass
    
    def check_alerts(self):
        """
        Check for quality alerts and send notifications
        """
        # Your implementation here:
        pass

print("✅ EnterpriseQualityPipeline class defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.2: Create Configuration Management

# COMMAND ----------

# TODO: Create comprehensive configuration file
production_config = {
    "version": "1.0.0",
    "environment": "production",
    "catalog": "prod_data",
    "schema": "quality",
    "logging": {
        "level": "INFO",
        "destination": "databricks_logs",
        "include_metrics": True
    },
    "metrics": {
        "catalog": "prod_metrics",
        "schema": "quality_metrics",
        "retention_days": 365,
        "aggregation_interval": "1 hour"
    },
    "alerts": {
        "enabled": True,
        "channels": ["slack", "pagerduty", "email"],
        "thresholds": {
            "critical_pass_rate": 0.95,
            "warning_pass_rate": 0.98,
            "failure_spike": 1000
        }
    },
    "quarantine": {
        "enabled": True,
        "auto_remediate": False,
        "retention_days": 90,
        "review_frequency": "daily"
    },
    "tables": {
        # Define table-specific configurations
    }
}

# Save configuration
config_yaml = yaml.dump(production_config, default_flow_style=False)
dbutils.fs.put("/tmp/prod_quality_config.yaml", config_yaml, overwrite=True)

print("✅ Configuration created")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 2.3: Implement Error Handling

# COMMAND ----------

# TODO: Implement comprehensive error handling
class QualityPipelineException(Exception):
    """Base exception for quality pipeline"""
    pass

class ValidationException(QualityPipelineException):
    """Exception during validation"""
    pass

class ConfigurationException(QualityPipelineException):
    """Exception in configuration"""
    pass

# Add retry logic, circuit breakers, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 3: Testing and Validation (30 minutes)
# MAGIC
# MAGIC ### Task 3.1: Create Unit Tests

# COMMAND ----------

# TODO: Implement unit tests for pipeline
import unittest

class TestQualityPipeline(unittest.TestCase):
    """Unit tests for quality pipeline"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.pipeline = EnterpriseQualityPipeline("test", "/tmp/test_config.yaml")
        # Create test data
        pass
    
    def test_configuration_loading(self):
        """Test configuration loads correctly"""
        # Your test here:
        pass
    
    def test_validation_with_clean_data(self):
        """Test validation passes with clean data"""
        # Your test here:
        pass
    
    def test_validation_with_errors(self):
        """Test validation catches errors"""
        # Your test here:
        pass
    
    def test_quarantine_mechanism(self):
        """Test quarantine works correctly"""
        # Your test here:
        pass
    
    def test_metrics_collection(self):
        """Test metrics are collected"""
        # Your test here:
        pass
    
    def test_alert_triggering(self):
        """Test alerts trigger appropriately"""
        # Your test here:
        pass

# Run tests
# suite = unittest.TestLoader().loadTestsFromTestCase(TestQualityPipeline)
# unittest.TextTestRunner(verbosity=2).run(suite)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.2: Create Integration Tests

# COMMAND ----------

# TODO: Implement integration tests
# Test end-to-end pipeline with realistic data volumes

def test_end_to_end_pipeline():
    """
    Test complete pipeline flow:
    1. Ingest data
    2. Validate
    3. Quarantine bad records
    4. Generate metrics
    5. Trigger alerts
    6. Produce reports
    """
    # Your implementation here:
    pass

# Run integration test
# test_end_to_end_pipeline()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 3.3: Performance Testing

# COMMAND ----------

# TODO: Test pipeline performance
# - Validate processing time targets
# - Test with large data volumes
# - Identify bottlenecks
# - Optimize as needed

def performance_test(record_count=1000000):
    """
    Test pipeline performance with large dataset
    """
    # Your implementation here:
    pass

# Run performance test
# performance_test(1000000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 4: CI/CD Integration (25 minutes)
# MAGIC
# MAGIC ### Task 4.1: Create CI/CD Pipeline Script

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Create CI/CD pipeline configuration
# MAGIC
# MAGIC ```yaml
# MAGIC # .github/workflows/quality-pipeline-ci.yml
# MAGIC name: Data Quality Pipeline CI
# MAGIC
# MAGIC on:
# MAGIC   push:
# MAGIC     branches: [ main, develop ]
# MAGIC   pull_request:
# MAGIC     branches: [ main ]
# MAGIC
# MAGIC jobs:
# MAGIC   test:
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - uses: actions/checkout@v2
# MAGIC       
# MAGIC       - name: Set up Python
# MAGIC         uses: actions/setup-python@v2
# MAGIC         with:
# MAGIC           python-version: '3.9'
# MAGIC       
# MAGIC       - name: Install dependencies
# MAGIC         run: |
# MAGIC           pip install databricks-labs-dqx pyspark
# MAGIC       
# MAGIC       - name: Run unit tests
# MAGIC         run: |
# MAGIC           python -m pytest tests/
# MAGIC       
# MAGIC       - name: Run quality checks on sample data
# MAGIC         run: |
# MAGIC           python scripts/validate_sample_data.py
# MAGIC       
# MAGIC       - name: Generate quality report
# MAGIC         run: |
# MAGIC           python scripts/generate_report.py
# MAGIC
# MAGIC   deploy:
# MAGIC     needs: test
# MAGIC     if: github.ref == 'refs/heads/main'
# MAGIC     runs-on: ubuntu-latest
# MAGIC     steps:
# MAGIC       - name: Deploy to Databricks
# MAGIC         run: |
# MAGIC           databricks workspace import quality_pipeline.py /Production/quality_pipeline
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 4.2: Create Deployment Scripts

# COMMAND ----------

# TODO: Create deployment automation scripts

def deploy_to_environment(environment: str):
    """
    Deploy quality pipeline to specified environment
    
    Steps:
    1. Validate configuration
    2. Run tests
    3. Deploy notebooks
    4. Update pipeline configurations
    5. Restart streaming jobs if needed
    6. Verify deployment
    """
    # Your implementation here:
    pass

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exercise 5: Monitoring and Operations (30 minutes)
# MAGIC
# MAGIC ### Task 5.1: Create Operational Runbooks

# COMMAND ----------

# MAGIC %md
# MAGIC TODO: Create operational procedures
# MAGIC
# MAGIC **Daily Operations Checklist:**
# MAGIC 1. Review quality dashboard
# MAGIC 2. Check for alerts
# MAGIC 3. Review quarantine queue
# MAGIC 4. Validate metrics collection
# MAGIC 5. Check pipeline health
# MAGIC
# MAGIC **Weekly Operations Checklist:**
# MAGIC 1. Review quality trends
# MAGIC 2. Remediate quarantined data
# MAGIC 3. Update quality rules if needed
# MAGIC 4. Generate weekly report
# MAGIC 5. Review and act on recommendations
# MAGIC
# MAGIC **Monthly Operations Checklist:**
# MAGIC 1. Review quality SLAs
# MAGIC 2. Update quality requirements
# MAGIC 3. Conduct quality review meeting
# MAGIC 4. Plan quality improvements
# MAGIC 5. Update documentation

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.2: Create Monitoring Dashboards

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TODO: Create comprehensive monitoring views
# MAGIC
# MAGIC -- Pipeline Health View
# MAGIC CREATE OR REPLACE VIEW prod_monitoring.pipeline_health AS
# MAGIC SELECT 
# MAGIC   current_timestamp() as check_time,
# MAGIC   -- Add your metrics
# MAGIC   'HEALTHY' as status
# MAGIC FROM prod_metrics.quality_metrics
# MAGIC WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR;
# MAGIC
# MAGIC -- Active Alerts View
# MAGIC CREATE OR REPLACE VIEW prod_monitoring.active_alerts AS
# MAGIC -- Your SQL here
# MAGIC
# MAGIC -- Quality SLA Tracking
# MAGIC CREATE OR REPLACE VIEW prod_monitoring.quality_sla AS
# MAGIC -- Your SQL here

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task 5.3: Implement Health Checks

# COMMAND ----------

# TODO: Implement health check endpoints

def pipeline_health_check() -> Dict:
    """
    Comprehensive pipeline health check
    Returns health status and metrics
    """
    health_status = {
        "timestamp": datetime.now().isoformat(),
        "status": "UNKNOWN",
        "checks": {
            "configuration": "PASS",
            "metrics_collection": "PASS",
            "streaming_jobs": "PASS",
            "quarantine_processing": "PASS",
            "alerting": "PASS"
        },
        "metrics": {
            "latest_pass_rate": 0.0,
            "records_processed_last_hour": 0,
            "active_alerts": 0
        }
    }
    
    # Your implementation here:
    
    return health_status

# Run health check
health = pipeline_health_check()
print(json.dumps(health, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Project Deliverables
# MAGIC
# MAGIC ### Required Deliverables
# MAGIC
# MAGIC 1. **Architecture Documentation**
# MAGIC    - ✅ Data flow diagrams
# MAGIC    - ✅ Component descriptions
# MAGIC    - ✅ Integration points
# MAGIC
# MAGIC 2. **Code Implementation**
# MAGIC    - ✅ Production pipeline class
# MAGIC    - ✅ Configuration management
# MAGIC    - ✅ Error handling
# MAGIC    - ✅ Logging and monitoring
# MAGIC
# MAGIC 3. **Testing Suite**
# MAGIC    - ✅ Unit tests
# MAGIC    - ✅ Integration tests
# MAGIC    - ✅ Performance tests
# MAGIC
# MAGIC 4. **CI/CD Configuration**
# MAGIC    - ✅ Pipeline definitions
# MAGIC    - ✅ Deployment scripts
# MAGIC    - ✅ Environment configs
# MAGIC
# MAGIC 5. **Operational Documentation**
# MAGIC    - ✅ Runbooks
# MAGIC    - ✅ Monitoring dashboards
# MAGIC    - ✅ Alert procedures
# MAGIC    - ✅ Troubleshooting guides

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lab Summary and Reflection
# MAGIC
# MAGIC ### Key Accomplishments
# MAGIC ✅ Designed enterprise-grade quality architecture
# MAGIC
# MAGIC ✅ Implemented production patterns and best practices
# MAGIC
# MAGIC ✅ Created comprehensive testing strategy
# MAGIC
# MAGIC ✅ Integrated with CI/CD pipelines
# MAGIC
# MAGIC ✅ Established monitoring and operational procedures
# MAGIC
# MAGIC ### Production Readiness Checklist
# MAGIC
# MAGIC - [ ] Architecture reviewed and approved
# MAGIC - [ ] Code complete and tested
# MAGIC - [ ] Performance validated at scale
# MAGIC - [ ] Security review completed
# MAGIC - [ ] Documentation finalized
# MAGIC - [ ] Team trained on operations
# MAGIC - [ ] Monitoring and alerts configured
# MAGIC - [ ] Runbooks created
# MAGIC - [ ] Disaster recovery tested
# MAGIC - [ ] Stakeholder sign-off obtained

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You have completed the **Data Quality with DQX Framework Workshop**!
# MAGIC
# MAGIC ### What You've Learned
# MAGIC
# MAGIC 1. **Foundations** - Core DQX concepts and capabilities
# MAGIC 2. **Basic Checks** - Column and row-level validation
# MAGIC 3. **Profiling** - Automated quality rule generation
# MAGIC 4. **Reactions** - DROP, MARK, and QUARANTINE strategies
# MAGIC 5. **Streaming** - Real-time quality validation
# MAGIC 6. **DLT Integration** - Medallion architecture with DQX
# MAGIC 7. **Configuration** - Rules management and versioning
# MAGIC 8. **Dashboards** - Monitoring and reporting
# MAGIC 9. **Advanced Topics** - Custom checks, performance, CI/CD
# MAGIC 10. **Production** - Enterprise deployment patterns
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC 1. **Apply to Your Data** - Start with one use case
# MAGIC 2. **Iterate and Improve** - Refine rules based on learnings
# MAGIC 3. **Share Knowledge** - Train your team
# MAGIC 4. **Contribute** - Help improve DQX framework
# MAGIC 5. **Stay Connected** - Join the community
# MAGIC
# MAGIC ### Resources
# MAGIC
# MAGIC - 📚 [DQX Documentation](https://databrickslabs.github.io/dqx/)
# MAGIC - 💻 [DQX GitHub](https://github.com/databrickslabs/dqx)
# MAGIC - 👥 [Databricks Community](https://community.databricks.com/)
# MAGIC - 📧 [Support Channels](https://databricks.com/support)
# MAGIC
# MAGIC **Thank you for participating! Happy Data Quality Engineering! 🚀**
