# Databricks notebook source
"""
Data Quality Workshop Configuration
====================================

Centralized configuration for all data quality workshops.
Modify these settings based on your environment.
"""

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuration Module
# MAGIC
# MAGIC This module contains all configuration settings for the Data Quality workshop.

# COMMAND ----------

from dataclasses import dataclass
from typing import List, Dict

# COMMAND ----------

@dataclass
class CatalogConfig:
    """Unity Catalog configuration"""
    catalog: str = "main"
    schema: str = "default"
    
    def get_full_table_name(self, table_name: str) -> str:
        """Generate fully qualified table name"""
        return f"{self.catalog}.{self.schema}.{table_name}"

# COMMAND ----------

@dataclass
class QualityThresholds:
    """Quality score thresholds and SLA definitions"""
    
    # Quality score thresholds (0-100)
    gold_threshold: float = 99.0
    silver_threshold: float = 95.0
    bronze_threshold: float = 90.0
    acceptable_threshold: float = 85.0
    quarantine_threshold: float = 70.0
    
    # Completeness thresholds (percentage)
    completeness_critical: float = 99.5
    completeness_high: float = 98.0
    completeness_acceptable: float = 95.0
    
    # Anomaly detection thresholds
    zscore_threshold: float = 3.0
    iqr_multiplier: float = 1.5
    moving_avg_window_days: int = 7
    
    # Alert thresholds
    max_critical_failures: int = 0
    max_warning_failures: int = 5
    max_freshness_hours: int = 24

# COMMAND ----------

@dataclass
class TableNames:
    """Standard table naming conventions"""
    
    # Bronze layer
    bronze_suffix: str = "_bronze"
    
    # Silver layer
    silver_suffix: str = "_silver"
    silver_quarantine_suffix: str = "_silver_quarantine"
    
    # Gold layer
    gold_suffix: str = "_gold"
    gold_daily_suffix: str = "_gold_daily"
    gold_monthly_suffix: str = "_gold_monthly"
    
    # Quality tracking tables
    quality_metrics: str = "quality_metrics"
    validation_results: str = "validation_results"
    quarantine_metrics: str = "quarantine_metrics"
    anomaly_detection_results: str = "anomaly_detection_results"
    bronze_ingestion_metrics: str = "bronze_ingestion_metrics"
    data_profiles: str = "data_profiles"
    gx_validation_results: str = "gx_validation_results"

# COMMAND ----------

@dataclass
class ValidationRulesConfig:
    """Standard validation rules configuration"""
    
    # Email regex
    email_pattern: str = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
    
    # Phone regex (US format)
    phone_pattern_us: str = r'^\d{3}-\d{3}-\d{4}$'
    
    # Common value ranges
    age_min: int = 0
    age_max: int = 120
    amount_min: float = 0.01
    amount_max: float = 1000000.0
    
    # Valid status values
    valid_statuses: List[str] = None
    
    def __post_init__(self):
        if self.valid_statuses is None:
            self.valid_statuses = ["pending", "completed", "cancelled"]

# COMMAND ----------

@dataclass
class PipelineConfig:
    """Pipeline execution configuration"""
    
    # Parallelism
    shuffle_partitions: int = 200
    max_records_per_file: int = 10000
    
    # Checkpointing
    checkpoint_location: str = "/tmp/checkpoints"
    
    # Retry settings
    max_retries: int = 3
    retry_delay_seconds: int = 60
    
    # Batch settings
    batch_size: int = 10000
    enable_auto_optimize: bool = True
    enable_auto_compact: bool = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Global Configuration Instance

# COMMAND ----------

class DataQualityConfig:
    """Main configuration class for Data Quality framework"""
    
    def __init__(self):
        self.catalog = CatalogConfig()
        self.thresholds = QualityThresholds()
        self.tables = TableNames()
        self.validation_rules = ValidationRulesConfig()
        self.pipeline = PipelineConfig()
    
    def update_catalog(self, catalog: str, schema: str):
        """Update catalog configuration"""
        self.catalog.catalog = catalog
        self.catalog.schema = schema
    
    def get_table_name(self, base_name: str, layer: str) -> str:
        """Get fully qualified table name for a layer"""
        if layer == "bronze":
            suffix = self.tables.bronze_suffix
        elif layer == "silver":
            suffix = self.tables.silver_suffix
        elif layer == "gold":
            suffix = self.tables.gold_suffix
        else:
            suffix = ""
        
        table_name = f"{base_name}{suffix}"
        return self.catalog.get_full_table_name(table_name)
    
    def to_dict(self) -> Dict:
        """Convert configuration to dictionary"""
        return {
            'catalog': self.catalog.__dict__,
            'thresholds': self.thresholds.__dict__,
            'tables': self.tables.__dict__,
            'validation_rules': self.validation_rules.__dict__,
            'pipeline': self.pipeline.__dict__
        }

# COMMAND ----------

# Create global config instance
config = DataQualityConfig()

print("✅ Data Quality Configuration Loaded")
print(f"   Catalog: {config.catalog.catalog}")
print(f"   Schema: {config.catalog.schema}")
print(f"   Quality Threshold: {config.thresholds.silver_threshold}%")
print(f"   Quarantine Threshold: {config.thresholds.quarantine_threshold}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example Usage
# MAGIC
# MAGIC ```python
# MAGIC # Import configuration
# MAGIC from config import config
# MAGIC
# MAGIC # Update catalog
# MAGIC config.update_catalog("my_catalog", "my_schema")
# MAGIC
# MAGIC # Get table name
# MAGIC bronze_table = config.get_table_name("orders", "bronze")
# MAGIC # Returns: "my_catalog.my_schema.orders_bronze"
# MAGIC
# MAGIC # Access thresholds
# MAGIC if quality_score >= config.thresholds.silver_threshold:
# MAGIC     print("Quality meets Silver SLA")
# MAGIC
# MAGIC # Access validation rules
# MAGIC email_pattern = config.validation_rules.email_pattern
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Export

# COMMAND ----------

def export_config():
    """Export configuration as JSON"""
    import json
    config_dict = config.to_dict()
    print(json.dumps(config_dict, indent=2))
    return config_dict

# Uncomment to export
# export_config()
