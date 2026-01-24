# Databricks notebook source
# MAGIC %md
# MAGIC # Configuration-Based Quality Checks
# MAGIC
# MAGIC **Module 8: Managing Quality Rules with Configuration**
# MAGIC
# MAGIC | Field           | Details       |
# MAGIC |-----------------|---------------|
# MAGIC | Duration        | 45 minutes    |
# MAGIC | Level           | 300           |
# MAGIC | Type            | Demo          |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demo, you will be able to:
# MAGIC
# MAGIC 1. **Define quality rules in YAML** configuration files
# MAGIC 2. **Load rules dynamically** from configuration
# MAGIC 3. **Manage environment-specific** rules (dev, staging, prod)
# MAGIC 4. **Version control** quality standards
# MAGIC 5. **Update rules** without code changes

# COMMAND ----------

# DBTITLE 1,Cell 3
# Import required libraries
from dqx import ConfigValidator
from pyspark.sql import functions as F
import yaml
import json



# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: Why Configuration-Based Rules?
# MAGIC
# MAGIC ### Benefits
# MAGIC
# MAGIC | Benefit | Description |
# MAGIC |---------|-------------|
# MAGIC | **Separation of Concerns** | Rules separate from code logic |
# MAGIC | **Easy Updates** | Change rules without redeploying code |
# MAGIC | **Version Control** | Track rule changes over time |
# MAGIC | **Environment-Specific** | Different rules for dev/prod |
# MAGIC | **Collaboration** | Non-technical stakeholders can review |
# MAGIC | **Reusability** | Share rules across pipelines |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: YAML Configuration Format
# MAGIC
# MAGIC ### 2.1 Create Sample YAML Configuration

# COMMAND ----------

# DBTITLE 1,Cell 6
# Define quality rules in YAML format
quality_rules_yaml = """
version: "1.0"
description: "Data quality rules for transaction processing"
author: "Data Quality Team"
last_updated: "2024-01-20"

tables:
  - name: "transactions"
    description: "Transaction table quality rules"
    checks:
      - name: "transaction_id_not_null"
        type: "not_null"
        column: "transaction_id"
        level: "error"
        reaction: "drop"
        description: "Transaction ID is required"
      
      - name: "customer_id_not_null"
        type: "not_null"
        column: "customer_id"
        level: "error"
        reaction: "quarantine"
        description: "Customer ID must be present"
      
      - name: "amount_range"
        type: "between"
        column: "amount"
        min_value: 0
        max_value: 1000000
        level: "error"
        reaction: "quarantine"
        description: "Amount must be between 0 and 1M"
      
      - name: "valid_status"
        type: "in_set"
        column: "status"
        valid_values:
          - "pending"
          - "processing"
          - "completed"
          - "cancelled"
          - "refunded"
        level: "error"
        reaction: "quarantine"
        description: "Status must be valid"
      
      - name: "valid_payment_method"
        type: "in_set"
        column: "payment_method"
        valid_values:
          - "credit_card"
          - "debit_card"
          - "paypal"
          - "bank_transfer"
        level: "warning"
        reaction: "mark"
        description: "Payment method should be recognized"
      
      - name: "discount_validation"
        type: "expression"
        expression: "discount <= amount * 0.5"
        level: "error"
        reaction: "quarantine"
        description: "Discount cannot exceed 50% of amount"
    
    quarantine:
      table: "dqx_demo.quarantined_transactions"
      partition_by:
        - "date"
        - "status"
      add_metadata: true

  - name: "customers"
    description: "Customer table quality rules"
    checks:
      - name: "customer_id_unique"
        type: "unique"
        column: "customer_id"
        level: "error"
        reaction: "drop"
      
      - name: "customer_id_not_null"
        type: "not_null"
        column: "customer_id"
        level: "error"
        reaction: "drop"
      
      - name: "email_format"
        type: "regex"
        column: "email"
        pattern: "^[\\\\w\\\\.-]+@[\\\\w\\\\.-]+\\\\.\\\\w+$"
        level: "warning"
        reaction: "mark"
        description: "Email should be valid format"
      
      - name: "phone_format"
        type: "regex"
        column: "phone"
        pattern: "^\\\\d{3}-\\\\d{3}-\\\\d{4}$"
        level: "warning"
        reaction: "mark"
        description: "Phone should be XXX-XXX-XXXX format"
      
      - name: "age_range"
        type: "between"
        column: "age"
        min_value: 18
        max_value: 120
        level: "warning"
        description: "Age should be reasonable"
"""

# Write YAML to file
yaml_config_path = "/tmp/quality_rules.yaml"
dbutils.fs.put(yaml_config_path, quality_rules_yaml, overwrite=True)

print(f"✅ YAML configuration created at {yaml_config_path}")
print("\nConfiguration content:")
print(quality_rules_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Parse YAML Configuration

# COMMAND ----------

# DBTITLE 1,Cell 8
# Load and parse YAML
yaml_content = dbutils.fs.head(yaml_config_path)
config = yaml.safe_load(yaml_content)

print("✅ YAML configuration loaded")
print(f"\nVersion: {config['version']}")
print(f"Description: {config['description']}")
print(f"Author: {config['author']}")
print(f"Tables configured: {len(config['tables'])}")

# Display tables
for table in config['tables']:
    print(f"\nTable: {table['name']}")
    print(f"  Checks: {len(table['checks'])}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: Loading Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create Sample Transaction Data

# COMMAND ----------

# Create sample transaction data
transaction_data = [
    (1, "TXN001", "CUST001", 1000.00, 100.00, "completed", "credit_card", "2024-01-15"),
    (2, "TXN002", "CUST002", 500.00, 50.00, "completed", "paypal", "2024-01-16"),
    (3, "TXN003", None, 750.00, 75.00, "completed", "credit_card", "2024-01-17"),     # Issue: null customer
    (4, "TXN004", "CUST004", -200.00, 20.00, "completed", "debit_card", "2024-01-18"), # Issue: negative amount
    (5, "TXN005", "CUST005", 1500.00, 800.00, "completed", "paypal", "2024-01-19"),   # Issue: discount > 50%
    (6, "TXN006", "CUST006", 2000.00, 200.00, "invalid", "credit_card", "2024-01-20"), # Issue: invalid status
    (7, "TXN007", "CUST007", 3000.00, 300.00, "completed", "bitcoin", "2024-01-21"),  # Issue: invalid payment
]

transactions_df = spark.createDataFrame(
    transaction_data,
    ["id", "transaction_id", "customer_id", "amount", "discount", "status", "payment_method", "date"]
)

print("Sample transaction data:")
display(transactions_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Create Validator from Configuration

# COMMAND ----------

# DBTITLE 1,Cell 13
# Create validator from YAML configuration
validator = ConfigValidator.from_config(config)

print("✅ Validator created from configuration")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Apply Configuration-Based Validation

# COMMAND ----------

# DBTITLE 1,Cell 15
# Validate transactions using configuration
result_df, summary = validator.validate(
    df=transactions_df,
    table_name="transactions"
)

print(f"\nValidation Results:")
print(f"  Total Records: {summary.get('total_records', 'N/A')}")
print(f"  Valid Records: {summary.get('valid_records', 'N/A')}")
print(f"  Failed Records: {summary.get('failed_records', 'N/A')}")
print(f"  Pass Rate: {summary.get('pass_rate', 0):.2%}")

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: Environment-Specific Configuration
# MAGIC
# MAGIC ### 4.1 Development Environment Rules

# COMMAND ----------

# DBTITLE 1,Cell 17
# Development environment - more lenient rules
dev_config_yaml = """
version: "1.0"
environment: "development"

tables:
  - name: "transactions"
    checks:
      - name: "transaction_id_not_null"
        type: "not_null"
        column: "transaction_id"
        level: "warning"  # Warning in dev
        reaction: "mark"
      
      - name: "amount_positive"
        type: "greater_than"
        column: "amount"
        threshold: 0
        level: "warning"  # Warning in dev
        reaction: "mark"
      
      - name: "valid_status"
        type: "in_set"
        column: "status"
        valid_values: ["pending", "completed", "cancelled"]
        level: "warning"
        reaction: "mark"
"""

dev_config_path = "/tmp/quality_rules_dev.yaml"
dbutils.fs.put(dev_config_path, dev_config_yaml, overwrite=True)

print("✅ Development configuration created")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Production Environment Rules

# COMMAND ----------

# DBTITLE 1,Cell 19
# Production environment - strict rules
prod_config_yaml = """
version: "1.0"
environment: "production"

tables:
  - name: "transactions"
    checks:
      - name: "transaction_id_not_null"
        type: "not_null"
        column: "transaction_id"
        level: "error"  # Error in prod
        reaction: "drop"
      
      - name: "customer_id_not_null"
        type: "not_null"
        column: "customer_id"
        level: "error"
        reaction: "quarantine"
      
      - name: "amount_positive"
        type: "greater_than"
        column: "amount"
        threshold: 0
        level: "error"  # Error in prod
        reaction: "quarantine"
      
      - name: "valid_status"
        type: "in_set"
        column: "status"
        valid_values: ["pending", "processing", "completed", "cancelled", "refunded"]
        level: "error"
        reaction: "quarantine"
      
      - name: "discount_cap"
        type: "expression"
        expression: "discount <= amount * 0.5"
        level: "error"
        reaction: "quarantine"
    
    quarantine:
      table: "production.quarantined_transactions"
      partition_by: ["date"]
      add_metadata: true
"""

prod_config_path = "/tmp/quality_rules_prod.yaml"
dbutils.fs.put(prod_config_path, prod_config_yaml, overwrite=True)

print("✅ Production configuration created")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Compare Dev vs Prod Validation

# COMMAND ----------

# DBTITLE 1,Cell 21
# Load dev config
dev_config = yaml.safe_load(dbutils.fs.head(dev_config_path))
dev_validator = ConfigValidator.from_config(dev_config)

# Validate with dev rules
dev_result_df, dev_summary = dev_validator.validate(transactions_df, "transactions")

print("Development Environment Results:")
print(f"  Pass Rate: {dev_summary.get('pass_rate', 0):.2%}")
print(f"  Failed Records: {dev_summary.get('failed_records', 0)}")

# COMMAND ----------

# DBTITLE 1,Cell 22
# Load prod config
prod_config = yaml.safe_load(dbutils.fs.head(prod_config_path))
prod_validator = ConfigValidator.from_config(prod_config)

# Validate with prod rules
prod_result_df, prod_summary = prod_validator.validate(transactions_df, "transactions")

print("Production Environment Results:")
print(f"  Pass Rate: {prod_summary.get('pass_rate', 0):.2%}")
print(f"  Failed Records: {prod_summary.get('failed_records', 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: JSON Configuration Format
# MAGIC
# MAGIC ### 5.1 Create JSON Configuration

# COMMAND ----------

# DBTITLE 1,Cell 24
# Define rules in JSON format
quality_rules_json = {
    "version": "1.0",
    "description": "Quality rules in JSON format",
    "tables": [
        {
            "name": "orders",
            "checks": [
                {
                    "name": "order_id_not_null",
                    "type": "not_null",
                    "column": "order_id",
                    "level": "error",
                    "reaction": "drop"
                },
                {
                    "name": "order_amount_range",
                    "type": "between",
                    "column": "amount",
                    "min_value": 0,
                    "max_value": 100000,
                    "level": "error",
                    "reaction": "quarantine"
                },
                {
                    "name": "valid_order_status",
                    "type": "in_set",
                    "column": "status",
                    "valid_values": ["new", "shipped", "delivered"],
                    "level": "warning",
                    "reaction": "mark"
                }
            ]
        }
    ]
}

print("✅ JSON configuration created")
print("\nConfiguration:")
print(json.dumps(quality_rules_json, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Load JSON Configuration

# COMMAND ----------

# DBTITLE 1,Cell 26
# Create validator from JSON
json_validator = ConfigValidator.from_config(quality_rules_json)

print("✅ Validator created from JSON configuration")
print(f"Tables configured: {len(json_validator.tables)}")
print(f"Checks for 'orders' table: {len(json_validator.tables['orders']['checks'])}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 6: Dynamic Rule Updates
# MAGIC
# MAGIC ### 6.1 Update Configuration Without Code Changes

# COMMAND ----------

# DBTITLE 1,Cell 28
# Updated configuration with new rules
updated_yaml = """version: "1.1"
description: "Updated quality rules with additional checks"
last_updated: "2024-01-21"

tables:
  - name: "transactions"
    checks:
      - name: "transaction_id_not_null"
        type: "not_null"
        column: "transaction_id"
        level: "error"
        reaction: "drop"
      
      - name: "customer_id_not_null"
        type: "not_null"
        column: "customer_id"
        level: "error"
        reaction: "quarantine"
      
      - name: "amount_range"
        type: "between"
        column: "amount"
        min_value: 0
        max_value: 1000000
        level: "error"
        reaction: "quarantine"
      
      - name: "transaction_id_format"
        type: "regex"
        column: "transaction_id"
        pattern: "^TXN\\d{3,}$"
        level: "warning"
        reaction: "mark"
        description: "Transaction ID should start with TXN followed by digits"
      
      - name: "reasonable_discount"
        type: "less_than"
        column: "discount"
        threshold: 1000
        level: "warning"
        description: "Discount should be reasonable"
"""

print("✅ Updated configuration created (v1.1)")
print("\nNew rules added:")
print("  1. transaction_id_format - regex validation")
print("  2. reasonable_discount - threshold check")

# COMMAND ----------

# DBTITLE 1,Cell 29
# Load updated configuration
updated_config = yaml.safe_load(updated_yaml)
updated_validator = ConfigValidator.from_config(updated_config)

# Validate with updated rules
updated_result_df, updated_summary = updated_validator.validate(transactions_df, "transactions")

print(f"\nValidation with Updated Rules:")
print(f"  Version: {updated_config['version']}")
print(f"  Total Checks: {len(updated_config['tables'][0]['checks'])}")
print(f"  Pass Rate: {updated_summary.get('pass_rate', 0):.2%}")
print(f"  Failed Records: {updated_summary.get('failed_records', 0)}")

print("\nValidated data with new checks:")
display(updated_result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 7: Configuration Management Best Practices
# MAGIC
# MAGIC ### 7.1 Store Configuration in Delta Tables

# COMMAND ----------

# DBTITLE 1,Cell 31
# Demonstrate configuration table structure
# In production, you would store configurations in a Delta table

config_data = [
    ("transactions", "1.0", "production", quality_rules_yaml, "2024-01-21"),
    ("transactions", "1.0", "development", dev_config_yaml, "2024-01-21"),
]

config_df = spark.createDataFrame(
    config_data,
    ["table_name", "version", "environment", "config_yaml", "updated_date"]
)

print("✅ Configuration table structure demonstrated")
print("\nIn production, this would be saved to: dqx_demo.quality_rule_configs")
print(f"\nConfiguration records: {config_df.count()}")
display(config_df.select("table_name", "version", "environment", "updated_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Load Configuration from Delta Table

# COMMAND ----------

# DBTITLE 1,Cell 33
# Demonstrate loading configuration from table structure
def load_config_from_table(config_df, table_name, environment="production", version="1.0"):
    """Load quality rules configuration from DataFrame"""
    config_row = (
        config_df
        .filter(F.col("table_name") == table_name)
        .filter(F.col("environment") == environment)
        .filter(F.col("version") == version)
        .select("config_yaml")
        .first()
    )
    
    if config_row:
        config_yaml = config_row['config_yaml']
        config = yaml.safe_load(config_yaml)
        return config
    else:
        raise ValueError(f"No configuration found for {table_name} ({environment}, v{version})")

# Load config from DataFrame
loaded_config = load_config_from_table(config_df, "transactions", "production", "1.0")
table_validator = ConfigValidator.from_config(loaded_config)

print("✅ Configuration loaded from table structure")
print(f"Loaded config version: {loaded_config['version']}")
print(f"Checks configured: {len(loaded_config['tables'][0]['checks'])}")
print("\nThis demonstrates how configurations can be centrally managed in Delta tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Version Control Integration

# COMMAND ----------

# MAGIC %md
# MAGIC #### Git Repository Structure
# MAGIC
# MAGIC ```
# MAGIC data-quality-configs/
# MAGIC ├── production/
# MAGIC │   ├── transactions_v1.0.yaml
# MAGIC │   ├── customers_v1.0.yaml
# MAGIC │   └── orders_v1.0.yaml
# MAGIC ├── staging/
# MAGIC │   ├── transactions_v1.1.yaml
# MAGIC │   └── customers_v1.0.yaml
# MAGIC ├── development/
# MAGIC │   ├── transactions_v1.2.yaml
# MAGIC │   └── customers_v1.1.yaml
# MAGIC └── README.md
# MAGIC ```
# MAGIC
# MAGIC #### Best Practices:
# MAGIC - Use semantic versioning (major.minor.patch)
# MAGIC - Document changes in commit messages
# MAGIC - Require PR reviews for production rules
# MAGIC - Tag releases for production deployments
# MAGIC - Maintain CHANGELOG.md for rule changes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **YAML/JSON formats** make rules readable and maintainable
# MAGIC
# MAGIC ✅ **Environment-specific configs** enable different rules per environment
# MAGIC
# MAGIC ✅ **Dynamic loading** allows rule updates without code changes
# MAGIC
# MAGIC ✅ **Version control** tracks rule evolution over time
# MAGIC
# MAGIC ✅ **Delta table storage** enables centralized configuration management
# MAGIC
# MAGIC ✅ **ConfigValidator** simplifies loading and applying configurations
# MAGIC
# MAGIC ✅ **Separation of concerns** improves collaboration and governance

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll explore:
# MAGIC 1. **Building quality dashboards** for monitoring
# MAGIC 2. **Validation summary metrics** and KPIs
# MAGIC 3. **Trend analysis** and alerting
# MAGIC 4. **SQL views** for quality reporting
# MAGIC
# MAGIC **Continue to**: `08_Validation_Summary_and_Quality_Dashboard`
