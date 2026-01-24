# Databricks notebook source
# MAGIC %md
# MAGIC # Foundations of Data Quality with DQX
# MAGIC
# MAGIC **Module 1: Introduction to DQX Framework**
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
# MAGIC 1. **What is DQX** - Databricks Labs data quality framework for Apache Spark
# MAGIC 2. **Core Capabilities** - Key features and benefits of DQX
# MAGIC 3. **DQX Architecture** - How DQX components work together
# MAGIC 4. **Comparison with Other Tools** - How DQX compares to alternatives
# MAGIC 5. **Use Cases** - When and why to use DQX

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is DQX?
# MAGIC
# MAGIC **DQX (Data Quality Framework)** is an open-source data quality framework from Databricks Labs for Apache Spark that enables you to:
# MAGIC
# MAGIC - ✅ Define data quality checks using code or configuration
# MAGIC - ✅ Monitor data quality in real-time
# MAGIC - ✅ Automatically profile data and generate quality rules
# MAGIC - ✅ Handle failed checks with custom reactions
# MAGIC - ✅ Track quality metrics and build dashboards
# MAGIC - ✅ Support both batch and streaming workloads
# MAGIC
# MAGIC **Official Documentation**: [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)
# MAGIC
# MAGIC **GitHub Repository**: [https://github.com/databrickslabs/dqx](https://github.com/databrickslabs/dqx)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Capabilities
# MAGIC
# MAGIC ### 1. 📊 Detailed Information on Failed Checks
# MAGIC Get comprehensive insights into why a check has failed, including:
# MAGIC - Which records failed
# MAGIC - Which columns have issues
# MAGIC - Specific validation rules that were violated
# MAGIC
# MAGIC ### 2. 🔄 Data Format Agnostic
# MAGIC Works seamlessly with:
# MAGIC - PySpark DataFrames
# MAGIC - Delta Lake tables
# MAGIC - Any Spark-compatible data source
# MAGIC
# MAGIC ### 3. ⚡ Batch & Streaming Support
# MAGIC - Spark Batch processing
# MAGIC - Spark Structured Streaming
# MAGIC - Lakeflow Pipelines (DLT) integration
# MAGIC
# MAGIC ### 4. 🎯 Custom Reactions to Failed Checks
# MAGIC Flexible handling of invalid data:
# MAGIC - **Drop**: Remove invalid records
# MAGIC - **Mark**: Flag bad records with quality indicators
# MAGIC - **Quarantine**: Move failed records to separate storage
# MAGIC
# MAGIC ### 5. ⚠️ Check Levels
# MAGIC - **Warning**: Log issues but continue processing
# MAGIC - **Error**: Critical failures that require action
# MAGIC
# MAGIC ### 6. 📏 Row & Column Level Rules
# MAGIC - Column-level: Validate individual columns (nulls, ranges, patterns)
# MAGIC - Row-level: Validate business logic across columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Concepts
# MAGIC
# MAGIC ### Data Quality Checks
# MAGIC **Definition**: Rules that validate data against expected criteria
# MAGIC
# MAGIC **Examples**:
# MAGIC - NOT NULL checks
# MAGIC - Range validations (min/max)
# MAGIC - Pattern matching (regex)
# MAGIC - Uniqueness constraints
# MAGIC - Business logic rules
# MAGIC
# MAGIC ### Check Levels
# MAGIC | Level | Severity | Action |
# MAGIC |-------|----------|--------|
# MAGIC | Warning | Low | Log and continue |
# MAGIC | Error | High | Fail pipeline or quarantine |
# MAGIC
# MAGIC ### Reactions
# MAGIC Actions taken when checks fail:
# MAGIC
# MAGIC | Reaction | Description | Use Case |
# MAGIC |----------|-------------|----------|
# MAGIC | DROP | Remove invalid records | Critical fields |
# MAGIC | MARK | Add quality flag column | Warnings |
# MAGIC | QUARANTINE | Move to separate table | Review/remediation |
# MAGIC
# MAGIC ### Profiling
# MAGIC Automatic analysis of data characteristics:
# MAGIC - Column statistics (min, max, avg, stddev)
# MAGIC - Null percentages
# MAGIC - Uniqueness metrics
# MAGIC - Data distributions
# MAGIC - Pattern detection
# MAGIC
# MAGIC ### Validation Summary
# MAGIC Aggregated quality metrics:
# MAGIC - Total records processed
# MAGIC - Valid/invalid record counts
# MAGIC - Pass rate percentage
# MAGIC - Failed checks summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQX Architecture
# MAGIC
# MAGIC ### High-Level Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                     Data Sources                             │
# MAGIC │  (Delta Tables, Streaming, Files, External Sources)          │
# MAGIC └─────────────────────┬───────────────────────────────────────┘
# MAGIC                       │
# MAGIC                       ▼
# MAGIC ┌─────────────────────────────────────────────────────────────┐
# MAGIC │                    DQX Framework                             │
# MAGIC │  ┌───────────────┐  ┌──────────────┐  ┌─────────────────┐  │
# MAGIC │  │   Validator   │  │   Profiler   │  │ RuleGenerator   │  │
# MAGIC │  └───────────────┘  └──────────────┘  └─────────────────┘  │
# MAGIC │  ┌───────────────┐  ┌──────────────┐  ┌─────────────────┐  │
# MAGIC │  │ ConfigLoader  │  │   Metrics    │  │   Reactions     │  │
# MAGIC │  └───────────────┘  └──────────────┘  └─────────────────┘  │
# MAGIC └─────────────────────┬───────────────────────────────────────┘
# MAGIC                       │
# MAGIC          ┌────────────┼────────────┐
# MAGIC          ▼            ▼            ▼
# MAGIC   ┌─────────┐  ┌──────────┐  ┌──────────────┐
# MAGIC   │  Clean  │  │Quarantine│  │   Metrics    │
# MAGIC   │  Data   │  │  Table   │  │   Dashboard  │
# MAGIC   └─────────┘  └──────────┘  └──────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Core Components
# MAGIC
# MAGIC 1. **Validator**: Main component for defining and running quality checks
# MAGIC 2. **Profiler**: Analyzes data and generates statistics
# MAGIC 3. **RuleGenerator**: Auto-generates quality rules from profiles
# MAGIC 4. **ConfigLoader**: Loads quality rules from YAML/JSON configuration
# MAGIC 5. **MetricsCollector**: Tracks validation results and quality metrics
# MAGIC 6. **Reaction Handlers**: Execute actions on failed checks (drop, mark, quarantine)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQX vs Other Data Quality Tools
# MAGIC
# MAGIC ### DQX vs Great Expectations
# MAGIC
# MAGIC | Feature | DQX | Great Expectations |
# MAGIC |---------|-----|-------------------|
# MAGIC | **Spark Native** | ✅ Yes | ⚠️ Partial |
# MAGIC | **Streaming Support** | ✅ Yes | ❌ No |
# MAGIC | **DLT Integration** | ✅ Native | ❌ No |
# MAGIC | **Auto Profiling** | ✅ Yes | ✅ Yes |
# MAGIC | **Quarantine** | ✅ Built-in | ⚠️ Manual |
# MAGIC | **Performance** | ⚡ Fast (Spark) | 🐢 Slower (Pandas) |
# MAGIC
# MAGIC ### DQX vs DLT Expectations
# MAGIC
# MAGIC | Feature | DQX | DLT Expectations |
# MAGIC |---------|-----|------------------|
# MAGIC | **Flexibility** | ✅ High | ⚠️ Limited |
# MAGIC | **Batch & Stream** | ✅ Both | ✅ Both |
# MAGIC | **Config-based** | ✅ Yes | ❌ Code only |
# MAGIC | **Custom Reactions** | ✅ Rich | ⚠️ Basic |
# MAGIC | **Profiling** | ✅ Yes | ❌ No |
# MAGIC | **Multi-table** | ✅ Yes | ⚠️ Limited |
# MAGIC | **Can Combine** | ✅ Yes | ✅ Yes |
# MAGIC
# MAGIC **Best Practice**: Use DQX and DLT Expectations together for comprehensive quality!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use Cases for DQX
# MAGIC
# MAGIC ### 1. 🏭 Data Pipeline Quality Gates
# MAGIC - Validate data at each stage of medallion architecture
# MAGIC - Bronze: Basic schema and format checks
# MAGIC - Silver: Business logic validation
# MAGIC - Gold: Aggregate consistency checks
# MAGIC
# MAGIC ### 2. 🔄 Real-Time Data Quality Monitoring
# MAGIC - Streaming data validation
# MAGIC - Immediate quarantine of bad records
# MAGIC - Real-time quality dashboards
# MAGIC - Alerting on quality degradation
# MAGIC
# MAGIC ### 3. 📊 Data Profiling and Discovery
# MAGIC - Understand new data sources
# MAGIC - Generate baseline quality metrics
# MAGIC - Identify data quality issues
# MAGIC - Auto-generate validation rules
# MAGIC
# MAGIC ### 4. 🔍 Data Quality Remediation
# MAGIC - Quarantine bad records for review
# MAGIC - Track remediation progress
# MAGIC - Re-validate fixed records
# MAGIC - Maintain audit trail
# MAGIC
# MAGIC ### 5. 📈 Quality Metrics and Reporting
# MAGIC - Track quality trends over time
# MAGIC - Generate quality dashboards
# MAGIC - SLA monitoring
# MAGIC - Compliance reporting
# MAGIC
# MAGIC ### 6. 🚀 Production Data Pipelines
# MAGIC - Automated quality checks in CI/CD
# MAGIC - Environment-specific rules
# MAGIC - Version-controlled quality standards
# MAGIC - Production monitoring and alerting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Best Practices
# MAGIC
# MAGIC ### 1. Start Simple, Iterate
# MAGIC - Begin with critical column checks (NOT NULL, uniqueness)
# MAGIC - Add business logic rules gradually
# MAGIC - Use profiling to discover issues
# MAGIC
# MAGIC ### 2. Layer Your Checks
# MAGIC - Bronze: Format and schema validation
# MAGIC - Silver: Business logic and consistency
# MAGIC - Gold: Aggregate and cross-table checks
# MAGIC
# MAGIC ### 3. Choose Appropriate Reactions
# MAGIC - **DROP**: Only for truly invalid data
# MAGIC - **MARK**: For warnings that need review
# MAGIC - **QUARANTINE**: For remediation workflows
# MAGIC
# MAGIC ### 4. Monitor Quality Metrics
# MAGIC - Track pass rates over time
# MAGIC - Alert on degradation
# MAGIC - Review quarantined records regularly
# MAGIC
# MAGIC ### 5. Use Configuration for Rules
# MAGIC - Store rules in YAML/JSON
# MAGIC - Version control your rules
# MAGIC - Environment-specific configurations
# MAGIC
# MAGIC ### 6. Combine with DLT
# MAGIC - Use DLT expectations for simple checks
# MAGIC - Use DQX for complex validation
# MAGIC - Leverage both for comprehensive quality

# COMMAND ----------

# MAGIC %md
# MAGIC ## When to Use DQX
# MAGIC
# MAGIC ### ✅ Great Fit
# MAGIC - Building data quality into Spark pipelines
# MAGIC - Need both batch and streaming validation
# MAGIC - Want automated profiling and rule generation
# MAGIC - Need flexible reaction strategies
# MAGIC - Using Lakeflow Pipelines (DLT)
# MAGIC - Want configuration-based quality rules
# MAGIC
# MAGIC ### ⚠️ Consider Alternatives
# MAGIC - Non-Spark environments (use Great Expectations)
# MAGIC - Simple DLT-only pipelines (use DLT expectations alone)
# MAGIC - Data catalog/lineage focus (use Unity Catalog features)

# COMMAND ----------

# MAGIC %md
# MAGIC ## DQX Installation
# MAGIC
# MAGIC We'll cover installation in detail in the next module, but here's a preview:
# MAGIC
# MAGIC ```python
# MAGIC # Install from PyPI
# MAGIC %pip install databricks-labs-dqx
# MAGIC
# MAGIC # Restart Python kernel
# MAGIC dbutils.library.restartPython()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC ✅ **DQX is a powerful Spark-native data quality framework** from Databricks Labs
# MAGIC
# MAGIC ✅ **Supports both batch and streaming** workloads with DLT integration
# MAGIC
# MAGIC ✅ **Provides flexible reactions** to handle data quality issues (drop, mark, quarantine)
# MAGIC
# MAGIC ✅ **Auto-generates quality rules** from data profiling
# MAGIC
# MAGIC ✅ **Configuration-driven** with YAML/JSON support
# MAGIC
# MAGIC ✅ **Best used with DLT expectations** for comprehensive quality
# MAGIC
# MAGIC ✅ **Ideal for production pipelines** with quality monitoring and alerting

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC In the next module, we'll:
# MAGIC 1. Install DQX in your environment
# MAGIC 2. Create your first data quality check
# MAGIC 3. Run validation on sample data
# MAGIC 4. Understand validation results
# MAGIC
# MAGIC **Continue to**: `01_Getting_Started_First_Quality_Check`
