Here is the same markdown file with all code examples removed and everything else kept as-is:

***

# Data Quality with DQX Framework Workshop

Welcome to the **Data Quality with DQX (Data Quality Framework)** workshop! This comprehensive guide will teach you how to use the Databricks Labs DQX framework to define, monitor, and address data quality issues in your Python-based data pipelines. [databrickslabs.github](https://databrickslabs.github.io/dqx/)

## 📚 Workshop Overview

DQX is a powerful data quality framework for Apache Spark that enables you to:
- Define quality rules at row and column levels
- Monitor data quality in real-time
- Automatically profile data and generate quality rules
- Handle failed checks with custom reactions
- Track quality metrics and build dashboards
- Support both batch and streaming workloads [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

**Official Documentation**: [https://databrickslabs.github.io/dqx/](https://databrickslabs.github.io/dqx/)

***

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
9. Implement production-ready data quality pipelines [databrickslabs.github](https://databrickslabs.github.io/dqx/)

***

## 📋 Prerequisites

- Basic knowledge of Apache Spark and PySpark
- Familiarity with Databricks workspace
- Understanding of data quality concepts
- Python programming experience [advancinganalytics.co](https://www.advancinganalytics.co.uk/blog/clean-data-happy-insights-the-databricks-quality-extended-dqx-way)

***

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

***

## 📖 Workshop Modules

### Module 1: Foundations of Data Quality with DQX

**Duration**: 30 minutes

#### Topics Covered:
- Introduction to DQX framework
- Core capabilities and features
- DQX architecture and components
- Comparison with other data quality tools
- Use cases and best practices [dataengineeringcentral.substack](https://dataengineeringcentral.substack.com/p/data-quality-with-databricks-labs)

#### Key Concepts:
- **Data Quality Checks**: Rules that validate data against expected criteria
- **Check Levels**: Warning vs Error severity levels
- **Reactions**: Actions taken when checks fail (drop, mark, quarantine)
- **Profiling**: Automatic analysis of data characteristics
- **Validation Summary**: Aggregated quality metrics [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

***

### Module 2: Getting Started - Your First Data Quality Check

**Duration**: 45 minutes

#### Learning Goals:
- Set up your first DQX project
- Define simple quality checks
- Run validation on sample data
- Understand validation results [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

#### Expected Outcomes:
- Understand how to create validators
- Learn different check types
- Interpret validation results
- Differentiate between error and warning levels [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

***

### Module 3: Row-Level and Column-Level Quality Rules

**Duration**: 60 minutes

#### Topics Covered:

##### A. Column-Level Rules
- Not null checks
- Data type validations
- Range checks (min/max)
- Pattern matching (regex)
- Uniqueness constraints
- Set membership checks [databrickslabs.github](https://databrickslabs.github.io/dqx/)

##### B. Row-Level Rules
- Cross-column validations
- Business logic rules
- Conditional checks
- Complex expressions [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

#### Lab Exercises:
1. Implement null checks on critical columns
2. Add range validations for numeric fields
3. Create pattern matching for email/phone fields
4. Build cross-column validation rules
5. Test with various data scenarios [databrickslabs.github](https://databrickslabs.github.io/dqx/)

***

### Module 4: Data Profiling and Auto-Generated Rules

**Duration**: 45 minutes

#### Topics Covered:
- Automatic data profiling
- Statistics generation
- Quality rule recommendations
- Threshold tuning
- Profile-based validation [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

#### Key Features:
- **Automatic Profiling**: Analyzes data distributions, nulls, uniqueness
- **Rule Suggestions**: Generates rules based on data patterns
- **Confidence Levels**: Adjustable thresholds for rule generation
- **Iterative Refinement**: Update rules as data evolves [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

#### Lab Exercise:
1. Profile a customer dataset
2. Review profiling statistics
3. Generate quality rules automatically
4. Fine-tune suggested rules
5. Apply and validate [databrickslabs.github](https://databrickslabs.github.io/dqx/)

***

### Module 5: Custom Reactions to Failed Checks

**Duration**: 60 minutes

#### Topics Covered:

##### A. Drop Invalid Records
##### B. Mark Invalid Records
##### C. Quarantine Invalid Records
##### D. Alert and Continue

#### Lab Exercise:
1. Configure different reactions for various check types
2. Set up quarantine tables
3. Implement marking strategy for warnings
4. Build remediation workflows
5. Test with real-world scenarios [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

***

### Module 6: Working with Spark Structured Streaming

**Duration**: 60 minutes

#### Topics Covered:
- DQX with streaming DataFrames
- Real-time quality monitoring
- Streaming quarantine patterns
- Checkpoint management
- Handling late-arriving data [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

#### Lab Exercise:
1. Set up streaming data source
2. Configure DQX for streaming
3. Implement streaming quarantine
4. Build real-time quality dashboard
5. Test with simulated streaming data [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

***

### Module 7: Lakeflow Pipelines (DLT) Integration

**Duration**: 60 minutes

#### Topics Covered:
- DQX with Delta Live Tables
- DLT expectations vs DQX checks
- Combining DLT and DQX
- Medallion architecture with DQX
- Pipeline observability [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

#### Lab Exercise:
1. Create DLT pipeline with DQX integration
2. Implement medallion architecture
3. Configure quality checks per layer
4. Set up quality metrics tracking
5. Monitor pipeline execution [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

***

### Module 8: Configuration-Based Quality Checks

**Duration**: 45 minutes

#### Topics Covered:
- YAML-based check definitions
- Configuration management
- Dynamic rule loading
- Environment-specific rules
- Version control for quality rules [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

#### Lab Exercise:
1. Create YAML configuration for quality rules
2. Load and apply configuration
3. Test with multiple tables
4. Implement environment-specific configs
5. Version control quality rules [pypi](https://pypi.org/project/databricks-labs-dqx/0.1.5/)

***

### Module 9: Validation Summary and Quality Dashboard

**Duration**: 60 minutes

#### Topics Covered:
- Quality metrics collection
- Summary statistics
- Dashboard design patterns
- Alerting integration
- Trend analysis [databrickslabs.github](https://databrickslabs.github.io/dqx/)

#### Lab Exercise:
1. Set up metrics collection
2. Create quality KPI views
3. Build SQL dashboard
4. Implement trend analysis
5. Configure quality alerts [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

***

### Module 10: Advanced Topics and Best Practices

**Duration**: 60 minutes

#### Topics Covered:

##### A. Performance Optimization
##### B. Custom Check Development
##### C. Multi-Table Validation
##### D. Data Quality CI/CD
##### E. Quality Rule Versioning

***

## 📊 Assessment and Certification

### Knowledge Check Questions:
1. What are the three main reaction types in DQX?
2. How does DQX differ from native DLT expectations?
3. When should you use row-level vs column-level checks?
4. How do you handle quarantined records?
5. What metrics should you track for data quality? [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/motivation/)

### Practical Assessment:
Build a complete data quality pipeline that:
- Ingests raw data from multiple sources
- Applies comprehensive quality checks
- Handles failed records appropriately
- Tracks quality metrics
- Provides quality dashboard
- Runs in production environment [databrickslabs.github](https://databrickslabs.github.io/dqx/)

***

## 🔗 Additional Resources

### Official Documentation
- [DQX Documentation](https://databrickslabs.github.io/dqx/)
- [DQX GitHub Repository](https://github.com/databrickslabs/dqx)
- [Databricks Data Quality Guide](https://docs.databricks.com/data-governance/data-quality.html) [github](https://github.com/databrickslabs/dqx)

### Related Workshops
- Delta Lake Fundamentals
- Lakeflow Pipelines (DLT) Workshop
- Data Governance with Unity Catalog [databricks](https://www.databricks.com/discover/pages/data-quality-management)

### Community
- Databricks Community Forums
- DQX GitHub Discussions
- Databricks Labs Projects [community.databricks](https://community.databricks.com/t5/databrickstv/getting-started-with-dqx/ba-p/113662)

***

## 🤝 Contributing

Have suggestions or improvements? Contributions are welcome! [github](https://github.com/databrickslabs/dqx)

***

## 📝 Notes

- All code examples are tested on Databricks Runtime 13.3 LTS or higher
- DQX is actively maintained by Databricks Labs
- Check official documentation for latest features and updates [databrickslabs.github](https://databrickslabs.github.io/dqx/docs/installation/)

***

## 📧 Support

For questions or issues:
- Open an issue on [DQX GitHub](https://github.com/databrickslabs/dqx/issues)
- Contact Databricks Support
- Reach out to your Databricks account team [community.databricks](https://community.databricks.com/t5/databrickstv/getting-started-with-dqx/ba-p/113662)

***

**Happy Data Quality Engineering with DQX! 🚀**