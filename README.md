# 🧠 PySpark Data Engineering

Repository containing scripts and notebooks used for data management using PySpark and SQL in distributed environments. This repository includes best practices, reusable snippets, and solutions to common problems when handling large volumes of data.

## 🚀 Common features and tasks included

### 🔄 Data Ingestion and Transformation
- 📥 Reading data in formats such as Parquet, CSV, Delta, JSON from ADLS or external systems.
- 🔗 Joining and cleansing data from SAP
- 🧽 Normalization and validation of multi-table structures before persistence.
- 🕹️ Automation of ingestion processes by date, with widgets or external parameters
### 🧮 Processing with PySpark
- 📊 Calculating aggregations (by day, month, country, etc.) using `groupBy` and `agg`.
- 🔍 Comparing datasets (e.g., UDL vs. MDL differences).
- 🧬 Calculating data quality metrics (nulls, duplicates, skew).
- 🧠 Using custom functions (`udf`, `withColumn`, `lit`, `when`).
- 🧰 Applying dynamic filters and complex joins with multiple conditions.

### 🧾 SQL on Spark
- 📝 Running SQL queries on DataFrames or tables registered in the metastore (`%sql` in Databricks).
- ⚖️ Data comparison with `EXCEPT`, `UNION ALL`, `JOIN`, and `CASE WHEN`.
- 🚀 Query optimization with temporal views and intermediate persistence.

### 🧰 Format and partition management
- 🗂️ Writing data to partitioned Delta tables (`partitionBy("COUNTRY", "FILE_DATE")`).
- 🔁 Migration of Parquet tables to Delta Lake to improve consistency.
- 🧾 Reading `_delta_log` and version control.

### 🔁 Scheduled processes and monitoring
- ✅ Generation of daily and monthly integrity validations between layers.
- ⚠️ Automatic detection of deviations of more than 5% in key metrics.
- ⏱️ Control executions by timestamp (`EXECUTION_TIMESTAMP`).
- 📧 Sending email alerts via Databricks Workflow or Azure Data Factory.

## 📦 Popular codes and patterns included

- 🔗 `unionByName` to combine multiple DataFrames.
- 🧩 `MERGE INTO` merges for efficient Delta table updates.
- ❓ `isEmpty()` and `count()` as pre-write validation.
- 🧱 Using `create_schema`, `truncate_table`, and `get_latest_hour` functions.

- ⚙️ Code modularization with functions such as:
- `ret_mdl(granularity)`
- `ret_udl(granularity)`
- `reading_source_date(start, end)`
- `loading_temp(df, path, table_type)`
