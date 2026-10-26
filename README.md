# Bank Marketing Data Engineering Pipeline

## Project Overview

An end-to-end Big Data Engineering project that processes the Bank Marketing dataset using **Hadoop, HDFS, PySpark, and Apache Hive**.

The pipeline stores raw data in HDFS, performs data cleaning and transformation with PySpark, loads the processed records into a Hive data warehouse, and runs SQL queries to analyze marketing campaign performance.

The project demonstrates practical concepts in ETL, dimensional modeling, analytical storage optimization, and data validation.

## Architecture

```text
Bank Marketing CSV
        |
        v
       HDFS
    Raw Storage
        |
        v
     PySpark
 Cleaning & ETL
        |
        v
 Hive Staging Table
        |
        v
 Hive Data Warehouse
   Star Schema
        |
        +-- Dimension Tables
        |
        +-- Fact Table
        |
        v
   SQL Analytics
```

## Dataset

The project uses the Bank Marketing dataset, which contains information about direct marketing campaigns conducted by a Portuguese banking institution.

- **Records:** 41,188
- **Columns:** 21
- **Target variable:** `y`
- **Source delimiter:** Semicolon (`;`)

The target variable is transformed into a numeric field named `subscribed`:

- `yes` → `1`
- `no` → `0`

The original dataset is not included in this repository. Place the source file in the HDFS location specified below before running the pipeline.

## Technologies

| Technology | Purpose |
|---|---|
| Ubuntu Linux | Execution environment |
| Apache Hadoop | Distributed data infrastructure |
| HDFS | Raw data storage |
| Apache Spark / PySpark | Data cleaning and ETL |
| Apache Hive | Data warehouse and SQL analytics |
| Beeline | Hive SQL access |
| ORC | Columnar storage format |
| Snappy | Data compression |
| SQL | Analytical queries |
| Git / GitHub | Version control |

## Project Structure

```text
BankingProject/
├── bank_pipeline.py
├── README.md
├── .gitignore
└── sql/
    └── analytics_queries.sql
```

## Pipeline Implementation

### 1. Raw Data Ingestion

The pipeline reads the source CSV file from HDFS:

```text
/user/hadoop/BankProject/raw/bank-additional-full.csv
```

The input is loaded into a Spark DataFrame using the header row, inferred data types, and the semicolon delimiter.

### 2. Data Cleaning and Transformation

The PySpark pipeline performs the following operations:

- Handles categorical values marked as `unknown` by converting them to `NULL` where appropriate.
- Converts the target variable `y` into a numeric `subscribed` field.
- Renames columns containing dots to use Hive/Spark-friendly names.
- Casts selected columns to appropriate numeric data types.

### 3. Hive Data Warehouse

The pipeline creates the `bank_dw` database and organizes the processed data into a staging table, dimension tables, and a fact table.

The warehouse contains:

| Table | Purpose |
|---|---|
| `staging_bank` | Stores transformed records before dimensional modeling |
| `dim_customer` | Customer profile attributes |
| `dim_contact` | Communication channel attributes |
| `dim_campaign` | Campaign history attributes |
| `dim_date` | Campaign month and day-of-week attributes |
| `dim_economic` | Economic indicators |
| `fact_campaign` | Campaign records and analytical measures |

### 4. Dimensional Modeling

The warehouse follows a Star Schema design, with `fact_campaign` referencing five dimension tables through surrogate keys.

**Important modeling note:** The source dataset does not provide a true customer identifier. Therefore, `customer_key` identifies a distinct combination of customer profile attributes rather than a verified individual customer.

The `dim_date` table represents the available month and day-of-week attributes; it is not a complete calendar dimension containing every calendar date.

### 5. Storage Optimization

The fact table is configured to use:

- **ORC:** Columnar storage for analytical workloads.
- **Snappy:** Compression.
- **Partitioning by `month`:** Organizes records into month-based partitions.
- **Bucketing by `customer_key`:** Uses eight configured buckets.
- **Sorting by `customer_key`:** Configures the table's sorting property.

These techniques demonstrate common Hive data organization and storage optimization approaches. Actual performance benefits depend on the execution environment, table layout, and query workload.

### 6. Data Validation

The pipeline includes checks for:

- Record-count consistency across source, cleaned, staging, and fact data.
- `NULL` foreign keys in the fact table.

These checks help identify record-count discrepancies and missing key values. They do not constitute a complete data-quality testing framework.

## SQL Analytics

Analytical queries are available in:

`sql/analytics_queries.sql`

The queries explore campaign performance from different perspectives, including:

- Subscription rate by job category.
- Subscription rate by education.
- Subscription rate by contact type.
- Average call duration by subscription outcome.
- Subscription rate by month.
- Previous campaign outcome and current subscription.
- Campaign contact frequency.
- Subscription rates by marital status.
- Housing and personal loan attributes.
- Overall campaign performance.

These analyses help demonstrate how the warehouse can support business-oriented reporting and exploration.

## How to Run

The following commands assume that Hadoop, Spark, Hive, and the HiveServer2 service are configured in the execution environment.

### Step 1: Check Hadoop Services

```bash
jps
```

Confirm that the required Hadoop services are running.

### Step 2: Verify the HDFS Dataset

```bash
hdfs dfs -ls /user/hadoop/BankProject/raw/
```

Ensure that `bank-additional-full.csv` exists at the expected HDFS path.

### Step 3: Run the PySpark Pipeline

```bash
spark-submit /home/hadoop/BankingProject/bank_pipeline.py
```

Update the local script path if your project is stored elsewhere.

### Step 4: Run the Analytics Queries

```bash
beeline -u "jdbc:hive2://localhost:10000/default" -n hadoop \
-f /home/hadoop/BankingProject/sql/analytics_queries.sql
```

Update the SQL file path and connection settings to match your environment.

## Useful Hive Commands

List warehouse tables:

```sql
SHOW TABLES IN bank_dw;
```

Inspect the fact table schema:

```sql
DESCRIBE bank_dw.fact_campaign;
```

Inspect its table definition:

```sql
SHOW CREATE TABLE bank_dw.fact_campaign;
```

List fact table partitions:

```sql
SHOW PARTITIONS bank_dw.fact_campaign;
```

Count fact records:

```sql
SELECT COUNT(*)
FROM bank_dw.fact_campaign;
```

## Key Learning Outcomes

This project provides hands-on practice with:

- Building an ETL pipeline using PySpark.
- Reading data from HDFS.
- Cleaning and transforming structured data.
- Designing a Star Schema in Hive.
- Creating fact and dimension tables.
- Working with surrogate keys.
- Configuring ORC storage and Snappy compression.
- Applying Hive partitioning and bucketing.
- Implementing basic record-count and key validation.
- Writing SQL queries for business analytics.

## Author

**Shahd El-azab**

Big Data Engineering Project
