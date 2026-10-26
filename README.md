# Bank Marketing Data Engineering Pipeline

##  Project Overview

This project implements an end-to-end **Data Engineering pipeline** for the Bank Marketing dataset.

The pipeline takes raw banking marketing data, stores it in **HDFS**, processes and cleans it using **Apache Spark**, loads the transformed data into a **Hive Data Warehouse**, and performs analytical queries on the final data warehouse.

The project demonstrates practical Data Engineering concepts including:

- HDFS data storage
- Apache Spark / PySpark
- Data cleaning and transformation
- Hive Data Warehouse
- Star Schema
- Dimension and Fact tables
- ORC storage format
- Snappy compression
- Table partitioning
- Table bucketing
- Data validation
- SQL analytics

---

## Architecture

```text
                Bank Marketing CSV
                        │
                        ▼
              ┌─────────────────┐
              │      HDFS       │
              │  Raw Data Layer │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │     PySpark     │
              │ Cleaning & ETL  │
              └────────┬────────┘
                       │
                       ▼
              ┌─────────────────┐
              │ Hive Staging    │
              │     Table       │
              └────────┬────────┘
                       │
                       ▼
        ┌──────────────────────────────┐
        │       Hive Data Warehouse    │
        │                              │
        │  Customer Dimension          │
        │  Contact Dimension           │
        │  Campaign Dimension          │
        │  Date Dimension              │
        │  Economic Dimension          │
        │           ↓                  │
        │      Campaign Fact           │
        └──────────────┬───────────────┘
                       │
                       ▼
              ┌─────────────────┐
              │    Analytics    │
              │   SQL Queries   │
              └─────────────────┘
```

---

##  Dataset

The project uses the **Bank Marketing** dataset.

The dataset contains information about direct marketing campaigns performed by a Portuguese banking institution.

The source file contains:

- **41,188 records**
- **21 columns**

Important attributes include:

- Age
- Job
- Marital status
- Education
- Housing loan
- Personal loan
- Contact type
- Campaign information
- Previous campaign outcome
- Call duration
- Economic indicators
- Subscription target

The target variable is:

```text
y
```

where:

```text
yes → 1
no  → 0
```

The original dataset uses a semicolon (`;`) as the delimiter.

---

##  Technologies

| Technology | Purpose |
|---|---|
| Ubuntu Linux | Operating system |
| Apache Hadoop | Distributed storage and processing |
| HDFS | Raw and distributed data storage |
| Apache Spark | Data processing and ETL |
| PySpark | Python API for Spark |
| Apache Hive | Data warehouse and SQL analytics |
| Beeline | Hive SQL interface |
| ORC | Optimized table storage |
| Snappy | Data compression |
| Git / GitHub | Version control |

---

#  Project Structure

```text
BankingProject/
│
├── bank_pipeline.py
│
├── README.md
│
├── requirements.txt
│
├── sql/
│   └── analytics_queries.sql
│

```

The raw dataset is intentionally **not included in the GitHub repository**.

---

#  1. Raw Data Storage

The original CSV file is stored in HDFS.

HDFS path:

```text
/user/hadoop/BankProject/raw/bank-additional-full.csv
```

The file is read using:

```python
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("sep", ";") \
    .csv("hdfs:///user/hadoop/BankProject/raw/bank-additional-full.csv")
```

The source contains:

```text
41,188 records
21 columns
```

---

#  2. Data Cleaning

The PySpark pipeline performs several cleaning and transformation operations.

### Handling Unknown Values

Categorical values such as:

```text
unknown
```

are converted to:

```text
NULL
```

where appropriate.

### Target Transformation

The original target:

```text
y
```

is transformed into a numeric field:

```text
subscribed
```

with:

```text
yes → 1
no  → 0
```

### Column Name Cleaning

Columns containing dots are renamed to Hive/Spark-friendly names.

For example:

```text
emp.var.rate     → emp_var_rate
cons.price.idx   → cons_price_idx
cons.conf.idx    → cons_conf_idx
nr.employed      → nr_employed
```

### Data Types

Numeric columns are explicitly cast to appropriate numeric types.

---

#  3. Hive Data Warehouse

The Hive database created for the project is:

```text
bank_dw
```

The warehouse follows a **Star Schema** design.

## Tables

```text
staging_bank
dim_customer
dim_contact
dim_campaign
dim_date
dim_economic
fact_campaign
```

---

#  4. Star Schema

```text
                       dim_customer
                            │
                            │
                            ▼
dim_contact ───────► fact_campaign ◄────── dim_campaign
                            │
                            │
                            ├──────────────► dim_date
                            │
                            │
                            └──────────────► dim_economic
```

The central fact table stores campaign/contact events and references the dimension tables through surrogate keys.

---

#  5. Customer Dimension

Table:

```text
dim_customer
```

This dimension contains customer profile attributes such as:

- Age
- Job
- Marital status
- Education
- Default
- Housing
- Loan

### Important Note

The source dataset does not contain a true customer ID.

Therefore, the `customer_key` represents a distinct **customer profile combination**, rather than a confirmed real-world customer identifier.

---

#  6. Contact Dimension

Table:

```text
dim_contact
```

This dimension stores the type of communication used during the campaign.

Examples include:

```text
cellular
telephone
```

---

#  7. Campaign Dimension

Table:

```text
dim_campaign
```

This dimension represents campaign-history attributes, including previous campaign outcomes and related campaign information.

The surrogate key:

```text
campaign_key
```

is used by the fact table.

---

#  8. Date Dimension

Table:

```text
dim_date
```

The date dimension represents the campaign date-related information.

The dimension contains attributes such as:

- Month
- Day of week

A surrogate:

```text
date_key
```

is used to connect the date dimension to the fact table.

---

#  9. Economic Dimension

Table:

```text
dim_economic
```

This dimension contains economic indicators associated with the campaign records.

Examples include:

- Employment variation rate
- Consumer price index
- Consumer confidence index
- Euribor 3-month rate
- Number of employees

---

#  10. Fact Table

Table:

```text
fact_campaign
```

The fact table contains the measurable campaign events.

Important columns include:

```text
customer_key
contact_key
campaign_key
date_key
economic_key
campaign
previous
pdays
duration
subscribed
month
```

The fact table contains:

```text
41,188 records
```

which matches the source dataset record count.

---

#  11. ORC and Snappy Compression

The warehouse tables use **ORC** for optimized analytical storage.

The fact table uses:

```text
ORC
```

with:

```text
Snappy compression
```

This provides a columnar storage format suitable for analytical queries while reducing storage requirements.

---

#  12. Partitioning

The fact table is partitioned by:

```text
month
```

There are 10 partitions:

```text
apr
aug
dec
jul
jun
mar
may
nov
oct
sep
```

### Why partition by month?

`month` has relatively low cardinality and is frequently useful for analytical queries.

For example:

```sql
SELECT *
FROM fact_campaign
WHERE month = 'may';
```

Hive can use partition pruning and read only the relevant partition instead of scanning all month partitions.

This makes `month` a suitable partition column.

---

#  13. Bucketing

The fact table is also bucketed by:

```text
customer_key
```

using:

```text
8 buckets
```

The table is configured as:

```text
CLUSTERED BY (customer_key)
SORTED BY (customer_key)
INTO 8 BUCKETS
```

Bucketing distributes rows into a fixed number of files based on the hash of the bucket column.

### Partitioning vs Bucketing

Partitioning organizes data using explicit values:

```text
month = jan
month = feb
month = mar
...
```

Bucketing distributes records internally based on a hash:

```text
customer_key
       ↓
   hash function
       ↓
 ┌───┬───┬───┬───┬───┐
 │ B1│ B2│ B3│...│ B8│
 └───┴───┴───┴───┴───┘
```

The project therefore uses:

```text
Partitioning → month
Bucketing    → customer_key
```

---

#  14. Data Validation

The pipeline performs record-count and foreign-key validation.

The final validation results were:

```text
Source records       : 41,188
Clean records        : 41,188
Staging records      : 41,188
Customer dimension   : 13,006
Contact dimension    : 2
Campaign dimension   : 359
Date dimension       : 50
Economic dimension   : 375
Fact records         : 41,188
```

Validation checks:

```text
✓ Source → Clean count check PASSED
✓ Source → Staging count check PASSED
✓ Source → Fact count check PASSED
✓ Foreign key NULL check PASSED
```

This confirms that the ETL process preserved all source records through the staging and fact layers.

---

#  15. Analytics

The project includes SQL analytics queries stored in:

```text
sql/analytics_queries.sql
```

The queries analyze the campaign data from different dimensions.

Examples include:

### Subscription Rate by Job

Analyzes subscription rates across different job categories.

### Subscription Rate by Education

Analyzes subscription rates across education categories.

### Subscription by Contact Type

Compares campaign results across communication types.

### Average Call Duration

Calculates average call duration for different subscription outcomes.

### Subscription Rate by Month

Analyzes campaign performance across different months.

### Previous Campaign Outcome

Examines the relationship between previous campaign outcomes and current subscriptions.

### Campaign Contacts

Analyzes the relationship between the number of campaign contacts and subscriptions.

### Marital Status

Analyzes subscription rates across marital-status categories.

### Housing and Personal Loan

Analyzes subscription rates based on housing and personal loan status.

### Overall Campaign Performance

The final analytics query produced:

```text
Total Contacts              : 41,188
Total Subscriptions         : 4,640
Overall Subscription Rate   : 11.27%
Average Call Duration       : 258.29 seconds
Average Campaign Contacts  : 2.57
```

---

#  16. How to Run the Project

## Step 1 — Start Hadoop

Make sure the Hadoop services are running.

Check using:

```bash
jps
```

---

## Step 2 — Verify the Raw Dataset

Check the HDFS location:

```bash
hdfs dfs -ls /user/hadoop/BankProject/raw/
```

The dataset should be available at:

```text
/user/hadoop/BankProject/raw/bank-additional-full.csv
```

---

## Step 3 — Run the PySpark Pipeline

Run:

```bash
spark-submit /home/hadoop/BankingProject/bank_pipeline.py
```

The pipeline performs:

```text
Read
 ↓
Clean
 ↓
Transform
 ↓
Create staging table
 ↓
Create dimensions
 ↓
Create fact table
 ↓
Partition
 ↓
Bucket
 ↓
Validate
```

---

## Step 4 — Run Hive Analytics

Run:

```bash
beeline -u "jdbc:hive2://localhost:10000/default" -n hadoop \
-f /home/hadoop/BankingProject/sql/analytics_queries.sql
```

---

#  17. Useful Hive Commands

Show all tables:

```sql
SHOW TABLES IN bank_dw;
```

Describe the fact table:

```sql
DESCRIBE bank_dw.fact_campaign;
```

Show the table definition:

```sql
SHOW CREATE TABLE bank_dw.fact_campaign;
```

Show fact partitions:

```sql
SHOW PARTITIONS bank_dw.fact_campaign;
```

Count fact records:

```sql
SELECT COUNT(*)
FROM bank_dw.fact_campaign;
```

---

#  18. Key Results

The completed pipeline successfully processed:

```text
41,188 source records
```

and produced:

```text
41,188 fact records
```

The final warehouse contains:

```text
5 dimension/data supporting tables
+
1 fact table
+
1 staging table
```

The fact table is optimized using:

```text
ORC
Snappy
Partitioning
Bucketing
Sorting
```

The pipeline also successfully passed the implemented data-quality checks.

---

#  19. Project Goals Achieved

The project demonstrates the following Data Engineering concepts:

- [x] HDFS data storage
- [x] PySpark ETL
- [x] Data cleaning
- [x] Data transformation
- [x] Hive database
- [x] Star Schema
- [x] Dimension tables
- [x] Fact table
- [x] Surrogate keys
- [x] ORC storage
- [x] Snappy compression
- [x] Partitioning
- [x] Bucketing
- [x] Sorting
- [x] Data validation
- [x] SQL analytics
- [x] End-to-end pipeline

---

# ‍Author

**Shahd El-azab**
Data Engineering / Big Data Project
