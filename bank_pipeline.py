
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


APP_NAME = "BankMarketingETL"

SOURCE_PATH = (
    "hdfs:///user/hadoop/BankProject/raw/"
    "bank-additional-full.csv"
)

DATABASE = "bank_dw"

STAGING_TABLE = "staging_bank"
CUSTOMER_TABLE = "dim_customer"
CONTACT_TABLE = "dim_contact"
CAMPAIGN_TABLE = "dim_campaign"
DATE_TABLE = "dim_date"
ECONOMIC_TABLE = "dim_economic"
FACT_TABLE = "fact_campaign"


spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .enableHiveSupport()
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("\n" + "=" * 70)
print("       BANK MARKETING DATA ENGINEERING PIPELINE")
print("=" * 70)


# ============================================================
# 1. HIVE CONFIGURATION
# ============================================================

print("\n[1] Configuring Hive...")

spark.sql("SET hive.exec.dynamic.partition=true")
spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
spark.sql("SET hive.exec.max.dynamic.partitions=1000")
spark.sql("SET hive.exec.max.dynamic.partitions.pernode=100")
spark.sql("SET hive.vectorized.execution.enabled=true")

print("✓ Hive configuration completed")


# ============================================================
# 2. CREATE DATABASE
# ============================================================

print("\n[2] Creating Hive database...")

spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

print(f"✓ Database '{DATABASE}' is ready")


# ============================================================
# 3. READ SOURCE DATA
# ============================================================

print("\n[3] Reading source data from HDFS...")

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ";")
    .csv(SOURCE_PATH)
)

source_count = raw_df.count()

print(f"✓ Source records: {source_count:,}")
print(f"✓ Source columns: {len(raw_df.columns)}")


# ============================================================
# 4. CLEAN DATA
# ============================================================

print("\n[4] Cleaning source data...")

df = raw_df

columns_with_unknown = [
    "job",
    "marital",
    "education",
    "default",
    "housing",
    "loan",
    "contact",
    "poutcome"
]

for column_name in columns_with_unknown:
    df = df.withColumn(
        column_name,
        F.when(
            F.lower(F.col(column_name)) == "unknown",
            F.lit(None)
        ).otherwise(F.col(column_name))
    )


# Convert target variable:
# yes -> 1
# no  -> 0

df = df.withColumn(
    "y",
    F.when(
        F.lower(F.col("y")) == "yes",
        F.lit(1)
    )
    .when(
        F.lower(F.col("y")) == "no",
        F.lit(0)
    )
    .otherwise(None)
)


# Rename columns containing dots

df = (
    df
    .withColumnRenamed("emp.var.rate", "emp_var_rate")
    .withColumnRenamed("cons.price.idx", "cons_price_idx")
    .withColumnRenamed("cons.conf.idx", "cons_conf_idx")
    .withColumnRenamed("nr.employed", "nr_employed")
)


# Cast numeric columns

df = (
    df
    .withColumn("age", F.col("age").cast("int"))
    .withColumn("duration", F.col("duration").cast("int"))
    .withColumn("campaign", F.col("campaign").cast("int"))
    .withColumn("pdays", F.col("pdays").cast("int"))
    .withColumn("previous", F.col("previous").cast("int"))
    .withColumn("emp_var_rate", F.col("emp_var_rate").cast("double"))
    .withColumn("cons_price_idx", F.col("cons_price_idx").cast("double"))
    .withColumn("cons_conf_idx", F.col("cons_conf_idx").cast("double"))
    .withColumn("euribor3m", F.col("euribor3m").cast("double"))
    .withColumn("nr_employed", F.col("nr_employed").cast("double"))
)

clean_count = df.count()

print(f"✓ Records after cleaning: {clean_count:,}")


# ============================================================
# 5. CREATE STAGING TABLE
# ============================================================

print("\n[5] Creating staging table...")

spark.sql(f"DROP TABLE IF EXISTS {STAGING_TABLE}")

(
    df.write
    .mode("overwrite")
    .format("orc")
    .option("compression", "snappy")
    .saveAsTable(STAGING_TABLE)
)

staging_count = spark.table(STAGING_TABLE).count()

print(f"✓ {STAGING_TABLE} created")
print(f"✓ Staging records: {staging_count:,}")


# ============================================================
# 6. CREATE CUSTOMER DIMENSION
# ============================================================

print("\n[6] Creating dim_customer...")

customer_base = (
    df
    .select(
        "age",
        "job",
        "marital",
        "education",
        F.col("default").alias("default_status"),
        "housing",
        "loan"
    )
    .dropDuplicates()
)

customer_window = Window.orderBy(
    "age",
    "job",
    "marital",
    "education",
    "default_status",
    "housing",
    "loan"
)

dim_customer = (
    customer_base
    .withColumn(
        "customer_key",
        F.row_number().over(customer_window)
    )
    .select(
        "customer_key",
        "age",
        "job",
        "marital",
        "education",
        "default_status",
        "housing",
        "loan"
    )
)

spark.sql(f"DROP TABLE IF EXISTS {CUSTOMER_TABLE}")

(
    dim_customer.write
    .mode("overwrite")
    .format("orc")
    .option("compression", "snappy")
    .saveAsTable(CUSTOMER_TABLE)
)

customer_count = spark.table(CUSTOMER_TABLE).count()

print(f"✓ {CUSTOMER_TABLE} created")
print(f"✓ Rows: {customer_count:,}")


# ============================================================
# 7. CREATE CONTACT DIMENSION
# ============================================================

print("\n[7] Creating dim_contact...")

contact_base = (
    df
    .select("contact")
    .dropDuplicates()
)

contact_window = Window.orderBy("contact")

dim_contact = (
    contact_base
    .withColumn(
        "contact_key",
        F.row_number().over(contact_window)
    )
    .select(
        "contact_key",
        F.col("contact").alias("contact_type")
    )
)

spark.sql(f"DROP TABLE IF EXISTS {CONTACT_TABLE}")

(
    dim_contact.write
    .mode("overwrite")
    .format("orc")
    .option("compression", "snappy")
    .saveAsTable(CONTACT_TABLE)
)

contact_count = spark.table(CONTACT_TABLE).count()

print(f"✓ {CONTACT_TABLE} created")
print(f"✓ Rows: {contact_count:,}")


# ============================================================
# 8. CREATE CAMPAIGN DIMENSION
# ============================================================

print("\n[8] Creating dim_campaign...")

campaign_base = (
    df
    .select(
        "campaign",
        "pdays",
        "previous",
        "poutcome"
    )
    .dropDuplicates()
)

campaign_window = Window.orderBy(
    "campaign",
    "pdays",
    "previous",
    "poutcome"
)

dim_campaign = (
    campaign_base
    .withColumn(
        "campaign_key",
        F.row_number().over(campaign_window)
    )
    .select(
        "campaign_key",
        F.col("campaign").alias("campaign_contacts"),
        "pdays",
        F.col("previous").alias("previous_contacts"),
        F.col("poutcome").alias("previous_outcome")
    )
)

spark.sql(f"DROP TABLE IF EXISTS {CAMPAIGN_TABLE}")

(
    dim_campaign.write
    .mode("overwrite")
    .format("orc")
    .option("compression", "snappy")
    .saveAsTable(CAMPAIGN_TABLE)
)

campaign_count = spark.table(CAMPAIGN_TABLE).count()

print(f"✓ {CAMPAIGN_TABLE} created")
print(f"✓ Rows: {campaign_count:,}")


# ============================================================
# 9. CREATE DATE DIMENSION
# ============================================================

print("\n[9] Creating dim_date...")

date_base = (
    df
    .select(
        "month",
        "day_of_week"
    )
    .dropDuplicates()
)

date_window = Window.orderBy(
    "month",
    "day_of_week"
)

dim_date = (
    date_base
    .withColumn(
        "date_key",
        F.row_number().over(date_window)
    )
    .select(
        "date_key",
        "month",
        "day_of_week"
    )
)

spark.sql(f"DROP TABLE IF EXISTS {DATE_TABLE}")

(
    dim_date.write
    .mode("overwrite")
    .format("orc")
    .option("compression", "snappy")
    .saveAsTable(DATE_TABLE)
)

date_count = spark.table(DATE_TABLE).count()

print(f"✓ {DATE_TABLE} created")
print(f"✓ Rows: {date_count:,}")


# ============================================================
# 10. CREATE ECONOMIC DIMENSION
# ============================================================

print("\n[10] Creating dim_economic...")

economic_base = (
    df
    .select(
        "emp_var_rate",
        "cons_price_idx",
        "cons_conf_idx",
        "euribor3m",
        "nr_employed"
    )
    .dropDuplicates()
)

economic_window = Window.orderBy(
    "emp_var_rate",
    "cons_price_idx",
    "cons_conf_idx",
    "euribor3m",
    "nr_employed"
)

dim_economic = (
    economic_base
    .withColumn(
        "economic_key",
        F.row_number().over(economic_window)
    )
    .select(
        "economic_key",
        "emp_var_rate",
        "cons_price_idx",
        "cons_conf_idx",
        "euribor3m",
        "nr_employed"
    )
)

spark.sql(f"DROP TABLE IF EXISTS {ECONOMIC_TABLE}")

(
    dim_economic.write
    .mode("overwrite")
    .format("orc")
    .option("compression", "snappy")
    .saveAsTable(ECONOMIC_TABLE)
)

economic_count = spark.table(ECONOMIC_TABLE).count()

print(f"✓ {ECONOMIC_TABLE} created")
print(f"✓ Rows: {economic_count:,}")


# ============================================================
# 11. PREPARE DIMENSION LOOKUPS
# ============================================================

print("\n[11] Preparing dimension lookups...")

customer_lookup = dim_customer
contact_lookup = dim_contact
campaign_lookup = dim_campaign
date_lookup = dim_date
economic_lookup = dim_economic

print("✓ Dimension lookups ready")


# ============================================================
# 12. BUILD FACT TABLE
# ============================================================

print("\n[12] Building fact_campaign...")

fact_df = (
    df.alias("s")

    # Customer lookup
    .join(
        customer_lookup.alias("c"),
        (
            (F.col("s.age") == F.col("c.age")) &
            (F.col("s.job").eqNullSafe(F.col("c.job"))) &
            (F.col("s.marital").eqNullSafe(F.col("c.marital"))) &
            (F.col("s.education").eqNullSafe(F.col("c.education"))) &
            (
                F.col("s.default").eqNullSafe(
                    F.col("c.default_status")
                )
            ) &
            (
                F.col("s.housing").eqNullSafe(
                    F.col("c.housing")
                )
            ) &
            (
                F.col("s.loan").eqNullSafe(
                    F.col("c.loan")
                )
            )
        ),
        "left"
    )

    # Contact lookup
    .join(
        contact_lookup.alias("ct"),
        F.col("s.contact").eqNullSafe(
            F.col("ct.contact_type")
        ),
        "left"
    )

    # Campaign lookup
    .join(
        campaign_lookup.alias("ca"),
        (
            (F.col("s.campaign") ==
             F.col("ca.campaign_contacts")) &
            (F.col("s.pdays") ==
             F.col("ca.pdays")) &
            (F.col("s.previous") ==
             F.col("ca.previous_contacts")) &
            (
                F.col("s.poutcome").eqNullSafe(
                    F.col("ca.previous_outcome")
                )
            )
        ),
        "left"
    )

    # Date lookup
    .join(
        date_lookup.alias("d"),
        (
            (F.col("s.month") ==
             F.col("d.month")) &
            (F.col("s.day_of_week") ==
             F.col("d.day_of_week"))
        ),
        "left"
    )

    # Economic lookup
    .join(
        economic_lookup.alias("e"),
        (
            (F.col("s.emp_var_rate") ==
             F.col("e.emp_var_rate")) &
            (F.col("s.cons_price_idx") ==
             F.col("e.cons_price_idx")) &
            (F.col("s.cons_conf_idx") ==
             F.col("e.cons_conf_idx")) &
            (F.col("s.euribor3m") ==
             F.col("e.euribor3m")) &
            (F.col("s.nr_employed") ==
             F.col("e.nr_employed"))
        ),
        "left"
    )

    .select(
        F.col("c.customer_key"),
        F.col("ct.contact_key"),
        F.col("ca.campaign_key"),
        F.col("d.date_key"),
        F.col("e.economic_key"),
        F.col("s.campaign"),
        F.col("s.previous"),
        F.col("s.pdays"),
        F.col("s.duration"),
        F.col("s.y").alias("subscribed"),
        F.col("s.month").alias("month")
    )
)


fact_df = fact_df.cache()

fact_count = fact_df.count()

print(f"✓ Fact records generated: {fact_count:,}")


# ============================================================
# 13. CREATE PARTITIONED + BUCKETED FACT TABLE
# ============================================================

print("\n[13] Creating partitioned and bucketed fact_campaign...")

spark.sql(f"DROP TABLE IF EXISTS {FACT_TABLE}")


(
    fact_df
    .write
    .mode("overwrite")
    .format("orc")
    .option("compression", "snappy")
    .partitionBy("month")
    .bucketBy(8, "customer_key")
    .sortBy("customer_key")
    .saveAsTable(FACT_TABLE)
)

print(f"✓ {FACT_TABLE} created")
print("✓ Partition column: month")
print("✓ Bucket column: customer_key")
print("✓ Number of buckets: 8")
print("✓ Sort column: customer_key")
print("✓ Format: ORC")
print("✓ Compression: Snappy")


# ============================================================
# 14. VALIDATION
# ============================================================

print("\n[14] Running validation checks...")

final_staging_count = (
    spark.table(STAGING_TABLE).count()
)

final_fact_count = (
    spark.table(FACT_TABLE).count()
)


null_fk_count = (
    spark.table(FACT_TABLE)
    .filter(
        F.col("customer_key").isNull() |
        F.col("contact_key").isNull() |
        F.col("campaign_key").isNull() |
        F.col("date_key").isNull() |
        F.col("economic_key").isNull()
    )
    .count()
)


print("\n" + "-" * 70)
print("PIPELINE VALIDATION")
print("-" * 70)

print(f"Source records       : {source_count:,}")
print(f"Clean records        : {clean_count:,}")
print(f"Staging records      : {final_staging_count:,}")
print(f"Customer dimension   : {customer_count:,}")
print(f"Contact dimension    : {contact_count:,}")
print(f"Campaign dimension   : {campaign_count:,}")
print(f"Date dimension       : {date_count:,}")
print(f"Economic dimension   : {economic_count:,}")
print(f"Fact records         : {final_fact_count:,}")


validation_passed = True


if source_count == clean_count:
    print("✓ Source → Clean count check PASSED")
else:
    print("✗ Source → Clean count check FAILED")
    validation_passed = False


if source_count == final_staging_count:
    print("✓ Source → Staging count check PASSED")
else:
    print("✗ Source → Staging count check FAILED")
    validation_passed = False


if source_count == final_fact_count:
    print("✓ Source → Fact count check PASSED")
else:
    print("✗ Source → Fact count check FAILED")
    validation_passed = False


if null_fk_count == 0:
    print("✓ Foreign key NULL check PASSED")
else:
    print(
        f"✗ Foreign key NULL check FAILED: "
        f"{null_fk_count} rows"
    )
    validation_passed = False


# ============================================================
# 15. SHOW TABLES
# ============================================================

print("\n[15] Hive tables:")

spark.sql(
    "SHOW TABLES"
).show(truncate=False)


# ============================================================
# 16. FACT TABLE SAMPLE
# ============================================================

print("\n[16] Fact table sample:")

spark.sql(
    f"""
    SELECT *
    FROM {FACT_TABLE}
    LIMIT 10
    """
).show(truncate=False)


# ============================================================
# 17. SHOW PARTITIONS
# ============================================================

print("\n[17] Fact partitions:")

spark.sql(
    f"SHOW PARTITIONS {FACT_TABLE}"
).show(truncate=False)


# ============================================================
# 18. SHOW TABLE DDL
# ============================================================

print("\n[18] Fact table definition:")

spark.sql(
    f"SHOW CREATE TABLE {FACT_TABLE}"
).show(
    truncate=False
)


# ============================================================
# 19. FINAL RESULT
# ============================================================

if validation_passed:

    print("\n" + "=" * 70)
    print("          PIPELINE COMPLETED SUCCESSFULLY")
    print("=" * 70)

    print(f"""
Database:
    {DATABASE}

Tables:
    {STAGING_TABLE}
    {CUSTOMER_TABLE}
    {CONTACT_TABLE}
    {CAMPAIGN_TABLE}
    {DATE_TABLE}
    {ECONOMIC_TABLE}
    {FACT_TABLE}

Storage:
    ORC
    Snappy compression

Partition:
    month

Bucketing:
    customer_key

Number of buckets:
    8

Sorting:
    customer_key

Source records:
    {source_count:,}

Staging records:
    {final_staging_count:,}

Fact records:
    {final_fact_count:,}

Validation:
    PASSED
""")

    print("=" * 70)

else:

    print("\n" + "=" * 70)
    print("          PIPELINE FAILED VALIDATION")
    print("=" * 70)

    print("""
One or more validation checks failed.

The pipeline created the tables,
but the data quality checks did not pass.
""")

    print("=" * 70)

    spark.stop()

    raise RuntimeError(
        "Bank pipeline failed validation."
    )


fact_df.unpersist()

spark.stop()
