# Databricks notebook source
# =============================================================================
# COVID-19 Testing Data Transformation - Azure Databricks (PySpark)
# -----------------------------------------------------------------------------
# Purpose : Join raw ECDC testing data with date and country dimension tables,
#           aggregate to weekly grain, and write the processed output back to
#           ADLS Gen2 for downstream loading into Azure SQL Database via ADF.
#
# NOTE    : Credentials below are placeholders. In this project, the Service
#           Principal client ID / secret / tenant ID were configured directly
#           in the notebook for simplicity (no Key Vault used in this version -
#           see README "What's Next" for planned improvement). Do NOT commit
#           real secrets to source control - use environment variables,
#           Databricks secret scopes, or Azure Key Vault instead.
# =============================================================================

# COMMAND ----------

# ---------------------------------------------------------------------------
# Cell 1: Storage Authentication
# ---------------------------------------------------------------------------
# Configure authentication for Azure Data Lake Storage Gen2 using a
# Service Principal (OAuth 2.0 client credentials flow).

storage_account = "covidreportingdatalake1"
client_id = "<YOUR_CLIENT_ID>"          # Service Principal Application (client) ID
client_secret = "<YOUR_CLIENT_SECRET>"  # Service Principal client secret
tenant_id = "<YOUR_TENANT_ID>"          # Azure AD tenant ID

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Cell 2: Load Data & Register Temp Views
# ---------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType, DateType

# 1. Define paths pointing to the raw and lookup containers
dim_date_path = f"abfss://lookup@{storage_account}.dfs.core.windows.net/dim_date/"
dim_country_path = f"abfss://lookup@{storage_account}.dfs.core.windows.net/dim_country/"
raw_testing_path = f"abfss://raw@{storage_account}.dfs.core.windows.net/ecdc/testing/"

# 2. Define schema for dim_date
dim_date_schema = StructType([
    StructField("date_key", IntegerType(), True),
    StructField("the_date", DateType(), True),
    StructField("the_year", IntegerType(), True),
    StructField("the_month", IntegerType(), True),
    StructField("the_day", IntegerType(), True),
    StructField("day_name", StringType(), True),
    StructField("day_of_year", LongType(), True),
    StructField("week_of_month", LongType(), True),
    StructField("week_of_year", LongType(), True),
    StructField("month_name", StringType(), True),
    StructField("year_month", IntegerType(), True),
    StructField("year_week", IntegerType(), True)
])

# 3. Define schema for dim_country
dim_country_schema = StructType([
    StructField("country", StringType(), True),
    StructField("country_code_2_digit", StringType(), True),
    StructField("country_code_3_digit", StringType(), True)
])

# 4. Define schema for raw testing data
raw_testing_schema = StructType([
    StructField("country", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("year_week", StringType(), True),
    StructField("new_cases", LongType(), True),
    StructField("tests_done", LongType(), True),
    StructField("population", LongType(), True),
    StructField("testing_rate", DoubleType(), True),
    StructField("positivity_rate", DoubleType(), True),
    StructField("testing_data_source", StringType(), True)
])

# 5. Read CSVs and register as temporary views for SQL transformation
df_dim_date = spark.read.option("header", "true").schema(dim_date_schema).csv(dim_date_path)
df_dim_date.createOrReplaceTempView("dim_date")

df_dim_country = spark.read.option("header", "true").schema(dim_country_schema).csv(dim_country_path)
df_dim_country.createOrReplaceTempView("dim_country")

df_raw_testing = spark.read.option("header", "true").schema(raw_testing_schema).csv(raw_testing_path)
df_raw_testing.createOrReplaceTempView("raw_testing")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Cell 3: Transform - Join & Aggregate to Weekly Grain
# ---------------------------------------------------------------------------

# 1. Join raw testing data with date and country dimensions, aggregate by week
df_processed_testing = spark.sql("""
    SELECT t.country,
           c.country_code_2_digit,
           c.country_code_3_digit,
           t.year_week,
           MIN(d.the_date) AS week_start_date,
           MAX(d.the_date) AS week_end_date,
           t.new_cases,
           t.tests_done,
           t.population,
           t.testing_rate,
           t.positivity_rate,
           t.testing_data_source
      FROM raw_testing t
      JOIN dim_date d
        ON t.year_week = CONCAT(d.the_year, '-W', LPAD(d.week_of_year, 2, '0'))
      JOIN dim_country c
        ON t.country_code = c.country_code_2_digit
  GROUP BY t.country,
           c.country_code_2_digit,
           c.country_code_3_digit,
           t.year_week,
           t.new_cases,
           t.tests_done,
           t.population,
           t.testing_rate,
           t.positivity_rate,
           t.testing_data_source
""")

# COMMAND ----------

# ---------------------------------------------------------------------------
# Cell 4: Write Processed Output to ADLS Gen2
# ---------------------------------------------------------------------------

# 2. Define output destination path (processed container)
processed_testing_path = f"abfss://processed@{storage_account}.dfs.core.windows.net/ecdc/testing"

# 3. Write output as CSV with headers, ready for ADF Copy Activity into Azure SQL DB
df_processed_testing.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("delimiter", ",") \
    .csv(processed_testing_path)
