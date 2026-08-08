# Databricks notebook source
# Cell 1: Storage Authentication
storage_account = "covidreportingdatalake1"
client_id = "0845823e-f004-489b-be05-3c9d218f3e6c"
client_secret = "OKB8Q~4boc2GVF2s4CXf4Q.f54CVo3KFStDANbkC"
tenant_id = "6352c3c6-6251-4569-b9d8-56466f10eba3"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform Population By Age data by performing the transformations below
# MAGIC ####-----------------------------------------------------------------------
# MAGIC 1. Split the country code & age group
# MAGIC 2. Exclude all data other than 2019
# MAGIC 3. Remove non numeric data from percentage
# MAGIC 4. Pivot the data by age group
# MAGIC 5. Join to dim_country to get the country, 3 digit country code and the total population.
# MAGIC
# MAGIC ####-----------------------------------------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC ### Replace **storage account name** with your storage account name before executing. 

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the population data & create a temp view

# COMMAND ----------

raw_population_path = "abfss://raw@covidreportingdatalake1.dfs.core.windows.net/population"

df_raw_population = spark.read.csv(raw_population_path, sep=r'\t', header=True)

df_raw_population = df_raw_population.withColumn('age_group', regexp_replace(split(df_raw_population['indic_de,geo\\time'], ',')[0], 'PC_', '')).withColumn('country_code', split(df_raw_population['indic_de,geo\\time'], ',')[1])

df_raw_population = df_raw_population.select(col("country_code").alias("country_code"),
                                             col("age_group").alias("age_group"),
                                             col("2019 ").alias("percentage_2019"))

df_raw_population.createOrReplaceTempView("raw_population")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pivot the data by age group

# COMMAND ----------

# Create a data frame with pivoted percentages
df_raw_population_pivot = spark.sql("SELECT country_code, age_group, cast(regexp_replace(percentage_2019, '[a-z]', '') AS decimal(4,2)) AS percentage_2019 FROM raw_population WHERE length(country_code) = 2").groupBy("country_code").pivot("age_group").sum("percentage_2019").orderBy("country_code")
df_raw_population_pivot.createOrReplaceTempView("raw_population_pivot")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read the country lookup

# COMMAND ----------

# 1. Define direct ABFS paths
lookup_country_path = "abfss://lookup@covidreportingdatalake1.dfs.core.windows.net/dim_country"
processed_population_path = "abfss://processed@covidreportingdatalake1.dfs.core.windows.net/population"

# 2. Read country lookup directly from storage
df_dim_country = spark.read.csv(lookup_country_path, sep=r',', header=True)
df_dim_country.createOrReplaceTempView("dim_country")

# 3. Join pivoted population data with country lookup
df_processed_population = spark.sql("""
    SELECT c.country,
           c.country_code_2_digit,
           c.country_code_3_digit,
           c.population,
           p.Y0_14  AS age_group_0_14,
           p.Y15_24 AS age_group_15_24,
           p.Y25_49 AS age_group_25_49,
           p.Y50_64 AS age_group_50_64, 
           p.Y65_79 AS age_group_65_79,
           p.Y80_MAX AS age_group_80_max
      FROM raw_population_pivot p
      JOIN dim_country c ON p.country_code = country_code_2_digit
     ORDER BY country
""")

# 4. Write processed output directly to the processed container
df_processed_population.write.mode("overwrite").option("header", "true").csv(processed_population_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join population data with country lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write output to the processed mount point

# COMMAND ----------

df_processed_population.write.format("com.databricks.spark.csv").option("header","true").option("delimiter", ",").mode("overwrite").save("abfss://processed@covidreportingdatalake1.dfs.core.windows.net/population")

# COMMAND ----------

