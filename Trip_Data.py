# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to ADLS/DBFS. 
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.
# MAGIC 
# MAGIC 1. create mounting point for adls path in databricks
# MAGIC 2. initialisation of adls credentials so that cluster can access data from adls
# MAGIC 3. Load and create look up temp table
# MAGIC 4. Load and clean fhv data and write to adls as external hive tables
# MAGIC 5. work on a simple  use case based on fhv data
# MAGIC 6. Load and clean yellow data and write to adls as external hive tables
# MAGIC 7. work on a simple  use case based on fhv data
# MAGIC 8. Create simple use cases for green and fhvhv data

# COMMAND ----------

### mount the adl directory
import logging
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("py4j").setLevel(logging.ERROR)

spn_client_id = '********'
spn_directory_id = '*************'
MOUNT_ENV = '*****'

spn_secret = dbutils.secrets.get(scope="*****",key="*******")

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "*********",
           "fs.azure.account.oauth2.client.id": spn_client_id,
           "fs.azure.account.oauth2.client.secret": spn_secret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{spn_directory_id}/oauth2/token"}


adl_stage_name = "******"

# Optionally, you can add <directory-name> to the source URI of your mount point.
for one_mount in ["data"]:
    mount_point = f"/mnt/{MOUNT_ENV}/{one_mount}"
    source = f"abfss://{one_mount}@{adl_stage_name}.dfs.core.windows.net/"
    logging.info(f"Mounting {mount_point} at {source}")
    try:
        dbutils.fs.mount(
          mount_point=mount_point,
          source=source,
          extra_configs=configs)
    except Exception as e:
        if 'Directory already mounted' not in str(e):
            logging.exception("Mounting failed with unknown exception", exc_info=True)
        else:
            logging.info("Container already mounted")

# COMMAND ----------

## Adding ADLS Gen2 connection information: devmbxadl
	"spark.hadoop.fs.azure.account.auth.type.devmbxadl.dfs.core.windows.net" = "OAuth"
	"spark.hadoop.fs.azure.account.oauth.provider.type.devmbxadl.dfs.core.windows.net" = "********"
	"spark.hadoop.fs.azure.account.oauth2.client.id.devmbxadl.dfs.core.windows.net" = "*********"
	"spark.hadoop.fs.azure.account.oauth2.client.secret.devmbxadl.dfs.core.windows.net" = "**************"
	"spark.hadoop.fs.azure.account.oauth2.client.endpoint.devmbxadl.dfs.core.windows.net" = "************"

# COMMAND ----------

# MAGIC %md
# MAGIC load look up table

# COMMAND ----------

# File location and type
file_location = "mnt/dev/data/test/lookup/*"
file_type = "csv"

# CSV options
# not applying schema and datatype cleanup
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)
temp_table_name = "taxi_zone_lookup"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC load raw fhv data

# COMMAND ----------

# File location and type
file_location = "mnt/dev/data/test/fhv/*"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

temp_table_name = "raw_fhv_tripdata"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC clean raw fhv data and create external hive table in adls

# COMMAND ----------

transformed_fhv_sql = f"""
select distinct *,EXTRACT(year from cast(pickup_datetime as date)) as year, EXTRACT(month from cast(pickup_datetime as date)) as month from raw_fhv_tripdata
"""

# COMMAND ----------

transformed_fhv_df = spark.sql(transformed_fhv_sql)
transformed_fhv_df.write.partitionBy("year","month").mode("overwrite").option('path', '/mnt/dev/data/test/final/fhv/').saveAsTable('transformaed_hive_fhv')
display(transformed_fhv_df)

# COMMAND ----------

fhv_tripdata_df=spark.read.parquet('/mnt/dev/data/test/final/fhv/')
fhv_tripdata_df.createOrReplaceTempView('fhv_tripdata')

# COMMAND ----------

calculated_fhv_sql="""
select distinct Borough, month, count(*) as cnt from fhv_tripdata
join taxi_zone_lookup
on taxi_zone_lookup.locationid=fhv_tripdata.PULocationID
where Borough!='Unknown'
group by Borough,month order by month,cnt desc"""

# COMMAND ----------

calculated_fhv_df = spark.sql(calculated_fhv_sql)
calculated_fhv_df.write.mode("overwrite").option('path','/mnt/dev/data/test/calculated/fhv/').saveAsTable('calculated_hive_fhv')
display(calculated_fhv_df)

# COMMAND ----------

# MAGIC %md
# MAGIC load raw yellow data from adls

# COMMAND ----------

# File location and type
file_location = "mnt/dev/data/test/yellow/*"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

temp_table_name = "raw_yellow_tripdata"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

transformed_yellow_sql = f"""
select distinct *,EXTRACT(year from cast(tpep_pickup_datetime as date)) as year, EXTRACT(month from cast(tpep_pickup_datetime as date)) as month from raw_yellow_tripdata
where cast(tpep_pickup_datetime as date)  between '2020-01-01' and '2020-02-28' 
and fare_amount>0
"""

# COMMAND ----------

transformed_yellow_df = spark.sql(transformed_yellow_sql)
transformed_yellow_df.write.partitionBy("year","month").mode("overwrite").option('path', '/mnt/dev/data/test/final/yellow/').saveAsTable('transformed_hive_yellow')
display(transformed_yellow_df)

# COMMAND ----------

yellow_tripdata_df=spark.read.parquet('/mnt/dev/data/test/final/yellow/')
yellow_tripdata_df.createOrReplaceTempView('yellow_tripdata')

# COMMAND ----------

calculated_yellow_sql="""
select distinct cast(tpep_pickup_datetime as date) as trip_date, sum(passenger_count)  as passenger_count 
from yellow_tripdata  group by trip_date order by passenger_count desc"""

# COMMAND ----------

calculated_yellow_df = spark.sql(calculated_yellow_sql)
calculated_yellow_df.write.mode("overwrite").option('path', '/mnt/dev/data/test/calculated/yellow/').saveAsTable('calculated_hive_yellow')
display(calculated_yellow_df)

# COMMAND ----------

# MAGIC  %md
# MAGIC 
# MAGIC  ##### REPEAT THE SAME FOR GREEN AND FHVHV

# COMMAND ----------

# MAGIC %md
# MAGIC load green data

# COMMAND ----------

# File location and type
file_location = "mnt/dev/data/test/green/*"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

temp_table_name = "green_tripdata"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct VendorID,cast(lpep_pickup_datetime as date) as trip_date, count(*) from green_tripdata where cast(lpep_pickup_datetime as date)  between '2020-01-01' and '2020-02-28' and fare_amount>0 
# MAGIC and VendorID is not null group by  trip_date,VendorID order by trip_date

# COMMAND ----------

# MAGIC %md
# MAGIC load and process fhvhv data

# COMMAND ----------

# File location and type
file_location = "mnt/dev/data/test/fhvhv/*"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

temp_table_name = "fhvhv_tripdata"

df.createOrReplaceTempView(temp_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC select month(cast(pickup_datetime as date)) trip_month,
# MAGIC case when hvfhs_license_num='HV0002' then 'Juno'
# MAGIC      when hvfhs_license_num='HV0003' then  'Uber'
# MAGIC       when hvfhs_license_num='HV0004' then  'Via'
# MAGIC     when hvfhs_license_num='HV0005' then  'Lyft'
# MAGIC     end as business_vendor,count(*) as cnt from fhvhv_tripdata
# MAGIC     where cast(pickup_datetime as date)  between '2020-01-01' and '2020-02-28'
# MAGIC     group by trip_month,business_vendor order by trip_month,cnt

# COMMAND ----------

# MAGIC %md
# MAGIC option to upload data to filestore and create table

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/yellow_tripdata_2020_01.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

# Create a view or table

temp_table_name = "yellow_tripdata"

df.createOrReplaceTempView(temp_table_name)
