# Databricks notebook source
display(dbutils.fs.ls("/databricks-datasets"))

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/nyctaxi/tripdata/yellow"))

# COMMAND ----------

spark.read.option("header",True).csv("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Retail Org

# COMMAND ----------

display(dbutils.fs.ls("/databricks-datasets/retail-org/"))

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help("ls")

# COMMAND ----------

# MAGIC %fs ls /
# MAGIC

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/new", True)

# COMMAND ----------

for f in dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/"):
    if f.size > 0: print(f"File: {f.path} size in MB : {f.size / (1024 * 1024)}")


# COMMAND ----------

df = spark.read.format("csv").load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/")

# COMMAND ----------

df.display()

# COMMAND ----------

df = spark.read.format("csv").load("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019*")

# COMMAND ----------

df.show(3)

# COMMAND ----------

df.count()

# COMMAND ----------

files = "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019*"

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load(files)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Inferschema
# MAGIC

# COMMAND ----------

df.columns

# COMMAND ----------

df.dtypes

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.schema

# COMMAND ----------

df = (spark.read
      .format("csv")
      .option("header", True)
      .option("inferSchema",True)
      .option("samplingRatio",0.001)
      .load(files))

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC DDL

# COMMAND ----------

schema_string = "vendor_id INTEGER, pickup_datetime TIMESTAMP, dropoff_datetime TIMESTAMP, passenger_count INTEGER, trip_distance DOUBLE, rate_code_id INTEGER, store_and_fwd_flag STRING, pickup_location_id INTEGER, dropoff_location_id INTEGER, payment_type INTEGER, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE"

# COMMAND ----------

df = spark.read.format("csv").schema(schema_string).option("header", True).load(files)

# COMMAND ----------

display(df)

# COMMAND ----------

dfs = df\
    .sample(withReplacement=False, fraction=0.001, seed = 7)\
    .cache()

# COMMAND ----------

dfs.count()

# COMMAND ----------

