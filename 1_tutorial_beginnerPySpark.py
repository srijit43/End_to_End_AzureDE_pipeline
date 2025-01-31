# Databricks notebook source
# MAGIC %md
# MAGIC # Starting the class

# COMMAND ----------

# MAGIC %md
# MAGIC ## data reading

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',True).option('header',True).load('/FileStore/tables/BigMart_Sales.csv') #transformation

# COMMAND ----------

df.show()

# COMMAND ----------

df.display() #action

# COMMAND ----------

# MAGIC %md
# MAGIC ### You can click on the Spark Job and see the duration, job id and look inside what goes on 

# COMMAND ----------

# MAGIC %md
# MAGIC We will see how to import data from JSON, parquet formats

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables/')

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema',True).option('header',True)\
                                   .option('header',True)\
                                    .option('multiLine',False)\
                                    .load('/FileStore/tables/drivers.json')



# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## DDL and StructType

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
                    Item_Identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content string,
                    Item_Visibility double,
                    Item_Type string,
                    Item_MRP double,
                    Outlet_Identifier string,
                    Outlet_Establishment_Year integer,
                    Outlet_Size string,
                    Outlet_Location_Type string,
                    Outlet_Type string,
                    Item_Outlet_Sales double
                    '''





# COMMAND ----------

df = spark.read.format('csv').schema(my_ddl_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType() Schema

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

my_struct_schema = StructType([
    StructField('Item_Identifier',StringType(),True),
    StructField('Item_Weight',StringType(),True),
    StructField('Item_Fat_Content',StringType(),True),
    StructField('Item_Visibility',StringType(),True),
    StructField('Item_MRP',StringType(),True),
    StructField('Outlet_Identifier',StringType(),True),
    StructField('Outlet_Establishment_Year',StringType(),True),
    StructField('Outlet_Size',StringType(),True),
    StructField('Outlet_Location_Type',StringType(),True),
    StructField('Outlet_Type',StringType(),True),
    StructField('Item_Outlet_Sales',StringType(),True)
])

# COMMAND ----------

df = spark.read.format('csv').schema(my_struct_schema).option('header',True).load('/FileStore/tables/BigMart_Sales.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Transformation (DML Stuff)

# COMMAND ----------

df.display()

# COMMAND ----------

df_select = df.select('Item_Identifier') #chose your required columns

# COMMAND ----------

df_select.show()

# COMMAND ----------

df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

#using column object

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Alias

# COMMAND ----------

#column renaming
df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filter/Where condition

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Fat_Content')=='Low Fat')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3: fetch data in tier 1 or tier 2 where outlet size is null

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###  With Column Renamed

# COMMAND ----------

#modify colum
df = df.withColumn('flag',lit('new')).display()

# COMMAND ----------

df.withColumn('total_price', col('Item_Weight') * col('Item_MRP')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df_new = df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))

# COMMAND ----------

df_new.display()

# COMMAND ----------

df_new = df_new.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Low Fat","LF"))

# COMMAND ----------

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC typecast

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast('String'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Sorting Data

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Scenario 3: We want to perform sorting based on multiple columns

# COMMAND ----------

df.sort(col('Item_Weight').desc(), col('Item_Visibility').desc()).display()

# COMMAND ----------


