# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *

# COMMAND ----------

# dbutils.fs.ls('/mnt/COVID19Data/Sveta Milusheva - Econet/mar20')
base_path = '/mnt/COVID19Data/Sveta Milusheva - Econet/mar20/'
geo_path =  'mnt/COVID19Data/proof-of-concept/support-data/ZW/econet/geo-files/'

# COMMAND ----------

# Load tower mapping to districts
cells = spark.read.format("csv")\
  .option("header", "true")\
  .load(geo_path + 'zw_admin3_tower_map.csv')

# COMMAND ----------

cells.show()

# COMMAND ----------

# Set default schema
schema = StructType([
  StructField("msisdn", IntegerType(), True),
  StructField("call_datetime", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("location_id", StringType(), True)
])

# Import one day at a time

mar20 = spark.read.format("csv")\
  .option("header", "true")\
  .load(base_path + 'MOH_EWZ_20200320.csv', schema = schema)

mar21 = spark.read.format("csv")\
  .option("header", "true")\
  .load(base_path + 'MOH_EWZ_20200320.csv', schema = schema)



# COMMAND ----------

# Process data

def create_vars(df, cells):
    df = df.withColumn("call_datetime", to_timestamp("call_datetime","dd/MM/yyyy HH:mm:ss"))
    #get call_date from call_datetime
    df = df.withColumn('call_date', raw_df.call_datetime.cast('date'))

    return df

foo = create_vars(mar20, cells)
  

# COMMAND ----------

# mar20.show()

# COMMAND ----------


# 1. Merge with tower mapping to wards

# 2. Recreate vars

# 4. 


# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

dbutils.fs.ls('/mnt/')

# COMMAND ----------

test_df = spark.read\
  .option('header', 'true')\
  .option('inferSchema', 'true')\
  .csv('/mnt/COVID19Data/proof-of-concept/new/ZW/telecel/world_bank_cdr_new.csv')


# COMMAND ----------

test_df.printSchema()