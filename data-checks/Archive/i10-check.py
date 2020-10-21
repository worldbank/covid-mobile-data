# Databricks notebook source
# dbutils.fs.ls('/mnt/')
# dbutils.fs.refreshMounts()

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Constat definitions
privacy_filter = 15
missing_value_code = 99999
cutoff_days = 7
max_duration = 21

user_window = Window\
    .partitionBy('msisdn').orderBy('call_datetime')


# COMMAND ----------

# dbutils.fs.ls('/mnt/COVID19Data/Sveta Milusheva - mar20')
base_path = '/mnt/COVID19Data/Sveta Milusheva - mar20/'
geo_path =  'mnt/COVID19Data/proof-of-concept/support-data/geo-files/'

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
    # Loading variables
    df = df.withColumn("call_datetime", to_timestamp("call_datetime","dd/MM/yyyy HH:mm:ss"))
    #get call_date from call_datetime
    df = df.withColumn('call_date', df.call_datetime.cast('date'))
    
    # Recreate analysis variables
    df = df.join(cells, df.location_id == cells.cell_id, how = 'left').drop('cell_id')\
      .orderBy('msisdn', 'call_datetime')\
      .withColumn('region_lag', F.lag('region').over(user_window))\
      .withColumn('region_lead', F.lead('region').over(user_window))\
      .withColumn('call_datetime_lag', F.lag('call_datetime').over(user_window))\
      .withColumn('call_datetime_lead', F.lead('call_datetime').over(user_window))\
      .withColumn('hour_of_day', F.hour('call_datetime').cast('byte'))\
      .withColumn('hour', F.date_trunc('hour', F.col('call_datetime')))\
      .withColumn('week', F.date_trunc('week', F.col('call_datetime')))\
      .withColumn('month', F.date_trunc('month', F.col('call_datetime')))\
      .withColumn('constant', F.lit(1).cast('byte'))\
      .withColumn('day', F.date_trunc('day', F.col('call_datetime')))\
      .na.fill({'region' : missing_value_code ,
                'region_lag' : missing_value_code ,
                'region_lead' : missing_value_code })    

    return df

mar20 = create_vars(mar20, cells)
mar21 = create_vars(mar21, cells)

# COMMAND ----------

mar20.columns

# COMMAND ----------

# Create simple OD matrix
def simp_od(df):
  
  # Kepp if region and region_lag/lead are not the same
  df = df.where((F.col('region_lag') != F.col('region')) | (F.col('region_lead') != F.col('region')) | (F.col('call_datetime_lead').isNull()))
  
  # Aggregate total sum by region and region_lag
  agg_df = df.groupby('region', 'region_lag')\
    .agg(F.count("*"))
  
  return agg_df
  
m20_agg = simp_od(mar20)
m21_agg = simp_od(mar21)

# COMMAND ----------

m20_agg.show()

# COMMAND ----------

# mar20.show()

# COMMAND ----------


# 1. Merge with tower mapping to wards

# 2. Recreate vars

# 4. 


# COMMAND ----------

a

# COMMAND ----------



# COMMAND ----------

test_df = spark.read\
  .option('header', 'true')\
  .option('inferSchema', 'true')\
  .csv('/mnt/COVID19Data/proof-of-concept/new/ZW/telecel/world_bank_cdr_new.csv')
dd

# COMMAND ----------

test_df.printSchema()
