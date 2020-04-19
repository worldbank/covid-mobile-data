# Databricks notebook source
# DBTITLE 1,Verbose notebook to run through aggregation steps
# MAGIC %md 
# MAGIC ## Notebook organization
# MAGIC 1. Load CDR data from csvs and convert columns to what we need
# MAGIC 2. Basic sanity checks
# MAGIC 3. Import tower - admin region mapping
# MAGIC 3. Run sql queries and save as csvs

# COMMAND ----------

# MAGIC %md
# MAGIC Importing the necessary code:

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/DataSource 

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/import_packages

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/utilities

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/sql_code_aggregates

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/outliers

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/voronoi

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/tower_clustering

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/flowminder_aggregations

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/custom_aggregations

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Load CDR Data 
# MAGIC - Load from CSV files
# MAGIC - Standardize columns
# MAGIC - Save as parquet file

# COMMAND ----------

basepath = '/mnt/COVID19Data/proof-of-concept'

# COMMAND ----------

# DBTITLE 1,Load Config file for this datasource
# MAGIC %run COVID19DataAnalysis/datasource_config_files/datasource-configure-zw1

# COMMAND ----------

# DBTITLE 1,Configure and create DataSource object
#Set up the datasource object, and show the config settings
ds = DataSource(datasource_configs)
ds.show_config()

# COMMAND ----------

#Standardize the csv and save as parque
ds.standardize_csv_files(show=True)
ds.save_as_parquet()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 2. Raw data checks
# MAGIC - Transactions per day
# MAGIC - Unique IDs per day
# MAGIC - Unique IDs per week
# MAGIC - Some country level summary stats

# COMMAND ----------

ds.load_standardized_parquet_file()
calls = ds.parquet_df
calls.createOrReplaceTempView("calls")

# COMMAND ----------

# calls.limit(5).toPandas()

# COMMAND ----------

# DBTITLE 1,Plot of transactions per day
# Active accounts per day

# Set up frame
fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (20, 5))

num_data_points = calls\
        .groupby('call_date')\
        .count()
save_csv(num_data_points, ds.results_path, 'total_transactions_per_day')

num_data_points = num_data_points\
        .toPandas()\
        .set_index('call_date')

num_data_points = fill_zero_dates(num_data_points)
sns.lineplot(x = num_data_points.index , y = num_data_points.values[:,0], ax = axes, color ='tab:blue')

axes.set(title = 'Number of data points per day', ylabel = 'count', xlabel = 'Day')
display(plt.show())

# COMMAND ----------

# DBTITLE 1,Plot of distinct IDs per day
# Active accounts per day

# Set up frame
fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (20, 5))

distinct_ids = calls\
        .groupby('call_date')\
        .agg(F.countDistinct('msisdn'))
save_csv(distinct_ids, ds.results_path, 'total_unique_subscribers_per_day')
distinct_ids = distinct_ids\
        .toPandas()\
        .set_index('call_date')
distinct_ids = fill_zero_dates(distinct_ids)
sns.lineplot(x = distinct_ids.index , y = distinct_ids.values[:,0], ax = axes, color ='tab:red')

axes.set(title = 'Number of distinct IDs per day', ylabel = 'count', xlabel = 'Day')
display(plt.show())

# COMMAND ----------

# DBTITLE 1,Plot of distinct IDs per week
# Active accounts per day

# Set up frame
fig, axes = plt.subplots(nrows=1,ncols=1, figsize = (20, 5))

distinct_ids_week = calls.withColumn('week', F.date_trunc('week', F.col('call_datetime')))\
        .where((F.col('call_datetime') > dt.datetime(2020,2,3)) & (F.col('call_datetime') < dt.datetime(2020,3,30)))\
        .groupby('week')\
        .agg(F.countDistinct('msisdn'))
save_csv(distinct_ids_week, ds.results_path, 'distinct_ids_per_week')
distinct_ids_week = distinct_ids_week\
        .toPandas()\
        .set_index('week')
sns.lineplot(x = distinct_ids.index , y = distinct_ids.values[:,0], ax = axes, color ='tab:green')

axes.set(title = 'Number of distinct IDs per week', ylabel = 'count', xlabel = 'week')
display(plt.show())

# COMMAND ----------

# DBTITLE 1,Country level summary stats (Feb 1 users only)
# helper function to count number of times cond(ition) is met
cnt_cond = lambda cond: F.sum(F.when(cond, 1).otherwise(0))
feb_users = calls.withColumn('feb_user', F.when(cnt_cond(calls['call_datetime'] < dt.datetime(2020,3,1)).over(user_window) > 0, 1).otherwise(0)).where(F.col('feb_user') == 1)
num_data_points_feb = feb_users\
        .groupby('call_date')\
        .count()
save_csv(num_data_points_feb, ds.results_path, 'total_transactions_by_users_seen_in_feb_per_day')
distinct_ids_feb = feb_users\
        .groupby('call_date')\
        .agg(F.countDistinct('msisdn'))
save_csv(distinct_ids_feb, ds.results_path, 'total_unique_subscribers_seen_in_feb_per_day')

# COMMAND ----------

# DBTITLE 1,Outlier counts
thresholds = {'min_transactions' : 3,
              'max_avg_transactions' : 100,
              'max_transactions_in_single_day' : 300}

outliers = outlier_counter(calls, thresholds = thresholds)
outliers.count()

# COMMAND ----------

# MAGIC %md ## 3. Load shapefiles of admin regions and tower locations

# COMMAND ----------

# DBTITLE 1,Shapefiles and tower locations
sites = spark.read.format("csv")\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .option("mode", "DROPMALFORMED")\
  .load(ds.support_data + "/geo-files/" + ds.geofiles["tower_sites"])
sites = sites.toPandas()

# COMMAND ----------

shape = spark.read.format("csv")\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .option("mode", "DROPMALFORMED")\
  .load(ds.support_data + "/geo-files/" + ds.geofiles["admin3"])
shape = shape.toPandas()

shape['geometry'] = shape['geometry'].apply(wkt.loads)
shape_gpd = gpd.GeoDataFrame(shape, geometry = 'geometry', crs = 'epsg:4326')

# COMMAND ----------

shape2 = spark.read.format("csv")\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .option("mode", "DROPMALFORMED")\
  .load(ds.support_data + "/geo-files/" + ds.geofiles["admin2"])
shape2 = shape2.toPandas()
from shapely import wkt
shape2['geometry'] = shape2['geometry'].apply(wkt.loads)
shape_gpd2 = gpd.GeoDataFrame(shape2, geometry = 'geometry', crs = 'epsg:4326')

# COMMAND ----------

shape_aivin = spark.read.format("csv")\
  .option("header", "true")\
  .option("delimiter", ",")\
  .option("inferSchema", "true")\
  .option("mode", "DROPMALFORMED")\
  .load(ds.support_data + "/geo-files/" + ds.geofiles["admin2_aivin"])
shape_aivin = shape_aivin.toPandas()
from shapely import wkt
shape_aivin['geometry'] = shape_aivin['geometry'].apply(wkt.loads)
shape_aivin_gpd = gpd.GeoDataFrame(shape_aivin, geometry = 'geometry', crs = 'epsg:4326')

# COMMAND ----------

# DBTITLE 1,Create regions
sites_handler = tower_clusterer(shape = shape_gpd, 
                                  sites_df = sites, 
                                  region_var = 'ADM3_PCODE',
                                  result_path = ds.results_path + '/admin3',
                                  filename = ds.geofiles["admin3"])
sites_handler.cluster_towers()
regions = sites_handler.towers_regions_clusters

# COMMAND ----------

display(sites_handler.shape.plot())

# COMMAND ----------

sites_handler2 = tower_clusterer(shape = shape_gpd2, 
                                  sites_df = sites, 
                                  region_var = 'ID_2',
                                  result_path = ds.results_path + '/admin2',
                                  filename = ds.geofiles["admin2"])
sites_handler2.cluster_towers()
regions2 = sites_handler2.towers_regions_clusters                                 

# COMMAND ----------

display(sites_handler2.shape.plot())

# COMMAND ----------

sites_handler_aivin = tower_clusterer(shape = shape_aivin_gpd, 
                                  sites_df = sites, 
                                  region_var = 'DIST2012',
                                  result_path = result_path + '/admin2',
                                  filename = ds.geofiles["admin2_aivin"])
sites_handler_aivin.cluster_towers()
regions_aivin = sites_handler_aivin.towers_regions_clusters                 

# COMMAND ----------

display(sites_handler_aivin.shape.plot())

# COMMAND ----------

voronoi_handler = voronoi_maker(spark_df = calls, 
                                  sites_handler = sites_handler, 
                                  result_path = ds.results_path + '/voronoi',
                                  filename = ds.geofiles["voronoi"])
voronoi_handler.make_voronoi()
voronois = voronoi_handler.voronoi_dict

# COMMAND ----------

display(voronoi_handler.voronoi_gpd.plot())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregations
# MAGIC - aggregations of flowminder indicators for amdin level 3
# MAGIC - aggregations of flowminder indicators for amdin level 2
# MAGIC - aggregations of custom indicators for amdin level 3
# MAGIC - aggregations of custom indicators for amdin level 2

# COMMAND ----------

# DBTITLE 1,Define time period
dates_sql = {'start_date' : "\'2020-02-01\'",
         'end_date' :  "\'2020-03-31\'",
         'start_date_weeks' :  "\'2020-02-03\'",
         'end_date_weeks' : "\'2020-03-29\'"}

dates = {'start_date' : dt.datetime(2020,2,1),
         'end_date' : dt.datetime(2020,3,31),
         'start_date_weeks' : dt.datetime(2020,2,3),
         'end_date_weeks' : dt.datetime(2020,3,29)}

# COMMAND ----------

# DBTITLE 1,Aggregation of flowminder indicators at admin3 level
# admin3 level
regions.createOrReplaceTempView("cells")
agg_flowminder = aggregator(result_stub = ds.results_path + '/admin3/flowminder',
                            dates_sql = dates_sql)

attempt_aggregation(agg_flowminder, aggregator_type = 'flowminder', no_of_attempts = 4)

# COMMAND ----------

# DBTITLE 1,Aggregation of flowminder indicators at admin2 level
# admin2 level
regions2.createOrReplaceTempView("cells")
agg_flowminder = aggregator(result_stub = '/admin2/flowminder',
                            dates_sql = dates_sql)

attempt_aggregation(agg_flowminder, aggregator_type = 'flowminder', no_of_attempts = 4)

# COMMAND ----------

# DBTITLE 1,Aggregation of custom indicators at admin3 level
# admin3 level
regions.createOrReplaceTempView("cells")

agg_custom = custom_aggregator(sites_handler, 
                               re_use_home_locations = False,
                               result_stub = '/admin3/custom',
                               dates = dates)

attempt_aggregation(agg_custom, indicators_to_produce= 'all', aggregator_type = 'custom', no_of_attempts = 4)

# COMMAND ----------

# DBTITLE 1,Aggregation of custom indicators at admin2 level
# admin2 level
regions2.createOrReplaceTempView("cells")

agg_custom = custom_aggregator(sites_handler2, 
                               re_use_home_locations = False,
                               result_stub = '/admin2/custom',
                               dates = dates)

indicators_to_produce = {'table_1' : ['home_vs_day_location', ['day', {'home_location_frequency' : 'month'}]]}

attempt_aggregation(agg_custom, indicators_to_produce, aggregator_type = 'custom', no_of_attempts = 4)

# COMMAND ----------

# DBTITLE 1,Aggregation of custom indicators at tower level
# voronoi
voronois.createOrReplaceTempView("cells")

agg_custom = custom_aggregator(voronoi_handler, 
                               re_use_home_locations = False,
                               result_stub = '/voronoi/custom',
                               dates = dates)

indicators_to_produce = {'table_1' : ['unique_subscribers', 'hour']}

attempt_aggregation(agg_custom, indicators_to_produce, aggregator_type = 'custom', no_of_attempts = 4)

# COMMAND ----------

# admin2 2012 level
regions_aivin.createOrReplaceTempView("cells")

agg_custom = custom_aggregator(sites_handler_aivin, 
                               re_use_home_locations = True,
                               result_stub = '/admin2/custom/aivin',
                               dates = dates)
    
indicators_to_produce = {'table_1' : ['origin_destination_matrix_time_longest_only', 'day'],
                         'table_2' : ['home_vs_day_location', ['day', {'home_location_frequency' : 'month'}]],
                         'table_3' : ['home_vs_day_location', ['day', {'home_location_frequency' : 'week'}]]}

attempt_aggregation(agg_custom, indicators_to_produce, aggregator_type = 'custom', no_of_attempts = 4)

# COMMAND ----------

