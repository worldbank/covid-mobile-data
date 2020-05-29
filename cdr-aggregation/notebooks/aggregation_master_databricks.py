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

# MAGIC %run COVID19DataAnalysis/modules/aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/priority_aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/flowminder_aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/custom_aggregator

# COMMAND ----------

# MAGIC %run COVID19DataAnalysis/modules/scaled_aggregator

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 1. Load CDR Data 
# MAGIC - Load from CSV files
# MAGIC - Standardize columns
# MAGIC - Save as parquet file

# COMMAND ----------

# DBTITLE 1,Load Config file for this datasource
# MAGIC %run COVID19DataAnalysis/datasource_config_files/datasource-configure-zw1

# COMMAND ----------

# DBTITLE 1,Configure and create DataSource object
#Set up the datasource object, and show the config settings
ds = DataSource(datasource_configs)
ds.show_config()

# COMMAND ----------

# #Standardize the csv and save as parque
# ds.standardize_csv_files(show=True)
# ds.save_as_parquet()

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

ds.load_geo_csvs()

# COMMAND ----------

## Use this in case you want to cluster the towers and create a distance matrix

# ds.create_gpds()
# clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
# ds.admin2_tower_map, ds.distances = clusterer.cluster_towers()
# clusterer = tower_clusterer(ds, 'admin3', 'ADM3_PCODE')

# COMMAND ----------

## Use this in case you want to create a voronoi tesselation

# voronoi = voronoi_maker(ds, 'admin3', 'ADM3_PCODE')
# ds.voronoi = voronoi.make_voronoi()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregations
# MAGIC - aggregations of flowminder indicators for amdin level 3
# MAGIC - aggregations of flowminder indicators for amdin level 2
# MAGIC - aggregations of custom indicators for amdin level 3
# MAGIC - aggregations of custom indicators for amdin level 2

# COMMAND ----------

# DBTITLE 1,Aggregation of flowminder indicators at admin2 level
agg_flowminder_admin2 = flowminder_aggregator(result_stub = '/admin2/flowminder',
                            datasource = ds,
                            regions = 'admin2_tower_map')

agg_flowminder_admin2.attempt_aggregation()

# COMMAND ----------

# DBTITLE 1,Aggregation of flowminder indicators at admin3 level
agg_flowminder_admin3 = flowminder_aggregator(result_stub = '/admin3/flowminder',
                            datasource = ds,
                            regions = 'admin3_tower_map')

agg_flowminder_admin3.attempt_aggregation()

# COMMAND ----------

# DBTITLE 1,Aggregation of priority indicators at admin2 level
agg_priority_admin2 = priority_aggregator(result_stub = '/admin2/priority',
                               datasource = ds,
                               regions = 'admin2_tower_map')

agg_priority_admin2.attempt_aggregation()

# COMMAND ----------

# DBTITLE 1,Aggregation of priority indicators at admin3 level
agg_priority_admin3 = priority_aggregator(result_stub = '/admin3/priority',
                            datasource = ds,
                            regions = 'admin3_tower_map')

agg_priority_admin3.attempt_aggregation()

# COMMAND ----------

# DBTITLE 1,Aggregation of scaled indicators at admin2 level
agg_scaled_admin2 = scaled_aggregator(result_stub = '/admin2/scaled',
                               datasource = ds,
                               regions = 'admin2_tower_map')

agg_scaled_admin2.attempt_aggregation()

# COMMAND ----------

# DBTITLE 1,Aggregation of priority indicators for tower-cluster
agg_priority_tower = priority_aggregator(result_stub = '/voronoi/priority',
                               datasource = ds,
                               regions = 'voronoi_tower_map')

agg_priority_tower.attempt_aggregation(indicators_to_produce = {'unique_subscribers_per_hour' : ['unique_subscribers', 'hour'],
                                                        'mean_distance_per_day' : ['mean_distance', 'day'],
                                                        'mean_distance_per_week' : ['mean_distance', 'week']})

# COMMAND ----------

agg_priority_tower_harare = priority_aggregator(result_stub = '/voronoi/priority/harare',
                               datasource = ds,
                               regions = 'voronoi_tower_map_harare')

agg_priority_tower_harare.attempt_aggregation(indicators_to_produce = {'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day']})

# COMMAND ----------

agg_priority_tower_bulawayo = priority_aggregator(result_stub = '/voronoi/priority/bulawayo',
                               datasource = ds,
                               regions = 'voronoi_tower_map_bulawayo')

agg_priority_tower_bulawayo.attempt_aggregation(indicators_to_produce = {'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day']})