#!/usr/bin/env python
# coding: utf-8

# # Production of indicators for the COVID19 Mobility Task Force
# 
# In this notebook we produce indicators for the [COVID19 Mobility Task Force](https://github.com/worldbank/covid-mobile-data).
# 
# [Flowminder](https://covid19.flowminder.org) indicators are produced to increase the availability of comparable datasets across countries, and have been copied without modification from the [Flowminder COVID-19 github repository](https://github.com/Flowminder/COVID-19) (except for the start and end dates). These have been supplemented by a set of *priority* indicators with data for ingestion into the dashboard in this repository.
# 
# In this notebook we produce indicators in the following four steps:
# 
# - **Import code**: The code for the aggregation is included in the 'custom_aggregation' and 'flowminder_aggregation' scripts
# - **Import data**: 
# To set up the data import we need to place the CDR data files into the `data/new/CC/telco/` folder, where we replace `CC` with the country code and `telco` with the company abbreviation. 
# We also need to place csv files with the tower-region mapping and distance matrices into the `data/support-data/CC/telco/geofiles` folder, and then modify the `data/support_data/config_file.py` to specify:
#     - *geofiles*: the names of the geofiles, 
#     - *country_code*: country code and company abbreviation,
#     - *telecom_alias*: the path to the `data` folder,
#     - *data_paths*: the names to the subfolders in `data/new/CC/telco/` that hold the csv files. Simply change this to `[*]` if you didn't create subfolders and want to load all files.
#     - *dates*: set the start and end date of the data you want to produce the indicators for.
#     
# Find more information about the `config_file.py` settings see the [github page](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).
#     
# - **Run aggregations**: By default, we produce all flowminder and priority indicators. We've included 4 re-tries in case of failure, which we have experienced to help on databricks but is probably irrelevant in other settings. Note that before you can re-run these aggregations, you need to move the csv outputs that have been saved in `data/results/CC/telco/` in previous runs to another folder, else these indicators will be skipped. This prevents you from accidentally overwriting previous results. This way you can also delete the files only for the indicators you want to re-produce, and skip any indicatos you don't want to re-produce.
# 
# The outcome of this effort will be used to inform policy making using a [mobility indicator dashboard](https://github.com/worldbank/covid-mobile-data/tree/master/dashboard-dataviz).

# # Import code

# In[1]:


get_ipython().run_line_magic('load_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')


# In[2]:


from modules.DataSource import *


# In[3]:


config_file = '../config_file.py'


# In[4]:


exec(open(config_file).read())


# In[5]:


ds = DataSource(datasource_configs)
ds.show_config()


# In[6]:


from modules.setup import *


# # Import data

# ## Load CDR data

# ### Process/standardize raw data, save as parquet, and then load it

# In[7]:


# ds.standardize_csv_files(show=True)
# ds.save_as_parquet()


# In[8]:


#ds.load_standardized_parquet_file()


# ### Alternatively, specify and load hive table

# In[9]:


# # Specify and load hive data
# ds.parquet_df = ds.spark.sql("""SELECT {} AS msisdn, 
#                                        {} AS call_datetime, 
#                                        {} AS location_id FROM {}""".format(ds.hive_vars['msisdn'],
#                                                                            ds.hive_vars['call_datetime'],
#                                                                            ds.hive_vars['location_id'],
#                                                                            ds.hive_vars['calls']))


# ### Or load a sample file

# In[10]:


## Use this in case you want to sample the data and run the code on the sample

# #ds.sample_and_save(number_of_ids=1000)
ds.load_sample('sample_feb_mar2020')
ds.parquet_df = ds.sample_df


# ## Load geo data

# In[11]:


ds.load_geo_csvs()


# In[12]:


## Use this in case you want to cluster the towers and create a distance matrix

# ds.create_gpds()
# from modules.tower_clustering import *
# clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
# ds.admin2_tower_map, ds.distances = clusterer.cluster_towers()
# clusterer = tower_clusterer(ds, 'admin3', 'ADM3_PCODE')
# ds.admin3_tower_map, ds.distances  = clusterer.cluster_towers()


# In[13]:


## Use this in case you want to create a voronoi tesselation

# from modules.voronoi import *
# voronoi = voronoi_maker(ds, 'admin3', 'ADM3_PCODE')
# ds.voronoi = voronoi.make_voronoi()


# # Run aggregations

# ## Flowminder indicators for admin2

# In[14]:


agg_flowminder_admin2 = flowminder_aggregator(result_stub = '/admin2/flowminder',
                            datasource = ds,
                            regions = 'admin2_tower_map')

agg_flowminder_admin2.attempt_aggregation()


# ## Flowminder indicators for admin3

# In[15]:


agg_flowminder_admin3 = flowminder_aggregator(result_stub = '/admin3/flowminder',
                            datasource = ds,
                            regions = 'admin3_tower_map')

agg_flowminder_admin3.attempt_aggregation()


# ## Priority indicators for admin2

# In[16]:


agg_priority_admin2 = priority_aggregator(result_stub = '/admin2/priority',
                               datasource = ds,
                               regions = 'admin2_tower_map')

agg_priority_admin2.attempt_aggregation(indicators_to_produce = {'unique_subscribers_per_day' : ['unique_subscribers', 'day'],
                                                                 'percent_of_all_subscribers_active_per_day' : ['percent_of_all_subscribers_active', 'day'],
                                                                 'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day'],
                                                                 'mean_distance_per_day' : ['mean_distance', 'day'],
                                                                 'mean_distance_per_week' : ['mean_distance', 'week'],
                                                                 'origin_destination_matrix_time_per_day' : ['origin_destination_matrix_time', 'day'],
                                                                 'home_vs_day_location_per_day' : ['home_vs_day_location_per_day', ['day','week']],
                                                                 'home_vs_day_location_per_day' : ['home_vs_day_location_per_day', ['day','month']]})


# ## Priority indicators for admin3

# In[17]:


agg_priority_admin3 = priority_aggregator(result_stub = '/admin3/priority',
                            datasource = ds,
                            regions = 'admin3_tower_map')

agg_priority_admin3.attempt_aggregation(indicators_to_produce = {'transactions_per_hour' : ['transactions', 'hour'],
                                                                 'transactions_per_hour' : ['transactions', 'hour']})


# ## Scaled priority indicators for admin2

# In[ ]:


agg_scaled_admin2 = scaled_aggregator(result_stub = '/admin2/scaled',
                               datasource = ds,
                               regions = 'admin2_tower_map')

agg_scaled_admin2.attempt_aggregation()


# ## Priority indicators for tower-cluster

# In[ ]:


agg_priority_tower = priority_aggregator(result_stub = '/voronoi/priority',
                               datasource = ds,
                               regions = 'voronoi_tower_map')

agg_priority_tower.attempt_aggregation(indicators_to_produce = {'unique_subscribers_per_hour' : ['unique_subscribers', 'hour'],
                                                        'mean_distance_per_day' : ['mean_distance', 'day'],
                                                        'mean_distance_per_week' : ['mean_distance', 'week']})


# In[ ]:


agg_priority_tower_harare = priority_aggregator(result_stub = '/voronoi/priority/harare',
                               datasource = ds,
                               regions = 'voronoi_tower_map_harare')

agg_priority_tower_harare.attempt_aggregation(indicators_to_produce = {'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day']})


# In[ ]:


agg_priority_tower_bulawayo = priority_aggregator(result_stub = '/voronoi/priority/bulawayo',
                               datasource = ds,
                               regions = 'voronoi_tower_map_bulawayo')

agg_priority_tower_bulawayo.attempt_aggregation(indicators_to_produce = {'origin_destination_connection_matrix_per_day' : ['origin_destination_connection_matrix', 'day']})


# # Produce script

# In[ ]:


get_ipython().system('jupyter nbconvert --to script *.ipynb')


# In[ ]:




