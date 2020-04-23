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
#
# Find more information about the `config_file.py` settings see the [github page](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).
#
# - **Set dates**: Set the start and end date of the data you want to produce the indicators for.
# - **Run aggregations**: By default, we produce all flowminder and priority indicators. We've included 4 re-tries in case of failure, which we have experienced to help on databricks but is probably irrelevant in other settings. Note that before you can re-run these aggregations, you need to move the csv outputs that have been saved in `data/results/CC/telco/` in previous runs to another folder, else these indicators will be skipped. This prevents you from accidentally overwriting previous results. This way you can also delete the files only for the indicators you want to re-produce, and skip any indicatos you don't want to re-produce.
#
# The outcome of this effort will be used to inform policy making using a [mobility indicator dashboard](https://github.com/worldbank/covid-mobile-data/tree/master/dashboard-dataviz).

# # Import code

# In[ ]:


# get_ipython().run_line_magic('load_ext', 'autoreload')
# get_ipython().run_line_magic('autoreload', '2')


# In[ ]:


from modules.setup import *


# In[ ]:


spark


# # Import data

# ## Set up the configuration for data standardization

# In[ ]:


import os
home = os.environ['HOME']
data = os.path.join(home, 'work/data')
config_file = os.path.join(data, 'support-data/config_file.py')


# In[ ]:


exec(open(config_file).read())


# In[ ]:


ds = DataSource(datasource_configs)
ds.show_config()


# ## Standardize raw csv files

# In[ ]:


ds.standardize_csv_files(show=True)
ds.save_as_parquet()


# In[ ]:


ds.load_standardized_parquet_file()


# In[ ]:


## Use this in case you want to sample the data and run the code on the sample

#ds.sample_and_save(number_of_ids=1000)
# ds.load_sample()
# ds.parquet_df = ds.sample_df


# ## Load geo data

# In[ ]:


ds.load_geo_csvs()


# In[ ]:


## Use this in case you want to cluster the towers and create a distance matrix

ds.create_gpds()
from modules.tower_clustering import *
clusterer = tower_clusterer(ds, 'admin2', 'ID_2')
ds.admin2_tower_map, ds.distances = clusterer.cluster_towers()
clusterer = tower_clusterer(ds, 'admin3', 'ADM3_PCODE')
ds.admin3_tower_map, ds.distances  = clusterer.cluster_towers()


# In[ ]:


## Use this in case you want to create a voronoi tesselation

from modules.voronoi import *
voronoi = voronoi_maker(ds, 'admin3', 'ADM3_PCODE')
ds.voronoi = voronoi.make_voronoi()


# # Set dates
#
# These dates need to indicate which period of data the indicator should be created for:
#
# - **start_date**: This matters mainly for the computation of home locations. For now, we haven't implemented loading of previously saved home locations, therefore it is recommended to leave this unchanged for now.
# - **end_date**: This needs to be set to the day the dataset ends.

# In[ ]:


dates = {'start_date' : dt.datetime(2020,2,1),
         'end_date' : dt.datetime(2020,3,31)}
dates = add_week_dates(dates)


# # Run aggregations

# ## Flowminder indicators for admin2

# In[ ]:


agg_flowminder = aggregator(result_stub = '/admin2/flowminder',
                            datasource = ds,
                            regions = 'admin2_tower_map',
                            dates = dates)

agg_flowminder.attempt_aggregation()


# ## Flowminder indicators for admin3

# In[ ]:


agg_flowminder = aggregator(result_stub = '/admin3/flowminder',
                            datasource = ds,
                            regions = 'admin3_tower_map',
                            dates = dates)

agg_flowminder.attempt_aggregation()


# ## Priority indicators for admin2

# In[ ]:


agg_custom = custom_aggregator(result_stub = '/admin2/custom',
                               datasource = ds,
                               regions = 'admin2_tower_map',
                               dates = dates)

agg_custom.attempt_aggregation()


# ## Priority indicators for admin3

# In[ ]:


agg_custom = custom_aggregator(result_stub = '/admin3/custom',
                            datasource = ds,
                            regions = 'admin3_tower_map',
                            dates = dates)

agg_custom.attempt_aggregation()
