# Aggregation of CDR Data

### Table of contents

* [Initial set-up](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#set-up)
  * [DataSource class and config file](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#datasource-class-and-config-file)
  * [Docker set-up](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#docker-set-up)
  * [Databricks/datalake set-up](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#databricksdatalake-set-up)
* [Workflow description](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#workflow-description)
* [Indicators computation](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#indicators-computation)
* [Folder structure](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#folder-structure)



## Initial Set-up

The standardization and aggregation scripts in this repository are written for deployment using pyspark on linux and pyspark on Databricks (connected to a datalake). If you want to test the scripts and/or run them on a standalone machine, you can use the Dockerfile provided, which is based on the [jupyter/pyspark-notebook](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/selecting.html) image. We have special instructions for deploying the code on Databricks. In case you want to deploy the code using a different cluster setup and run into problems, please get in touch and we will do our best to help. Pull requests to add code that expands the range of deployment settings supported are welcome! Regardless which set-up you are using, you should start by creating a config file
that will be read by the [DataSource](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/modules/DataSource.py) class.

#### DataSource class and config file

The [DataSource](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/modules/DataSource.py) class reads raw pseudonymized CDR data using a config dict as the only constructor argument. We recommend setting up the config dict by adapting the [config_file_template](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/config_file_template.py).

###### Required parameters

* **base_path** `<class 'str'>`: The top data folder, where all data sub-folders for new data, standardized data, and results go. See folder structure for more details
* **country_code** and **telecom_alias** `<class 'str'>`: Within each data sub-folder, folders are organized after country and telecom, in case one setup is used for multiple countries or telecoms. See folder structure for more details
* **filestub** `<class 'str'>`: The name stub that will be used for all files generated, for example `<filestub>.parquet`
* **shapefiles** `<class list>`: A list of strings with the name of the geo_files that contain the admin region shapes.
* **dates** `<class dict>`: A dict that should have the two keys `"start_date"` and `"end_date"`, where the values are of type `datatime` and indicate the date range to be included
* **schema** `<class 'StructType'>`: The [spark schema](https://spark.apache.org/docs/latest/sql-reference.html) with the data types and column names. The `StructType` should have three `StructFields` and the order of the three `StructField` should match the order of the columns in the raw data files. See example below:
```
schema = StructType([
    StructField("msisdn", IntegerType(), True),
    StructField("call_datetime", StringType(), True), #Will be casted using datemask
    StructField("location_id", StringType(), True)
])
```

###### Optional parameters (and their default values)

* **data_paths**: Indicates the file paths (starting from `<base_path>/new/<country_code>/<telecom_alias>`) and file formats for the files that should be loaded and outputted in the standardized parquet file. This can be used to only read one sub-folder of data. For example by using `[mar20/*.csv]` to only read `.csv` files in the folder `mar20`. Default is `["*.csv.gz","*.csv"]` meaning all `.csv.gz` and `.csv` files immediately in the `<telecom_alias>` folder
* **geofiles** `<class dict>`: The aggregation scripts require mappings of the towers to administrative regions to which we want to aggregate. You can generate these mappings yourself using the `tower_clusterer` class, or you can contact us to support you with this task. If you are to do this yourself you will need to specify a list towers and their coordinates as well as the shapefiles of the administrative regions in csv format under `geofiles`. In case you already have this mapping or we supported you to generate it, you will need to specify the csv files containing the mapping under `geofiles`
* **load_seperator**: The delimiter used in the raw data files. Default is a comma - `,`
* **load_header** : Whether the raw data files has column names in the first row. Default is false (that they do not have column names in the first row) as we specify this in the schema
* **load_mode**: How will rows that does not fit the schema be handled? Default is `PERMISSIVE` where the record is loaded as good as possible and any errors will happen downstream. Alternatives are `DROPMALFORMED` where those records are skipped, and `FAILFAST` where the rest of the specific spark job loading the file is interrupted.
* **load_datemask**: The datestring mask that will be used when casting the datestring into a timestamp. The default is `dd/MM/yyyy HH:mm:ss`

###### Show setup of `DataSource` class

The DataSource class has a method called `show_config()` where a summary of the configuration of a class is showed in a readable format after it has been created. See example below:

```
#Set up the datasource object, and show the config settings
ds = DataSource(config_object)
ds.show_config()
```

##### Docker set-up

Unless you run a databricks/datalake set-up, the by far easiest way to get started is to run `aggregtion_offsite.ipynb` in a docker container. You first need to make sure that you have both `docker` and `docker-compose` installed. Get it [here](https://docs.docker.com/get-docker/). Then follow the steps blow:

1. Clone this repository if you haven’t yet.
1. Open a terminal and cd into the `cdr-aggregation` directory. Then run `docker-compose up` to start the docker container. You'll see the url for the jupyter server, copy that into your browser
1. Set up the config file. Use the [config_file_template](https://github.com/worldbank/covid-mobile-data/blob/master/covid-mobile-data/cdr-aggregation/config_file_template.py) and after you have modified it, save it in the `cdr-aggregation` folder. Use `"/home/jovyan/work/data"` as your base path in the docker environment
1. Set up the folder structure using [folder_setup.ipynb](https://github.com/worldbank/covid-mobile-data/blob/master/covid-mobile-data/cdr-aggregation\notebooks\folder_setup.ipynb) file in the `\cdr-aggregation\notebooks` folder
1. Add your raw data to the `<base_path>/new/<country_code>/<telecom_alias>` folder created in last step
1. Add shapefiles and tower locations, or alternatively add the tower-admin region mapping directly, to `<base_path>/support-data/<country_code>/<telecom_alias>/geofiles`. Get in touch to get assistance in accessing or creating these
1. Open the [aggregation_master.ipynb](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/aggregation_master.ipynb) notebook to run aggregations. If all went well, you should find the indicators in the `<base_path>/results/<country_code>/<telecom_alias>`

##### Databricks/datalake set-up
This is a manual tasks where the following steps need to be completed:
  1. Set up folders. Set up the `new`, `standardized`, `results`, `support_data` and `temporary` folders in the datalake according to the folder structure graph in the folder structure section below. Create the `country_code` and `telecom_alias` folder each folder.
  1. Set up the config file. Use the [config_file_template](https://github.com/worldbank/covid-mobile-data/blob/master/covid-mobile-data/cdr-aggregation/config_file_template.py) and after you have modified it, save it in the `cdr-aggregation` folder.
  1. Copy all the raw anonymized CDR data to the `<base_path>/new/<country_code>/<telecom_alias>` folder
  1. Add shapefiles and tower locations, or alternatively add the tower-admin region mapping directly, to `<base_path>/support-data/<country_code>/<telecom_alias>/geofiles`. Get in touch to get assistance in accessing or creating these.
  1. Then you can run the [aggregation_master.py](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/aggregation_master.py) notebook in your spark cluster creating aggregates from your data.

## Workflow description

The work flow in the CDR aggregation is separated into two main tasks (_standardization_ and _aggregation_)

##### Task: _standardization_

Input folder: `new`, Output folder: `standardized`

In the _standardization_ task the following is done:
* Read all files `<base_path>/new/<country_code>/<telecom_alias>` using the input from the config file
* Standardize column names and formats like this:

| column_name | format | example | description |
|---|---|---|---|
| msisdn | int | 123456789 | ID in call record (may never be a direct identifier like phone number) |
| call_datetime | time stamp | 2020-02-05 15:37:38 | The date and time of the record |
| location_id | string | ABC12345 | Tower ID or other location ID |
| call_date | date | 2020-02-05 | The date of the record - deducted form call_datetime |

* Standardized data is saved in parquet files in `<base_path>/standardized/<country_code>/<telecom_alias>`

##### Task: _aggregation_

Input folder: `standardized` and `support_data`, Output folder: `results`

In the _aggregation_ task the following is done:

* The parquet files will be processed using spark to produce aggregate indicators. See list of indicators in the indicator section.
* Save the aggregated indicators in normal unpartitioned csv files in the `<base_path>/results/<country_code>/<telecom_alias>`  folder.

**To run the aggregation, either run the [aggregation_master.ipynb](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/aggregation_master.ipynb) notebook or the [aggregation_master.py](./notebooks/aggregation_master.py) script.**

The aggregation  builds on much of the work that has been developed by [Flowminder](https://web.flowminder.org) for the purpose of supporting MNOs in producing basic indicators. Their code can be found in their [GitHub account](https://github.com/Flowminder).

## Indicators computation

* __Timeframe__: Indicators are built using data from Feb 1 2020 and going forward in real time.
* __Data Type__: CDR or, if available, network probes.
* __Description__: Towers are clustered using Ward’s hierarchical clustering, with a maximum distance constraint of 1km from the centroid of the cluster resulting in ‘flat’ clusters. We use the clustering function: https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.cluster.hierarchy.fcluster.html with criterion: distance and t = 1km. We then assign admin units based on the location of the centroid of the cluster (all observations in the cluster are then assigned to that admin unit).

Priority 1 indicators are given preference, priority 2 indicators are meant to help with assessing changes in the underlying data that could affect the indicators of interest, and priority 3 indicators are secondary.

##### Main indicators

| | String Key |	Geographic Level | Time Level | How to Calculate | Priority |
|-|-----------|------------------|------------|------------------|----------|
|1|	transactions |	Lowest admin level | Hour |	Sum across all observations in the given hour and lowest admin area.| 2 |
|2|	unique_subscribers |	Tower cluster level, Lowest admin level | Hour |Sum all unique subscribers with an observation in the given admin area and time period. | 1|
|3|	unique_subscribers_country |	Lowest admin level, Admin Level 2, Country | Day	| Sum all unique subscribers with an observation in the given admin area and time period. |1|
|4| percent_of_all_subscribers_active | Lowest admin level, Admin Level 2 | Day | Focusing just on the baseline period, calculate home location per individual and aggregate total SIMs per home location. On a daily basis, calculate the number of active users (defined as those that have made at least one observation on that day) but only focused on those SIMs that had at least one observation during the baseline period.  Take the ratio of active residents to total residents.|2|
|5|	origin_destination_connection_matrix|	Lowest admin level and Admin Level 2 | Day |	1. For each subscriber, list the unique regions that they visited within the time day, ordered by time. Create pairs of regions by pairing the nth region with all other regions that come after it. For example, the sequence [A, A, B, C, D] would result in the pairings [AA, AB, AC, AD, BC, BD, CD]. For each pair, count the number of times that pair appears. <br>2. For each subscriber, look at the location of the first observation of the day. Look back and get the location of the last observation before this one (up to seven days prior) to form a pair, keep the date assigned to this pair as the date of the first observation of the day <br>3. Sum all pairs from 1 and from 2 for each day (also keep the sum of 1 and sum of 2 as variables). |1|
|6|	unique_subscriber_home_locations | Lowest admin level |Week | Look at the location of the last observation on each day of the week and take the modal location to be the home location. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific week. |3|
|7| mean_distance |Lowest admin level and Admin 2 level | Day | Calculate distance traveled by each subscriber daily based on location of all calls (compute distance between tower cluster centroids). We define the distance to be counted towards the destination observation, which means that for every observation we calculate the distance from the previous, lagged observation for a user. We only consider lagged observations that are less than seven days prior. We then group subscribers by their home location and calculate mean distance traveled and SD for all subscribers from the same home region. |3|
|8|	mean_distance | Lowest admin level and Admin 2 level | Week |	Calculate distance traveled by each subscriber weekly based on location of all calls (compute distance between tower cluster centroids). We define the distance to be counted towards the destination observation, which means that for every observation we calculate the distance from the previous, lagged observation for a user. We only consider lagged observations that are less than seven days prior. We then group subscribers by their home location and calculate mean distance traveled and SD for all subscribers from the same home region. |3|

##### Indicators for Epidemiological modeling
| | Indicator |	Geographic Level | Time Level | How to Calculate | Priority |
|-|-----------|------------------|------------|------------------|----------|
|9|	home_vs_day_location | Admin level 2 | Day |	1. For days with at least one observation, assign the district where the most time is spent (so each subscriber present on a day should have one location) <br>2. Use weekly or monthly assigned home location (#6 above) for each person. <br>3. Create matrix of home location vs day location: for each home location sum the total number of people from that home location that are in each district (including the home one) based on where they spent the most time and get mean and SD for time spent in the location (by home location). |3|
|10|	origin_destination_matrix_time | Lowest admin level and Admin 2 Level	| Day |	1.  For each SIM, calculate movement between consecutive observations, considering only observations with a time difference of less than seven days.. <br>2. Calculate time spent in the origin and time spent in the destination for each trip <br>3. For each day, sum all people going from X to Y area and the average time spent in X before going and spent in Y after arriving.|1|
|11|	unique_subscriber_home_locations | Lowest admin level |Month | Look at the location of the last observation on each day of the month and take the modal location to be the home location. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific month. |3|

### Folder structure

There are four top level folders. `new`, `standardized`, `results` and `support_data`. All of those folders will have a country code folder (using the name `country_code` from the config file) and a telecom company folder (using the name `telecom_alias` from the config file). We recommend using a two letter country code for the country folder, like US for USA, SE for Sweden. The telecom folder can be named after the company or an alias to not disclose the company name. See the graph below of the required folder.

```
<base_path>
|-- new
| |-- <country_code>
|   |-- <telecom_alias>
|-- standardized
| |-- <country_code>
|   |-- <telecom_alias>
|-- support_data
| |-- <country_code>
|   |-- <telecom_alias>
|     |-- geo-files
|-- temporary
| |-- <country_code>
|   |-- <telecom_alias>
|-- results
| |-- <country_code>
|   |-- <telecom_alias>
```

##### Folder: `new`

* In each `<telecom_alias>` folder in the `new` folder, the raw data files should be saved in a format that can be read by spark. For example, `.csv` or `.csv.gz`
* Data can be stored directly in the `<telecom_alias>` folder or in subfolders, like `<telecom_alias>/feb20` and `<telecom_alias>/mar20`. If sub-folders are used, then that has to be indicated in the `data_paths` parameter in the config file.

##### Folder: `standardized`

* In each `<telecom_alias>` folder in the `standardized` folder, the standardized data will be saved in `.parquet` files.
* Samples that are sub-sets of the full data that is quicker to run will also be saved here as `.parquet` files.
* There should be no sub-folders in this folder.

##### Folder: `support-data`
* In each `<telecom_alias>` folder in the `support-data` folder, other files than raw or standardized data needed in in the aggregation are stored. These files include tower location data and shape files with the different administrative geographic areas. The name of these files are specified in the config file in `geofiles` and `shapefiles`

##### Folder: `temporary`

* No manual edits are ever done in this folder. It is just to save temporary results.

##### Folder: `results`

* In each `<telecom_alias>` folder in the `results` folder,
* All content of the `results` folder is synced with a WB OneDrive folder shared with the wider team
  * It is therefore important to not include any individual level data, or in any other way identifying data.
