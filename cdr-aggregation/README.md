# Aggregation of CDR Data

### Table of contents

* [Summary of indicators](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#summary-of-indicators)
* [Initial set-up](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#set-up)
  * [DataSource class and config file](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#datasource-class-and-config-file)
  * [Docker set-up](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#docker-set-up)
  * [Databricks/datalake set-up](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#databricksdatalake-set-up)
* [Workflow description](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#workflow-description)
* [Indicators computation](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#indicators-computation)
* [Folder structure](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#folder-structure)

## Summary of indicators

Here is a short summary of the key indicators produced, how they should be interpreted, and their limitations. It also contains indicators standard levels of aggregation as produced by our team, but these may vary depending on country context. 

### Indicator 1 - Count of observations

**Standard time aggregation:** Hour

**Standard geographic aggregation:** Lowest administrative level.

Sum of all transactions (calls or messages) made within an hour and ward 

**Analysis**

This indicator is a simple measure of transaction volume and can show variations in cell phone usage over time. It's main use is to help in potentially scaling other indicators in case there are sudden changes in usage patterns which could in turn affect the measures of other indicators.

### Indicator 2 - Count of unique subscribers

**Standard time aggregation:** Hour

**Standard geographic aggregation:** Lowest administrative level.

Number of unique subscriber IDs that made a transaction within an hour and region.

**Analysis:**

his indicator is a proxy for population and can be used to asses changes in population density. The hourly nature of the indicator is more conducive to use in urban settings where there is higher phone usage and changes over the course of a day. This indicator is especially useful to identify areas that might experience sudden influxes of people (for example an area with a market), and therefore useful for identifying possible hotspots for spread of disease. 

**Caveats and limitations:** 

As variations are a function of usage, it can reflect changes in cellphone usage instead of changes in population. In areas with few towers, this indicator is unlikely to be useful since it will primarily capture phone useage rather than people going in and out of an area. or is a simple measure of transaction volume and can show variations in cell phone usage over time. It's main use is to help in potentially scaling other indicators in case there are sudden changes in usage patterns which could in turn affect the measures of other indicators.


### Indicator 3 - Count of unique subscribers	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Number of unique subscriber IDs that made a transaction within a day and region.

**Analysis:**

This indicator is a proxy for population and can be used to asses changes in population density. 							

**Caveats and limitations:** 

As variations are a function of usage, it can reflect changes in cellphone usage instead of changes in population. Given that it is highly dependent on phone usage, and how many subscribers are active on a given day, scaling by the total subscribers active in the country is one way to try to account for this.

### Indicator 4 - Ratio of residents active that day based on those present during baseline

**Standard time aggregation:** Day

**Standard geographic aggregation:** Country 

Ratio of residents active that day based on those present during baseline, set as at least a month before lockdwon measures to prevent the spread of SARS-CoV-2. Active users are defined as those that have made at least one transaction on that day, restricted to SIMs that had at least one observation during the baseline period.

**Analysis:**

A proxy to changes in cellphone usage through time that can be used to scale other indicators to account for days/locations where people may be less or more likely to use their phone.

**Caveats and limitations:** 

This indicator uses a weekly home location (see indicator 6), it may be difficult to distinguish changes in home location to changes in usage. Additionally, there is natural attrition that happens as some people stop using their SIM and also as new SIMs are added, and this is not able to take account of this natural fluctuation so it will bias the results to look like the active subscribers is slowly going down over time, so then if this is used to scale other variables, it will lead to errors. We tested several other ways of doing this indicator, and the best seemed to be to look across all users during the entire period of interest, and then look at how many of them are active on any given day, and count it based on home region in case there is variability by region in active users.

### Indicator 5 - Origin Destination Matrix - trips between two regions

**Standard time aggregation:** 

**Standard geographic aggregation:** Lowest administrative level and up. 


**Analysis:**


**Caveats and limitations:** 

### Indicator 6 - Number of residents

**Standard time aggregation:** Week

**Standard geographic aggregation:** Lowest administrative level.

Residency is defined as the modal location of the last observation on each day of the week. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific week.

**Analysis:**

It is a proxy for location of residency.  Weekly home locations may not capture the precise home location because people might make few calls in a week, so the location that is the mode may only have appeared on one or two days and may not represent the actual home location. Nevertheless, the weekly definition reflects the typical place a person is located in a given week and helps to capture if they deviate from the typical place. This is useful in situations like the announcement of a lockdown, when people might change their permanent location for some period of time during the lockdown, so this allows for an adjustment to reflect the new locations where people are located. Otherwise, if they are still assigned to their initial permanent home, it will look like there is a lot of movement and people not at home, but that is not contributing to risk necessarily if the person only moved once and then remained in the new location. This variable therefore helps to provide a flexible home location reflecting changing situations.

**Caveats and limitations:**

Due to the limited time dimension, weekly home location can be a poor measure for permanent residency depending on the phone usage, for example, if someone only has one or two observations a week. For permanent residency, usually a month or more of data would provide a better proxy, especially for infrequent users.

### Indicator 7 - Mean and Standard Deviation of distance traveled (by home location)	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Total distance travelled by each SIM in that day, aggregated by averaging across SIMs with the same home location. Distance is defined as the distance between tower centroids of the current transaction and that of the previous transaction.

**Analysis:**

Proxy for daily distance travelled. Since it looks at movement between tower clusters, it not only captures movement across administrative areas (as all other mobility indicators), but also movement within an administrative area if there are multiple tower clusters in it. Therefore, it can help to better account for the amount of mobility happening.

**Caveats and limitations:** 

Distances are a function of the mobile phone towers density. In a rural area where there are fewer towers with wide coverage, distances may increase in discrete jumps as moving within the coverage of a tower will not be detected. Additionally, moving within the area of a single tower will not be detected, so in areas with only a few towers, many people will look like they have a distance of 0. In contrast, in urban settings with multiple towers there is significantly more precision. For that reason, this variable might be more useful in urban, high tower-density areas

### Indicator 8 - Mean and Standard Deviation of distance traveled (by home location)	

**Standard time aggregation:** Week

**Standard geographic aggregation:** Lowest administrative level and up. 

Total distance travelled by each SIM in that week, aggregated by averaging across SIMs with the same home location. Distance is defined as the distance between tower centroids of the current transaction and that of the previous transaction.

**Analysis:**

Proxy for weekly distance travelled. Since it looks at movement between tower clusters, it not only captures movement across administrative areas (as all other mobility indicators), but also movement within an administrative area if there are multiple tower clusters in it. Therefore, it can help to better account for the amount of mobility happening. Also, the weekly nature (compared to the daily) better helps to capture changes for individuals with less observations for whom no distance traveled might be captured when looking daily, but when looking weekly, we may see them traveling across areas.

**Caveats and limitations:** 

'Distances are a function of the mobile phone towers density. In a rural area where there are fewer towers with wide coverage, distances may increase in discrete jumps as moving within the coverage of a tower will not be detected. Additionally, moving within the area of a single tower will not be detected, so in areas with only a few towers, many people will look like they have a distance of 0. In contrast, in urban settings with multiple towers there is significantly more precision. For that reason, this variable might be more useful in urban, high tower-density areas.

### Indicator 9 - Daily locations based on Home Region with average stay time and SD of stay time	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Region where users spent most of the time in that day and duration of that stay. Time is defined as the difference between time of first call in reference region to the time of first call outside the region. The users are than aggregated by their home regoin and the region where they spent the most time that day. Users that do not have any observations that day are not included.

**Analysis:**

This variable is used for epidemiological Agent Based Modeling. It makes it possible to calculate the probability that an individual from a given home location is likely to be in their home location on a given day. This then allows for assigning agents in an ABM a probability of being in some other region outside of their home on any given day. Since the weekly home location definition is used, the focus is on movement away from the home happening in the given week. If monthly home location were used, it could look like many people have a probability of traveling on a given day, but actually they are now semi-permanently in a new location and do not represent a high risk because they are no longer moving after the initial move. 

**Caveats and limitations:** 

'This way of looking at mobility misses out on all the movement that might happen on a given day between different locations where the person only spends a little bit of time, which could be relevant for highly transmissable infectious diseases such as respiratory infections. If we try to look at the probability of being outside of the home location at any point that day, we end up in situations where for some admin areas, there's more than a 90% likelihood of going outside of the home area. For the Zimbabwe ABM, it was decided that this would overmeasure movement (this is especially true for people that live at the border of admin areas and might look like they are engaging in lots of movement outside of the home region, but actually their calls just are routed through different nearby towers that are within range of them), and so we went with the more conservative measure of having had to spend the majority of the day in a location.

### Indicator 10 - Simple Origin Destination Matrix - trips between consecutive in time regions with time	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Simple Origin Destination Matrix - Similar to indicator 5, but only counts consecutive trips. For example, the sequence [A, A, B, C, D] would result in the pairings [AA, AB,  BC,  CD]. It counts the number of times each pair appears and time spent at both origin and destination regions. It also includes consecutive trips across days up to 7 days.

**Analysis:**

It is a proxy for daily mobility across regions with more information on commuting patterns since it also contains information on the approximate duration of stay in a location. It can be used in two ways. First, it counts all the pairs of consecutive moves. So unlike indicator 5, in the situation of ABABABAB in a single day, it will count 7 different moves rather than just 2. Second, combined with the duration, it can help to calculate an estimate of imported disease incidence if combined with incidence data. If we know the incidence in all locations at a given point in time, we can multiply it times the average duration to calculate the probability of being infected and sum across the total number of people entering to calculate total imported incidence. The standard deviation can be used in cases where there is an incubation period and the average duration is less than the incubation period, to recreate the distribution and still count those that stay for a duration longer than the incubation period.

**Caveats and limitations:** 

Importantly, since the indicator only looks at consecutive movement, if someone is passing through a location on their way to their final destination, the link between the origin and final destination will be missed. This especially impacts longer travel and will underestimate longer distance travel and durations.  Also, as only movements associated with a cellphone transaction are observed, we miss movement happening when there are no observations from a given location and time comparisons can be biased if users change their cellphone usage behaviour.

### Indicator 11 - Number of residents

**Standard time aggregation:** Month

**Standard geographic aggregation:** Lowest administrative level and up. 

Residency is defined as the modal location of the last observation on each day of the month. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific week.

**Analysis:**

It is a proxy for location of permanent residency. It can help to demonstrate if there are more permanent shifts in the location of a population.

**Caveats and limitations:** 

It could fail to capture temporary relocations, for example, if people move to the country area to spend lockdown away from the city. Additionally, if a permanent relocation happens in the middle of a month, then within that month, for part of the month it will look like the person is constantly away from their permanent residence, when actually they have moved to a new one, this could lead to bias when calculating number of people away from home, especially if a large event causes a lot of people to relocate permanently. 



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
