# Files for aggregation of CDR Data

## Workflow Tasks

The work flow in the CDR aggregation is separated into two main tasks (_standardization_ and _aggregation_) but those tasks needs to be preceded by the setup task.

### Task: _set-up_
**Output:** `new`

This is a manual tasks where the folloowing steps needs to be completed:
  1. Set up folders. Set up the `new`, `standardized`, `results` and `support_data` according to the folder structure section below.
  1. Copy all the raw anonymized CDR data to the `<base_path>/new/<country_code>/<telecom_alias>` folder. See folder structure section for details.
  1. Set up configuration object for the DataSource class used to standardize data. See the DataSource class section below.
#### DataSource class and config file

The [DataSource](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/modules/DataSource.py) class reads raw pseudonymized CDR data using a config dict as the only constructor argument. We recommend setting up the config dict by adapting the [config_file_template](https://github.com/worldbank/covid-mobile-data/blob/master/covid-mobile-data/cdr-aggregation/config_file_template.py).

###### Required parameters

* **base_path** `<class 'str'>`: The top data folder, where all data sub-folders for new data, standardized data, and results go. See folder structure for more details
* **country_code** and **telecom_alias** `<class 'str'>`: Within each data sub-folder, folders are organized after country and telecom, in case one setup is used for multiple countries or telecoms. See folder structure for more details
* **filestub** `<class 'str'>`: The name stub that will be used for all files generated, for example `<filestub>.parquet`
* **shapefiles** `<class list>`: A list of strings - SEBASTIAN
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
* **geofiles** `<class dict>`: - SEBASTIAN, how would you describe these?
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
  1. Then you can run the [aggregation_notebook](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/aggregation_notebook.py) in your spark cluster creating aggregates from your data.

### Task: _standardization_

**Input:** `new`, **Output:** `standardized`

In the _standardization_ tasks we do the following:
* Decompress any files in `new/CC/company` compressed with compressing formats that cannot be read on the fly, for example `.zip`.
* Read all files `new/CC/company` - done differently based on file format (`.csv`, `.txt`)
* Standardize column names and formats like this:

| column_name | format | example | description |
|---|---|---|---|
| msisdn | int | 123456789 | ID in call record (may never be a direct identifier like phone number) |
| call_datetime | time stamp | 2020-02-05 15:37:38 | The date and time of the record |
| location_id | string | ABC12345 | Tower ID or other location ID |
| call_date | date | 2020-02-05 | The date of the record - deducted form call_datetime |

* Then the files should be saved in `standardized/CC/company` in parquet format.

### Task: _aggregation_

**Input:** `standardized` and `support_data`, **Output:** `results`

In the _aggregation_ tasks we do the following:

* The parquet files will be processed using spark to produce aggregate indicators
* Save the aggregated indicators in normal unpartitioned csv files in the `results` folder.

## DataSource class

To facilitate the standardization task we have created a class called [DataSource](https://github.com/worldbank/covid-mobile-data/blob/master/cdr-aggregation/notebooks/modules/DataSource.py). This class reads raw anonymized CDR data, and use a configuration JSON object to output standardized parquet files ready for aggregation. The only argument needed when creating the class is a JSON style dict where some values are required and some values are optional and have default values.

#### Required parameters
```
{
  "base_path": "",
  "country_code": "",
  "telecom_alias": "",
  "filestub":"",
  "schema" : ""
}
```
* **base_path**, **country_code** and **telecom_alias**: Parts of the folder paths. See the folder path graph below.
* **filestub**: The name stub that will be used for all files generated, for example `<filestub>.parquet`.
* **schema**: The [spark schema](https://spark.apache.org/docs/latest/sql-reference.html) with the data types and column names. See example below:

Schema example. The order of the `StructField()` should match the order of the columns in the raw data files. The _msisdn_ values should only use alternative mock _msisdn_ vlaues. The _call_datetime_ column should be imported as string and will be casted to TimestampType using the optional `load_datemask` parameter (see below).
```
schema = StructType([
  StructField("msisdn", IntegerType(), True),
  StructField("call_datetime", StringType(), True),
  StructField("location_id", StringType(), True)
])
```

#### Optional parameters (and their default values)
```
{
  "data_paths" : ["*csv.gz","*csv"],
  "load_seperator":",",
  "load_header":"false",
  "load_mode":"PERMISSIVE",
  "load_datemask":"dd/MM/yyyy HH:mm:ss"
}
```
* **data_paths**: Indicates the file paths (starting from `<base_path>/new/<country_code>/<telecom_alias>`) and file formats for the files that should be loaded and outputted in the standardized parquet file. Default is all `.csv.gz` and `.csv` files immediately in the `<telecom_alias>` folder.
* **load_seperator**: The delimiter used in the raw data files. Default is a comma - `,`.
* **load_header** : Whether the raw data files has column names in the first row. Default is false (that they do not have column names in the first row).
* **load_mode**: How will rows that does not fit the schema be handled? Default is `PERMISSIVE` where the record is loaded as good as possible and any errors will happen downstream. Alternatives are `DROPMALFORMED` where those records are skipped, and `FAILFAST` where the rest of the specific spark job loading the file is interupted.
* **load_datemask**: The datestring mask that will be used when casting the datestring into a timestamp. The default is `dd/MM/yyyy HH:mm:ss`

The DataSource class has a method called `show_config()` where a summary of the configuaration of a class is showed in a readable format. See example below:

```
#Set up the datasource object, and show the config settings
ds = DataSource(config_object)
ds.show_config()
```

## Folder structure

There are four top level folders. `new`, `standardized`, `results` and `support_data`. All of those folders will have a country code folder, `country_code` in the graph below (where the country code would be US for USA, SE for Sweden, etc.) The next folder level will be the name or alias of the telecom provider. These two folder levels will be identical across all three top level folders. Details specific to those folders are described in sections below.

```
base_path
|-- new
| |-- country_code
|   |-- telecom_alias1
|   |-- telecom_alias2
|-- standardized
| |-- country_code
|   |-- telecom_alias1
|   |-- telecom_alias2
|-- results
| |-- country_code
|   |-- telecom_alias1
|   |-- telecom_alias2
|-- support_data
| |-- country_code
|   |-- telecom_alias1
|   |-- telecom_alias2
```

### Folder: `new`

**Locaton:**
* Proof-of-concept phase: `"/mnt/COVID19Data/proof-of-concept/new"`
* Large scale phase: `"/mnt/COVID19Data/new"`

**Content:**
* The first two layers for the `new` folder will be `new/CC/company`, where `CC` is country code and `company` is the name of the telecom company. There will be one `CC` folder per country participating, and one `company` folder for each telecom company participating in that country.
  * If a multinational company is participating, there will be one folder per company in each country folder.
* In each `company` folder, the files should be pulled or pushed from MNOs exactly as they are received.
  * If the telecom company used subfolders when submitting their data, then keep them as is

### Folder: `standardized`

**Locaton:**
* Proof-of-concept phase: `"/mnt/COVID19Data/proof-of-concept/standardized"`
* Large scale phase: `"/mnt/COVID19Data/standardized"`

**Content:**
* The first two layers for the `new` folder will be `standardized/CC/company`, where `CC` is country code and `company` is the name of the telecom company, identically to the `new` folder.
* The files in the `standardized` folder will be parquet files.
* In each `company` folder there will not be any subfolders, even if there are subfolders in the corresponding `company` folder in the `new` folder. All parquet files should be saved directly in the `company` folder.

### Folder: `results`

**Locaton:**
* Proof-of-concept phase: `"/mnt/COVID19Data/proof-of-concept/results"`
* Large scale phase: `"/mnt/COVID19Data/results"`

**Content:**
* The first two layers for the `new` folder will be `results/CC/company`, where `CC` is country code and `company` is the name of the telecom company, identically to the `new` folder.
* In each `company` folder there are the following subfolders (currently just one, but more may be added if we have more types of outputs):
  * A subfolder called `tables` where the aggregated data is saved in normal unpartitioned csv files
* All content of the `results` folder is synced with a WB OneDrive folder shared with the wider team
  * It is therefore important to not include any individual level data, or in any other way identifying data.
