# Files for aggregation of CDR Data

## Workflow

### Folder structure

There are three top level folders. `new`, `standardized` and `results`. All of those folders will have a country code folder, `CC` in the graph below (where CC will be US for USA, SE for Sweden, etc.) The next folder level will be the name of the telecom provider. These two folder levels will be identical across all three top level folders. Details specific to those folders are described in sections below.

```
mnt
|- COVID19Data
|-- new
|--- CC
|---- telecom1
|---- telecom2
|-- standardized
|--- CC
|---- telecom1
|---- telecom2
|-- results
|--- CC
|---- telecom1
|----- tables
|---- telecom2
|----- tables
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

### Folder: `standardized`

**Locaton:**
* Proof-of-concept phase: `"/mnt/COVID19Data/proof-of-concept/standardized"`
* Large scale phase: `"/mnt/COVID19Data/standardized"`

**Content:**
* The first two layers for the `new` folder will be `standardized/CC/company`, where `CC` is country code and `company` is the name of the telecom company, identically to the `new` folder.
* The files in the `standardized` folder will be parquet files.
* In each `company` folder there will not be any subfolders, even if there are subfolders in the corresponding `company` folder in the `new` folder. All parquet files should be saved directly in the `company` folder.

### Task: _aggregation_

**Input:** `standardized`, **Output:** `results`
In the _aggregation_ tasks we do the following:

* The parquet files will be processed using spark to produce aggregate indicators
* Save the aggregated indicators in normal unpartitioned csv files in the `results` folder.

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
