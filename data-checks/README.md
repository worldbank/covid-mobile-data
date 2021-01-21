# Data Checks

This folder contains code for running basic checks of aggregated CDR indicators. The data quality checks are intended to achieve the following:
1. **Ensure the data is complete.** This means that there are no missing values in two main dimensions: spatial-all admin areas should have data; and temporal: all time slots (month, day and hour) should have data. This check is required for all indicators.
2. **Cell tower down checks**. This is a special type of missing data where the data may be missing due to cell tower. This check is required for all indicators?
3. **Consistency checks**. This check can be done for a single indicator to check for several things. But it can also be done cross indicators to ensure consistency of total numbers.

## Requirements

- Python3
- pandas
- numpy
- plotly

## Basic usage:

```bash
$ git clone git@github.com:worldbank/covid-mobile-data.git
$ cd covid-mobile-data/data-checks/
$ python checker.py   --Path path/to/indicators
                    [--prefix "your_prefix_"]
                    [--outputs path/to/outputs]
```

## Custom usage usage:
You can create an instance of the checker class to customize any of the default values.

```python
from checker import *

check = checker(path = 'path/to/indicaotrs',
                outputs_path = 'path/to/outputs',
                level = 'subfolder',
                ind_dict = {
                 'i1' : 'transactions_per_hour.csv',
                 'i5':  'origin_destination_connection_matrix_per_day.csv'},
                prefix = 'your_prefix_',
                col_names_dict = {
                    'i1_col_names': {'Time':'hour', 'Geography':'region' 'Count':'count'},
                    'i2_col_names': {'Time':'hour','Geography':'region' 'Count':'count'},
                    'i3_col_names': {'Time':'day','Geography':'region' 'Count':'count'},
                    'i4_col_names': ('day','count','percent_active'),
                    'i5_col_names': {'Time':'connection_date' 'Geography_01':'region_from''Geography_02':'region_to',
                                    'Subcrib_Count':'subscriber_count',
                                     'OD_Count':'od_count','Total_Count':'total_count'},
                    'i6_col_names': ('week','home_region','count'),
                    'i7_col_names': ('home_region','day','mean_distance','stdev_distance'),
                    'i8_col_names': ('home_region','week','mean_distance','stdev_distance'),
                    'i9_col_names': ('day','region','home_region','mean_duration','stdev_duration','count'),
                    'i10_col_names': ('day','region',
                    'region_lag','total_duration_destination',
                                      'avg_duration_destination',
                                      'count_destination' 'stddev_duration_destination'
                                     'total_duration_origin','avg_duration_origin' 'count_origin',
                                      'stddev_duration_origin'),
                    'i11_col_names': ('month','home_region','count')
                }))
```