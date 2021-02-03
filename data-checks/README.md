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

## Custom usage:
You can create an instance of the checker class to customize any of the default values.

```python
from checker import *

check = checker(path = 'path/to/indicaotrs',
                outputs_path = 'path/to/outputs',
                level = 'subfolder',
                ind_dict = {'i1' : 'transactions_per_hour.csv',
                            'i3' : 'unique_subscrivers_per_day.csv',
                            'i5' :  'origin_destination_connection_matrix_per_day.csv'},
                prefix = 'your_prefix_',
                col_names_dict = col_names_dict = {
                    'i1': {'Time':'hour', 
                           'Geography':'region',
                           'Count':'count'},
                    'i3': {'Time':'day',
                           'Geography':'region',
                           'Count':'count'},
                    'i5': {'Time':'connection_date',
                           'Geography_from':'region_from',
                           'Geography_to':'region_to',
                           'Count':'total_count'} })
```
