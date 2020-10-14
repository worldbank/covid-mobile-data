
# Custom suffixes?
# Class??

EXPORT = False

#-----------------------------------------------------------------#
# Settings

import os
import re
import pandas as pd
import numpy as np
import datetime as dt

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
DATA_POC = DATA_path + "proof-of-concept/"

OUT_panel = DATA_POC + "panel_indicators/"


# CHANGE:
IRESULTS = DATA_path + "Isaac-results/"

IFLOW_path = IRESULTS + "flowminder/"
ICUST_path = IRESULTS + "custom/"

INEW_PATH_2_mar = IRESULTS + "Archive/e_17_06_2020_coverage_03_to_04/admin2_priority/mar1-mar31/"
INEW_PATH_2_apr = IRESULTS + "Archive/e_17_06_2020_coverage_03_to_04/admin2_priority/mar23-apr30/"

INEW_PATH_3_mar = IRESULTS + "Archive/e_17_06_2020_coverage_03_to_04/admin3_priority/mar1-mar31/"
INEW_PATH_3_apr = IRESULTS + "Archive/e_17_06_2020_coverage_03_to_04/admin3_priority/mar23-apr30/"


IOLD_PATH_2_mar = IRESULTS + "custom/admin2/"
IOLD_PATH_3_mar = IRESULTS + "Archive/e_08_06_2020_coverage_04_to_05/admin3_custom/"


# Load list of internal indicators to make it
# easier to bulk load files
DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"

internal_indicators = pd.read_csv(DATA_POC + 'indicators_list.csv')
internal_indicators['path'] = DATA_path + internal_indicators['path']   

# Load files function
def loadfiles(file_name, 
              files_df = internal_indicators,
              admin = 3,
              path_external = None):
    if path_external is None:
        # Set intex
        idx = files_df[(files_df['file'] == file_name) & (files_df['level'] == admin)].index.values[0]    # Load internal
        # Custom file names for i5, i7 and i9
        if file_name in ['mean_distance_per_day', 
                        'origin_destination_connection_matrix_per_day',
                        'mean_distance_per_week',
                        'month_home_vs_day_location_per_day',
                        'week_home_vs_day_location_per_day']:
            file_name_i = file_name + '_7day_limit.csv'
        else:
            file_name_i = file_name + '.csv'
        # External names
        print(file_name, admin)
        # Load data
        d = None
        d = pd.read_csv(files_df['path'][idx] + file_name_i)
    else:
        print(file_name)
        file_name = file_name + '.csv'
        d = None
        d = pd.read_csv(path_external + file_name)
        # Patch clean of headers in the middle of the data
        c1_name = d.columns[0]
        d = d[~d[c1_name].astype(str).str.contains(c1_name)]
    # Turn everything to string for simplicity
    d.astype(str)
    return d

# i1 = loadfiles('transactions_per_hour')

# i1e = loadfiles('transactions_per_hour',
#                path_external= INEW_PATH_3_apr)

# Drop custom missigs
def drop_custna(data, columns):
    na_list = ['nan', '', '99999', float("inf")] 
    for cols in columns:
        data = data[~(data[cols].isin(na_list))]
    return(data)

# Clean function
def clean(d, index_cols):
    # Remove missins
    d = d.dropna()
    # All but the last column
    #index_cols = list(d.columns[0:-1])
    # d = drop_custna(d, index_cols)
    return(d)

#-----------------------------------------------------------------#
# Load indicators
i5_index = ['connection_date', 'region_from', 'region_to']

i5 = loadfiles('origin_destination_connection_matrix_per_day',
               admin = 2)
i5e_mar = loadfiles('origin_destination_connection_matrix_per_day',
                    path_external= INEW_PATH_2_mar)
i5e_apr = loadfiles('origin_destination_connection_matrix_per_day',
                    path_external= INEW_PATH_2_apr)

i7_index = ['home_region', 'day']
i7 = loadfiles('mean_distance_per_day', admin = 2)

# March files where only rerun for i5 and i9 so I'm using the old extraction from feb to apr
i7e_mar = loadfiles('mean_distance_per_day',
                    path_external= IOLD_PATH_2_mar)
i7e_apr = loadfiles('mean_distance_per_day',
                    path_external= INEW_PATH_2_apr)

#-----------------------------------------------------------------#
# Panel
# Create panel
def panel(d,
               de,
               index_cols,
               #countvars,
               r_suffix = '_ecnt',
               timevar = None,
               how = 'outer'):
    if timevar is None:
        timevar = index_cols[0]
    # MAke sure time var is date
    d[timevar] = d[timevar].astype('datetime64')    
    de[timevar] = de[timevar].astype('datetime64')    
    # Join
    md = d.merge(de,
                 on = index_cols, 
                 how = how,
                 suffixes=('', r_suffix))
    return md


d1_bol = (p7['day'] >= np.datetime64(dt.date(2020, 3, 15)))
d2_bol = (p7['day'] >= np.datetime64(dt.date(2020, 4, 1)))

#--------#
# i5 Panel
p5 = panel(i5, i5e_mar, i5_index, timevar = 'connection_date')
p5 = panel(p5, 
                 i5e_apr, 
                 i5_index,
                r_suffix= '_ecnt_apr', 
                 timevar = 'connection_date')

d1_bol = (p5['connection_date'] >= np.datetime64(dt.date(2020, 3, 15)))
d2_bol = (p5['connection_date'] >= np.datetime64(dt.date(2020, 4, 1)))


countvars =  ['subscriber_count','od_count', 'total_count']
for var in countvars:
    varname = var + '_p'
    # Base value as our indicator
    p5[varname] = p5[var]
    # Replace values based on dates
    p5.loc[d1_bol, varname] = p5.loc[d1_bol, var + '_ecnt'] 
    p5.loc[d2_bol, varname] = p5.loc[d2_bol, var + '_ecnt_apr'] 

p5 = p5.dropna(subset = ['connection_date']).sort_values(i5_index)

# p5.to_csv('C:/Users/wb519128/Desktop/i5_test.csv', index = False)

if EXPORT:
    p5.to_csv(OUT_panel + 'i5_admin2_temp.csv', index = False)

#--------#
# i7 Panel
p7 = panel(i7, i7e_mar, i7_index, timevar = 'day')
p7 = panel(p7,
            i7e_apr, 
            i7_index, 
            r_suffix= '_ecnt_apr', 
            timevar = 'day')


d1_bol = (p7['day'] >= np.datetime64(dt.date(2020, 3, 15)))
d2_bol = (p7['day'] >= np.datetime64(dt.date(2020, 4, 1)))

countvars =  ['mean_distance', 'stdev_distance']
for var in countvars:
    varname = var + '_p'
    # Base value as our indicator
    p7[varname] = p7[var]
    # Replace values based on dates
    p7.loc[d1_bol, varname] = p7.loc[d1_bol, var + '_ecnt'] 
    p7.loc[d2_bol, varname] = p7.loc[d2_bol, var + '_ecnt_apr'] 
    



p7 = p7.dropna(subset = ['day']).sort_values(i7_index)

# Export
if EXPORT:
    p7.to_csv(OUT_panel + 'i7_admin2_temp.csv', index = False)


# p7.to_csv('C:/Users/wb519128/Desktop/i7_test.csv', index = False)

