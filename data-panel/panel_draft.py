#-----------------------------------------------------------------#
# CREATE PANEL
#-----------------------------------------------------------------#

# This file combines two different sources of the indicators created
# in cdr-aggregation to create a panel.

# Dates at which different sources are connected are specific to 
# each indicator and the particularities of those sources

#-----------------------------------------------------------------#
# TO DO

# Rewrite load function

# Reorganize file paths to remove dependency on MASTER.py

#-----------------------------------------------------------------#
# Settings

import os
import re
import pandas as pd
import numpy as np
import datetime as dt

#-----------------------------------------------------------------#
# Globals

# Default connection date. 
append_date = dt.date(2020, 3, 15)

#-----------------------------------------------------------------#
# Function definitions

# Drop custom missigs
def drop_custna(data, columns):
    na_list = ['nan', '', '99999',  float("inf")] 
    for cols in columns:
        data = data[~(data[cols].isin(na_list))]
    return(data)

# Load files function
def loadfiles(file_name, 
              files_df = internal_indicators,
              admin = 3):
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
    file_name_e = file_name + '.csv'
    print(file_name, admin)
    # Load data
    d = None
    d = pd.read_csv(files_df['path'][idx] + file_name_i)
    # Load external
    if files_df['indicator'][idx] == 'flow':
        ext_path = IFLOW_path
    else:
        ext_path = ICUST_path
    # Load external file
    ext_folder = ext_path + 'admin' + str(files_df['level'][idx]) + '/' 
    de = None
    de = pd.read_csv(ext_folder + file_name_e)
    # Patch cleannig of headers in the middle of the data
    c1_name = d.columns[0]
    de = de[~de[c1_name].astype(str).str.contains(c1_name)]    
    return([d, de])

# Clean function
def clean(d, index_cols):
    # Remove missins
    d = d.dropna()
    # All but the last column
    #index_cols = list(d.columns[0:-1])
    d = drop_custna(d, index_cols)
    return(d)

# Create panel
def simp_panel(d,
               de,
               index_cols,
               #countvars,
               append_date,
               compare = False,
               timevar = None,
               how = 'outer'):
    if timevar is None:
        timevar = index_cols[0]
    # Clean
    d = clean(d, index_cols)
    de = clean(de, index_cols)
    # Join
    md = d.merge(de,
                 on = index_cols, 
                 how = how,
                 suffixes=('', '_ecnt'))
    # Replace count values with internal until the 7th of march and 
    # external after
    countvars =  list(set(d.columns) - set(index_cols))
    for var in countvars:
        if compare:
            varname = var + '_p'
        else:
            varname = var
        
        md[varname] = np.where(pd.to_datetime(md[timevar]).dt.date <= append_date, 
                   md[var], 
                   md[var + '_ecnt'])
    # Remove other columns
    if not compare:
        md = md.filter(regex=r'^((?!_ecnt).)*$')
    # Return
    return md.sort_values(index_cols).dropna(subset= index_cols)

#-----------------------------------------------------------------#
# Load indicators

# Define indicator class that 
class i_indicator:
    """
    This class contains information to load indicator files both
    from our original indicators and externally created ones.
    
    load() method loads both datasets
    clean() method removes missings from both datasets
    """
    def __init__(self, 
                 file_name, 
                 index_cols,
                 admin = 3):
        self.file_name = file_name
        self.index_cols = index_cols
        self.admin = admin
        # Call methods when intializing
        self.load()
        self.clean()
    # Load data
    def load(self):
        self.data, self.data_e = loadfiles(self.file_name,
                                           admin = self.admin)
    # Clean data
    def clean(self):
        self.data = clean(self.data, self.index_cols)
        self.data_e = clean(self.data_e, self.index_cols)
    
    # Create panel data
    def create_panel(self,
                    timevar = None,
                    compare = False,
                    append_date = append_date):
        panel = simp_panel(self.data,
                           self.data_e, 
                           self.index_cols,
                           append_date,
                           compare = compare,
                           timevar=timevar)
        return panel

# Indicator 1
# 	Sum across all observations in the given hour and lowest admin 
# area.
i1 = i_indicator('transactions_per_hour', 
                 ['hour', 'region'])

# Indicator 2
# Sum all unique subscribers with an observation in the given 
# admin area and time period.
i2 = i_indicator('unique_subscribers_per_hour',
                 ['hour', 'region'])


# Indicator 3
# Sum all unique subscribers with an observation in the given 
# admin area and time period.
i3 = i_indicator('unique_subscribers_per_day',
                 ['day', 'region'])

# Indicator 4
# i4 = i_indicator('percent_of_all_subscribers_active_per_day',
#                  ['home_region', 'day'])

# Indicator 5
i5 = i_indicator('origin_destination_connection_matrix_per_day',
                 ['connection_date', 'region_from', 'region_to'])
# Indicator 7 
i7 = i_indicator('mean_distance_per_day',
                 ['home_region', 'day'])

# Indicator 8
i8 = i_indicator('mean_distance_per_week',
                 ['home_region', 'week'])

# Indicator 9
i9 = i_indicator('week_home_vs_day_location_per_day',
                 ['region', 'home_region', 'day'],
                 admin = 2)
 
#-----------------------------------------------------------------#
# Create panel 

# Make particular changes to  indicators as needed here

# Panel with defaults
i_list = [i1, i2, i3, i5, i9]
panel_list = list(map(lambda x: x.create_panel() , i_list)) 

# Custom arguments
i7_p = i7.create_panel( timevar = 'day')

#-----------------------------------------------------------------#
# Export 
 