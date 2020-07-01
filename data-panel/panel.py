#-----------------------------------------------------------------#
# CREATE PANEL
#-----------------------------------------------------------------#

import os
import re
import pandas as pd
import numpy as np
import datetime as dt

import seaborn as sns; sns.set()
from matplotlib import rcParams
import matplotlib.pyplot as plt

#-----------------------------------------------------------------#
# Settings 

EXPORT = False


#-----------------------------------------------------------------#
# Folder structure
DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
DATA_POC = DATA_path + "proof-of-concept/"
OUT_panel = DATA_POC + "panel_indicators/"

#-----------------------------------------------------------------#
# GLOBALS

#-------------------#
# Indicator dataframe

# Load list of indicators to make it easier to bulk load files
indicators_df = pd\
    .read_csv(DATA_POC + 'documentation/indicators_list.csv')

# Since sheet contains relative paths add path global
# to have absolute paths    
indicators_df['path'] = DATA_path + indicators_df['path']   
indicators_df['path_ecnt'] = DATA_path + indicators_df['path_ecnt']   

#-----------------------------------------------------------------#
# General functions

def clean(data, index_cols):
    na_list = [np.nan, '', '99999', 99999, float("inf")]
    data = data[~data[index_cols].isin(na_list).any(axis ='columns')]
    return(data)

# clean(i5_2.data_e_04, i5_2.index_cols)['region_from'].unique()

def out_merge)

#-----------------------------------------------------------------#
# Create indicator class

# Define indicator class that contains data for multiple extractions
class i_indicator:
    """
    This class contains information to load indicator files both
    from our original indicators and externally created ones.
    
    load() method loads both datasets
    clean() method removes missings from both datasets
    """
    def __init__(self, 
                 num, 
                 index_cols,
                 level = 3,
                 files_df = indicators_df):
        # self.file_name = file_name
        self.num = num
        self.index_cols = index_cols
        self.level = level
        self.files_df = files_df
        # Call methods when intializing
        self.load()
        self.clean()
    # Load files
    def load(self, full = True):
        idx = (self.files_df['indicator'] == str(self.num)) & (self.files_df['level'] == str(self.level))
        # Internal indicator
        folder = self.files_df['path'][idx].iat[0]
        file_name = self.files_df['file'][idx].iat[0] + '.csv'
        self.data = pd.read_csv(folder + file_name)
        # self.data = folder + file_name
        # External indicators
        if full:
            folder = self.files_df['path_ecnt'][idx].iat[0]
            file_name_03 = '2020_03_' + self.files_df['file'][idx].iat[0] + '.csv'
            file_name_04 = '2020_04_' + self.files_df['file'][idx].iat[0] + '.csv'
            file_name_05 = '2020_05_' + self.files_df['file'][idx].iat[0] + '.csv'
            file_name_06 = '2020_06_' + self.files_df['file'][idx].iat[0] + '.csv'
            self.data_e_03 = pd.read_csv(folder + file_name_03)
            self.data_e_04 = pd.read_csv(folder + file_name_04)
            self.data_e_05 = pd.read_csv(folder + file_name_05)
            # self.data_e_06 = pd.read_csv(folder + file_name_06)
    # Clean indicators
    def clean(self):
        self.data = clean(self.data, self.index_cols)
        self.data_e_03 = clean(self.data_e_03, self.index_cols)
        self.data_e_04 = clean(self.data_e_04, self.index_cols)
        self.data_e_05 = clean(self.data_e_05, self.index_cols)
    # Internal merge function
    # def out_merge(self, d1, d2, suffix, on = self.index_cols):
    #     return d1.merge(d2, on = on, how = 'outer', suffixes=('', suffix))
    
    # Create panel with other data sets being added as columns. Gambiarra braba !Arrumar!
    def create_panel(self, 
                     time_var, 
                     c_date_1 = np.datetime64(dt.date(2020, 3, 15)),
                     c_date_2 = np.datetime64(dt.date(2020, 4, 1)),
                     c_date_3 = np.datetime64(dt.date(2020, 5, 1)) ):
        # kwargs.setdefault('time_var', self.index_cols[0])
        self.panel = self.data\
            .merge(self.data_e_03,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_03'))\
            .merge(self.data_e_04,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_04'))\
            .merge(self.data_e_05,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_05'))
        # Create panel column
        d1_bol = (self.panel[time_var].astype('datetime64')  >= c_date_1)
        d2_bol = (self.panel[time_var].astype('datetime64')  >= c_date_2)
        d3_bol = (self.panel[time_var].astype('datetime64')  >= c_date_3)
        countvars =  list(set(self.data.columns) - set(self.index_cols))
        for var in countvars:
            varname = var + '_p'
            # Base value as our indicator
            self.panel[varname] = self.panel[var]
            # Replace values based on dates
            self.panel.loc[d1_bol, varname] = self.panel.loc[d1_bol, var + '_03'] 
            self.panel.loc[d2_bol, varname] = self.panel.loc[d2_bol, var + '_04']
            self.panel.loc[d3_bol, varname] = self.panel.loc[d3_bol, var + '_05']
        # Make sure order is fine
        # self.panel.sort_values(self.index_cols)          


# Indicator 1
# Count of observations - sum across all observations in the given hour and lowest admin 
# area.
i1 = i_indicator(num = 1,  index_cols = ['hour', 'region'])

# Indicator 3
# Count of observations - sum across all observations in the given hour and lowest admin 
# area.
i3 = i_indicator(num = 3,  index_cols = ['day', 'region'])

i3_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2)



# Indicator 5
# Origin Destination Matrix - trips between two regions
i5 = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 3)

i5_2 = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 2)



# Indicator 7
# Mean and Standard Deviation of distance traveled (by home location)
i7 = i_indicator(num = 7,  index_cols =['day','home_region'], level = 3)

i7_2 = i_indicator(num = 7,  index_cols =['day','home_region'], level = 2)

# Indicator 9
# Daily locations based on Home Region with average stay time and SD of stay time
i9 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 3)
i9_2 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 2)


#-----------------------------------------------------------------#
# Panel

# Create for all these indicators for now to check defaults

i1.create_panel(time_var = 'hour')
i5.create_panel(time_var = 'connection_date')
i5_2.create_panel(time_var = 'connection_date')

i3_2.create_panel(time_var = 'day')

i7.create_panel(time_var = 'day')
i7_2.create_panel(time_var = 'day')
i9.create_panel(time_var = 'day')
i9_2.create_panel(time_var = 'day')


i5.panel.sort_values(i5.index_cols)
i3_2.panel.sort_values(i3_2.index_cols)

#-----------------------------------------------------------------#
# Export
if EXPORT:
    i1.panel.sort_values(i1.index_cols).to_csv(OUT_panel + 'i1_admin3.csv', index = False)
    i3_2.panel.sort_values(i3.index_cols).to_csv(OUT_panel + 'i3_admin2.csv', index = False)
    i5.panel.sort_values(i5.index_cols).to_csv(OUT_panel + 'i5_admin3.csv', index = False)
    i5_2.panel.sort_values(i5.index_cols).to_csv(OUT_panel + 'i5_admin2.csv', index = False)
    i7.panel.sort_values(i7.index_cols).to_csv(OUT_panel + 'i7_admin3.csv', index = False)
    i7_2.panel.sort_values(i7.index_cols).to_csv(OUT_panel + 'i7_admin2.csv', index = False)
    i9.panel.sort_values(i9.index_cols).to_csv(OUT_panel + 'i9_admin3.csv', index = False)
    i9_2.panel.sort_values(i9.index_cols).to_csv(OUT_panel + 'i9_admin2.csv', index = False)