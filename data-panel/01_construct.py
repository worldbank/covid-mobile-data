#-----------------------------------------------------------------#
# CREATE PANEL
#-----------------------------------------------------------------#

# This code creates panel datasets combinig different versions of 
# indicator files. 


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
CODE_path = "C:/Users/wb519128/GitHub/covid-mobile-data/data-panel/"

DATA_POC = DATA_path + "proof-of-concept/"
DATA_panel = DATA_POC + "panel_indicators/"
DATA_panel_raw = DATA_panel + 'raw/'
DATA_panel_clean = DATA_panel + 'clean/'

OUT_hfcs = DATA_path + "data-checks/"


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

i3.create_panel(time_var = 'day')
i3_2.create_panel(time_var = 'day')

i7.create_panel(time_var = 'day')
i7_2.create_panel(time_var = 'day')
i9.create_panel(time_var = 'day')
i9_2.create_panel(time_var = 'day')

i5.panel.sort_values(i5.index_cols)
i3.panel.sort_values(i3_2.index_cols)

#-----------------------------------------------------------------#
# Export
if EXPORT:
    i1.panel.sort_values(i1.index_cols).to_csv(DATA_panel_raw + 'i1_admin3.csv', index = False)
    i3.panel.sort_values(i3.index_cols).to_csv(DATA_panel_raw + 'i3_admin3.csv', index = False)
    i3_2.panel.sort_values(i3.index_cols).to_csv(DATA_panel_raw + 'i3_admin2.csv', index = False)
    i5.panel.sort_values(i5.index_cols).to_csv(DATA_panel_raw + 'i5_admin3.csv', index = False)
    i5_2.panel.sort_values(i5.index_cols).to_csv(DATA_panel_raw + 'i5_admin2.csv', index = False)
    i7.panel.sort_values(i7.index_cols).to_csv(DATA_panel_raw + 'i7_admin3.csv', index = False)
    i7_2.panel.sort_values(i7.index_cols).to_csv(DATA_panel_raw + 'i7_admin2.csv', index = False)
    i9.panel.sort_values(i9.index_cols).to_csv(DATA_panel_raw + 'i9_admin3.csv', index = False)
    i9_2.panel.sort_values(i9.index_cols).to_csv(DATA_panel_raw + 'i9_admin2.csv', index = False)
    
#-----------------------------------------------------------------#
# Further cleaning

# Create usage outliers files
exec(open(CODE_path + 'usage_outliers.py').read())

#-----------------------------------------------------------------#
# Further cleaning

def clean_columns(indicator, timevar):
    # Remove comparison columns
    keepcols = copy.deepcopy(indicator.index_cols)
    keepcols.extend(indicator.panel.filter(like='_p', axis=1).columns.to_list())
    new_df = indicator.panel[keepcols]
    # Rename columns
    new_df.columns = new_df.columns.str.strip('_p')
    # Create time variables
    new_df['date'] = pd.to_datetime(new_df[timevar]).dt.date
    return new_df

def remove_towers_down(df, region_vars, outliers_df = i1_ag_df_tower_down):
    # Process outliers file
    outliers_df = copy.deepcopy(i1_ag_df_tower_down) # created in usage_outliers.py
    outliers_df = outliers_df\
        .drop(['hcount', 'avg_hours', 'h_diff'], axis = 1)\
        .rename(columns = {'region':'region_right'})
    outliers_df['flag'] = 1
    # Merge outliers
    if len(region_vars) == 1:
        new_df = df\
            .merge(outliers_df,
                        left_on = ['date', region_vars[0]],
                        right_on = ['date', 'region_right'],
                        how = 'outer')\
            .drop(['region_right'], axis = 1)
    else:
        new_df = df\
            .merge(outliers_df,
                        left_on = ['date', region_vars[0]],
                        right_on = ['date', 'region_right'],
                        how = 'outer')\
            .drop(['region_right'], axis = 1)\
            .merge(outliers_df,
                        left_on = ['date', region_vars[1]],
                        right_on = ['date', 'region_right'],
                        how = 'outer')\
            .drop(['region_right'], axis = 1)
        # Flag if either is true
        new_df['flag'] = ((new_df['flag_x'] == 1) | (new_df['flag_y'] == 1)).astype(int)
        new_df = new_df.drop(['flag_x', 'flag_y'], axis =1)
    # Drop outliers and processual columns
    new_df = new_df[~(new_df['flag'] == 1)].drop(['flag'], axis = 1)
    return new_df

def clean_pipeline(indicator, timevar, region_vars):
    return remove_towers_down( 
                       clean_columns(indicator, 
                                     timevar = timevar), 
                       region_vars = region_vars)
    

i1_cl_panel = clean_pipeline(i1,timevar = 'hour', region_vars = ['region'])
i3_cl_panel = clean_pipeline(i3, timevar = 'day', region_vars = ['region'])
i3_2_cl_panel = clean_columns(i3, timevar = 'day')
i5_cl_panel = clean_pipeline(i5,timevar = 'connection_date', region_vars = ['region_from', 'region_to'])
i5_2_cl_panel = clean_columns(i5, timevar = 'connection_date')
i7_cl_panel = clean_pipeline(i7,timevar = 'day', region_vars = ['home_region'])
i7_2_cl_panel = clean_columns(i7, timevar = 'day')
i9_cl_panel = clean_pipeline(i9,timevar = 'day', region_vars = ['region'])
i9_2_cl_panel = clean_columns(i9, timevar = 'day')

if EXPORT:
    i1_cl_panel.to_csv(DATA_panel_clean + 'i1_admin3.csv', index = False)
    i3_cl_panel.to_csv(DATA_panel_clean + 'i3_admin3.csv', index = False)
    i3_2_cl_panel.to_csv(DATA_panel_clean + 'i3_admin2.csv', index = False)
    i5_cl_panel.to_csv(DATA_panel_clean + 'i5_admin3.csv', index = False)
    i5_2_cl_panel.to_csv(DATA_panel_clean + 'i5_admin2.csv', index = False)
    i7_cl_panel.to_csv(DATA_panel_clean + 'i7_admin3.csv', index = False)
    i7_2_cl_panel.to_csv(DATA_panel_clean + 'i7_admin2.csv', index = False)
    i9_cl_panel.to_csv(DATA_panel_clean + 'i9_admin3.csv', index = False)
    i9_2_cl_panel.to_csv(DATA_panel_clean + 'i9_admin2.csv', index = False)

