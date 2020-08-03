#-----------------------------------------------------------------#
# CREATE PANEL
#-----------------------------------------------------------------#

# This code creates panel datasets combinig different versions of 
# indicator files. 


import os
import re
import copy
import pandas as pd
import numpy as np
import datetime as dt

import seaborn as sns; sns.set()
from matplotlib import rcParams
import matplotlib.pyplot as plt
from itertools import chain

# Import functions.
# This assumes the script is running from the folder where both files are 
from utils import *

#-----------------------------------------------------------------#
# Settings 

EXPORT = True

#-----------------------------------------------------------------#
# Folder structure

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
CODE_path = "C:/Users/wb519128/GitHub/covid-mobile-data/data-panel/"

DATA_POC = DATA_path + "proof-of-concept/"
DATA_panel = DATA_POC + "panel_indicators/"
# DATA_panel_raw = DATA_panel + 'raw/'
DATA_panel_comp = DATA_panel + 'comparisson/'
DATA_panel_clean = DATA_panel + 'clean/'

OUT_hfcs = DATA_POC + "outputs/data-checks/"


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

#-------------------#
# Set default values
default_levels_dict = {1: [3], 
                       2: [3],
                       3: [2,3],
                    #    4: ['country'],
                       5: [2,3,'tc_harare', 'tc_bulawayo'],
                       6: [3],
                       7: [2,3],
                       8: [2,3],
                       9: [2,3],
                    #    9: [2,3,'tower-cluster-harare', 'tower-cluster-bulawayo'],
                       10: [2,3],
                       11: [2,3]}

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
                 time_var = None,
                 region_vars = None,
                 level = 3,
                 files_df = indicators_df):
        # self.file_name = file_name
        self.num = num
        self.index_cols = index_cols
        self.level = level
        self.files_df = files_df
        # Set defaults for time and regions
        if time_var is None:
            self.time_var = self.index_cols[0]
        if region_vars is None:
            self.region_vars = self.index_cols[1:]
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
            self.data_e_06 = pd.read_csv(folder + file_name_06)
    # Clean indicators
    def clean(self):
        self.data = clean(self.data, self.index_cols)
        self.data_e_03 = clean(self.data_e_03, self.index_cols)
        self.data_e_04 = clean(self.data_e_04, self.index_cols)
        self.data_e_05 = clean(self.data_e_05, self.index_cols)
        self.data_e_06 = clean(self.data_e_06, self.index_cols)
    # Internal merge function
    # def out_merge(self, d1, d2, suffix, on = self.index_cols):
    #     return d1.merge(d2, on = on, how = 'outer', suffixes=('', suffix))
    
    # Create panel with other data sets being added as columns. Gambiarra braba !Arrumar!
    def create_panel(self, 
                    #  time_var, 
                     c_date_1 = np.datetime64(dt.date(2020, 3, 15)),
                     c_date_2 = np.datetime64(dt.date(2020, 4, 1)),
                     c_date_3 = np.datetime64(dt.date(2020, 5, 1)),
                     c_date_4 = np.datetime64(dt.date(2020, 6, 1)) ):
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
                   suffixes=('', '_05'))\
            .merge(self.data_e_06,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_06'))
        # Create panel column
        d1_bol = (self.panel[self.time_var].astype('datetime64')  >= c_date_1)
        d2_bol = (self.panel[self.time_var].astype('datetime64')  >= c_date_2)
        d3_bol = (self.panel[self.time_var].astype('datetime64')  >= c_date_3)
        d4_bol = (self.panel[self.time_var].astype('datetime64')  >= c_date_4)
        countvars =  list(set(self.data.columns) - set(self.index_cols))
        for var in countvars:
            varname = var + '_p'
            # Base value as our indicator
            self.panel[varname] = self.panel[var]
            # Replace values based on dates
            self.panel.loc[d1_bol, varname] = self.panel.loc[d1_bol, var + '_03'] 
            self.panel.loc[d2_bol, varname] = self.panel.loc[d2_bol, var + '_04']
            self.panel.loc[d3_bol, varname] = self.panel.loc[d3_bol, var + '_05']
            self.panel.loc[d4_bol, varname] = self.panel.loc[d3_bol, var + '_06']
    # Replace panel attribute with clean panel
    def create_clean_panel(self, 
                        #    time_var, 
                        #    region_vars, 
                           outliers_df):
        if self.level == 3:
            self.panel = clean_pipeline(self, self.time_var, self.region_vars, outliers_df)
        else:
            self._panel = clean_columns(self, self.time_var)
    
    # Set a saving method
    def save(self, path):
        self.panel.sort_values(self.index_cols).to_csv(path, index = False)

#-----------------------------------------------------------------#
# Constructor class

# Define panel constructor class
class panel_constructor:
    """
    This class contains loads files for specified indicators and creates
    panel data sets combining all the files 
    """
    def __init__(self, 
                 ilevels_dict = default_levels_dict):
        self.ilevels_dict = ilevels_dict
        # List all indicators loaded flattening dictionary of inficators and levels
        i_list = []
        for i in self.ilevels_dict.keys():
            i_list.append(['i' + str(i) + '_' + str(y) for y in self.ilevels_dict[i]] )
        self.i_list = list(chain.from_iterable(i_list))
        
        # Load indicators:
        # 1. Transactions per hour - Always created since it is needed for usage outliers
        self.i1_3 = i_indicator(num = 1,  index_cols = ['hour', 'region'])
        
        # 2. Unique subscribers per hour
        if 2 in self.ilevels_dict.keys():
            self.i2_3 = i_indicator(num = 2,  index_cols = ['hour', 'region'])
        
        # 3. Unique subscribers per day 
        if 3 in self.ilevels_dict.keys(): 
            if 3 in self.ilevels_dict[3]:
                self.i3_3 = i_indicator(num = 3,  index_cols = ['day', 'region'])
        if 3 in self.ilevels_dict.keys(): 
            if 2 in self.ilevels_dict[3]:
                self.i3_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2)
        
        # 4. Proportion of active subscribers
        # if 4 in self.ilevels_dict.keys():
        #     self.i4
        
        # 5 - Connection Matrix
        if 5 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[5]:
                self.i5_3 = i_indicator(num = 3,  index_cols = ['day', 'region'])
            if 2 in self.ilevels_dict[5]:
                self.i5_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2)
            if 'tc_harare' in self.ilevels_dict[5]:
                self.i5_tc_harare = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_harare')
            if 'tc_bulawayo' in self.ilevels_dict[5]:  
                self.i5_tc_bulawayo = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_bulawayo')
        
        # 6. Unique subscribers per home location
        if 6 in self.ilevels_dict.keys():
            self.i6_3 = i_indicator(num = 6,  index_cols = ['week', 'home_region'])
        
        # 7. Mean and Standard Deviation of distance traveled per day (by home location)
        if 7 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[7]:
                self.i7_3 = i_indicator(num = 7,  index_cols =['day','home_region'], level = 3)
            if 2 in self.ilevels_dict[7]:
                self.i7_2 = i_indicator(num = 7,  index_cols =['day','home_region'], level = 2)
        
        # 8. Mean and Standard Deviation of distance traveled per week (by home location)
        if 8 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[8]:
                self.i8_3 = i_indicator(num = 8,  index_cols =['week','home_region'], level = 3)
            if 2 in self.ilevels_dict[8]:
                self.i8_2 = i_indicator(num = 8,  index_cols =['week','home_region'], level = 2)
       
        # 9. Daily locations based on Home Region with average stay time and SD of stay time
        if 9 in self.ilevels_dict.keys(): 
            if 3 in self.ilevels_dict[9]:
                self.i9_3 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 3)
            if 2 in self.ilevels_dict[9]:
                self.i9_2 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 2)
        
        # 10. Simple OD matrix with duration of stay
        if 10 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[10]:
                self.i10_3 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 3)
            if 2 in self.ilevels_dict[10]:
                self.i10_2 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 2)
        
        # 11. Monthly unique subscribers per home region
        if 11 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[11]:
                self.i11_3 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 3)
            if 2 in self.ilevels_dict[11]:
                self.i11_2 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 2)
    # Create comparisson panel for all loaded indicators
    def dirty_panel(self):
        for i in self.i_list:
            getattr(self, i).create_panel()
            print('Created comp. panel ' + i)
        # self.i1_3.create_panel()
    # Create clean panel for all loaded indicators
    def clean_panel(self, outliers_df):
        for i in self.i_list:
            getattr(self, i).create_clean_panel(outliers_df = outliers_df)
            print('Created clean panel ' + i)
        # i1.create_clean_panel(outliers_df = outliers_df)
    # Export panel datasets for all loaded indicators
    def export(self, path):
        for i in self.i_list:
            exp_path = path + i + '.csv'
            getattr(self, i).save(path = exp_path)
            print('Saved ' + exp_path )


#-----------------------------------------------------------------#
# Load indicators and create comparisson "dirty" panel

# indicators = panel_constructor(ilevels_dict = {1: [3],
#                                                5: [2,3,'tc_harare', 'tc_bulawayo']})

# If no levels dictionary is provided, it will use the default, which is all of them!
indicators = panel_constructor()

indicators.dirty_panel()

#-----------------------------------------------------------------#
# Create usage outliers files

i1 = indicators.i1_3
exec(open(CODE_path + 'usage_outliers.py').read())


#-----------------------------------------------------------------#
# Export comparisson panel

if EXPORT:
    indicators.export(DATA_panel_comp)

#-----------------------------------------------------------------#
# Create clean panel

# This replaces the old panel attribute with the clean version, with
# standardized column names

indicators.clean_panel(i1_ag_df_tower_down)

#-----------------------------------------------------------------#
# Export
if EXPORT:
    indicators.export(DATA_panel_clean + 'test/')

