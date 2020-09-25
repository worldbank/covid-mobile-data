#-----------------------------------------------------------------#
# Panel construction classes
#-----------------------------------------------------------------#

import os
import re
import copy
import pandas as pd
import numpy as np
import datetime as dt

from itertools import chain

# Import functions.
# This assumes the script is running from the folder where both files are 
from utils import *

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
                 files_df,
                 time_var = None,
                 region_vars = None,
                 level = 3):
        # self.file_name = file_name
        self.num = num
        self.index_cols = index_cols
        self.level = level
        self.files_df = files_df
        # Set defaults for time and regions
        if time_var is None:
            self.time_var = self.index_cols[0]
        else:
            self.time_var = time_var
        if (region_vars is None) & (len(self.index_cols) > 1):
            self.region_vars = self.index_cols[1:]
        else:
            self.region_vars = region_vars
        # # Call methods when intializing
        self.load()
        self.clean()
    # Load files
    def load(self, full = True):
        idx = (self.files_df['indicator'] == self.num) & (self.files_df['level'] == str(self.level))
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
            self.panel = clean_columns(self, self.time_var)
    
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
                 ilevels_dict,
                 indicators_df):
        self.ilevels_dict = ilevels_dict
        self.indicators_df = indicators_df
        # List all indicators loaded flattening dictionary of inficators and levels
        i_list = []
        for i in self.ilevels_dict.keys():
            i_list.append(['i' + str(i) + '_' + str(y) for y in self.ilevels_dict[i]] )
        self.i_list = list(chain.from_iterable(i_list))
        
        # Load indicators:
        # 1. Transactions per hour - Always created since it is needed for usage outliers
        self.i1_3 = i_indicator(num = 1,  index_cols = ['hour', 'region'], files_df = self.indicators_df)
        
        # 2. Unique subscribers per hour
        if 2 in self.ilevels_dict.keys():
            self.i2_3 = i_indicator(num = 2,  index_cols = ['hour', 'region'], files_df = self.indicators_df)
        
        # 3. Unique subscribers per day 
        if 3 in self.ilevels_dict.keys(): 
            if 3 in self.ilevels_dict[3]:
                self.i3_3 = i_indicator(num = 3,  index_cols = ['day', 'region'], files_df = self.indicators_df)
        if 3 in self.ilevels_dict.keys(): 
            if 2 in self.ilevels_dict[3]:
                self.i3_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2, files_df = self.indicators_df)
        
        # 4. Proportion of active subscribers
        if 4 in self.ilevels_dict.keys():
            self.i4_country = i_indicator(num = 4,  index_cols = ['day'], level = 'country', files_df = self.indicators_df)
            
        # 5 - Connection Matrix
        if 5 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[5]:
                self.i5_3 = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], files_df = self.indicators_df)
            if 2 in self.ilevels_dict[5]:
                self.i5_2 = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 2, files_df = self.indicators_df)
            if 'tc_harare' in self.ilevels_dict[5]:
                self.i5_tc_harare = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_harare', files_df = self.indicators_df)
            if 'tc_bulawayo' in self.ilevels_dict[5]:  
                self.i5_tc_bulawayo = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_bulawayo', files_df = self.indicators_df)
        
        # 6. Unique subscribers per home location
        if 6 in self.ilevels_dict.keys():
            self.i6_3 = i_indicator(num = 6,  index_cols = ['week', 'home_region'], files_df = self.indicators_df)
        
        # 7. Mean and Standard Deviation of distance traveled per day (by home location)
        if 7 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[7]:
                self.i7_3 = i_indicator(num = 7,  index_cols =['day','home_region'], time_var = 'day', region_vars = ['home_region'], level = 3, files_df = self.indicators_df)
            if 2 in self.ilevels_dict[7]:
                self.i7_2 = i_indicator(num = 7,  index_cols =['day','home_region'], time_var = 'day', region_vars = ['home_region'], level = 2, files_df = self.indicators_df)
        
        # 8. Mean and Standard Deviation of distance traveled per week (by home location)
        if 8 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[8]:
                self.i8_3 = i_indicator(num = 8,  index_cols =['week','home_region'], time_var = 'week', region_vars = ['home_region'], level = 3, files_df = self.indicators_df)
            if 2 in self.ilevels_dict[8]:
                self.i8_2 = i_indicator(num = 8,  index_cols =['week','home_region'], time_var = 'week', region_vars = ['home_region'], level = 2, files_df = self.indicators_df)
       
        # 9. Daily locations based on Home Region with average stay time and SD of stay time
        if 9 in self.ilevels_dict.keys(): 
            if 3 in self.ilevels_dict[9]:
                self.i9_3 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 3, files_df = self.indicators_df)
            if 2 in self.ilevels_dict[9]:
                self.i9_2 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 2, files_df = self.indicators_df)
            if 'tc_bulawayo' in self.ilevels_dict[9]:
                self.i9_3 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 'tc_bulawayo', files_df = self.indicators_df)
            if 'tc_harare' in self.ilevels_dict[9]:
                self.i9_2 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 'tc_harare', files_df = self.indicators_df)
        
        # 10. Simple OD matrix with duration of stay
        if 10 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[10]:
                self.i10_3 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 3, files_df = self.indicators_df)
            if 2 in self.ilevels_dict[10]:
                self.i10_2 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 2, files_df = self.indicators_df)
        
        # 11. Monthly unique subscribers per home region
        if 11 in self.ilevels_dict.keys():
            if 3 in self.ilevels_dict[11]:
                self.i11_3 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 3, files_df = self.indicators_df)
            if 2 in self.ilevels_dict[11]:
                self.i11_2 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 2, files_df = self.indicators_df)
    
    # Create comparisson panel for all loaded indicators
    def dirty_panel(self):
        for i in self.i_list:
            getattr(self, i).create_panel()
            print('Created comp. panel ' + i)

    # Create clean panel for all loaded indicators
    def clean_panel(self, outliers_df):
        for i in self.i_list:
            getattr(self, i).create_clean_panel(outliers_df = outliers_df)
            print('Created clean panel ' + i)
        # i1.create_clean_panel(outliers_df = outliers_df)
    # Export panel datasets for all loaded indicators
    def export(self, path):
        print("Saving in " + path)
        for i in self.i_list:
            exp_path = path + i + '.csv'
            getattr(self, i).save(path = exp_path)
            print('Saved ' + i + '.csv' )

