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
                 level = 3,
                 connection_date_1 = dt.date(2020, 3, 15),
                 connection_date_2 = dt.date(2020, 4, 1),
                 connection_date_3 = dt.date(2020, 5, 1),
                 connection_date_4 = dt.date(2020, 6, 1) ):
        # self.file_name = file_name
        self.num = num
        self.index_cols = index_cols
        self.level = level
        self.files_df = files_df
        self.connection_date_1 = connection_date_1
        self.connection_date_2 = connection_date_2
        self.connection_date_3 = connection_date_3
        self.connection_date_4 = connection_date_4
        
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
        folder = self.files_df['path'][idx].iat[0]
        self.data_02 = pd.read_csv(folder + '202002_' + self.files_df['file'][idx].iat[0] + '.csv')
        self.data_03 = pd.read_csv(folder + '202003_' + self.files_df['file'][idx].iat[0] + '.csv')
        self.data_04 = pd.read_csv(folder + '202004_' + self.files_df['file'][idx].iat[0] + '.csv')
        self.data_05 = pd.read_csv(folder + '202005_' + self.files_df['file'][idx].iat[0] + '.csv')
        self.data_06 = pd.read_csv(folder + '202006_' + self.files_df['file'][idx].iat[0] + '.csv')
        self.data_07 = pd.read_csv(folder + '202007_' + self.files_df['file'][idx].iat[0] + '.csv')
    
    # Clean indicators
    def clean(self):
        self.data_02 = clean(self.data_02, self.index_cols)
        self.data_03 = clean(self.data_03, self.index_cols)
        self.data_04 = clean(self.data_04, self.index_cols)
        self.data_05 = clean(self.data_05, self.index_cols)
        self.data_06 = clean(self.data_06, self.index_cols)
        self.data_07 = clean(self.data_07, self.index_cols)
    
    # Create panel by appending all dfs
    def create_panel(self):
        df_list = [self.data_02, self.data_03, self.data_04,
                   self.data_05, self.data_06, self.data_07]
        self.panel = pd.concat(df_list).sort_values(self.index_cols)
    
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


# #-----------------------------------------------------------------#
# # Constructor class

# # Define panel constructor class
# class panel_constructor:
#     """
#     This class contains loads files for specified indicators and creates
#     panel data sets combining all the files 
#     """
#     def __init__(self, 
#                  ilevels_dict,
#                  indicators_df,
#                  ind_dict = None):
#         self.ilevels_dict = ilevels_dict
#         self.indicators_df = indicators_df
#         # List all indicators loaded flattening dictionary of inficators and levels
#         i_list = []
#         for i in self.ilevels_dict.keys():
#             i_list.append(['i' + str(i) + '_' + str(y) for y in self.ilevels_dict[i]] )
#         self.i_list = list(chain.from_iterable(i_list))
        
#         # Set default indicators dictionary
#         if ind_dict is None:
#             self.ind_dict = {
#                  1 : 'transactions_per_hour.csv',
#                  2 : 'unique_subscribers_per_hour.csv',
#                  3 : 'unique_subscribers_per_day.csv',
#                  4 : 'percent_of_all_subscribers_active_per_day.csv',
#                  5 : 'origin_destination_connection_matrix_per_day.csv',
#                  6 : 'unique_subscriber_home_locations_per_week.csv',
#                  7 : 'mean_distance_per_day.csv',
#                  8 : 'mean_distance_per_week.csv',
#                  9 : 'week_home_vs_day_location_per_day.csv',
#                  10: 'origin_destination_matrix_time_per_day.csv',
#                  11: 'unique_subscriber_home_locations_per_month.csv'}
#         else:
#             self.ind_dict = ind_dict
#         #----------------------------------------------------------#
#         # Load indicators:
#         # 1. Transactions per hour - Always created since it is needed for usage outliers
#         self.i1_3 = i_indicator(num = 1,  index_cols = ['hour', 'region'], files_df = self.indicators_df)
        
#         # 2. Unique subscribers per hour
#         if 2 in self.ilevels_dict.keys():
#             self.i2_3 = i_indicator(num = 2,  index_cols = ['hour', 'region'], files_df = self.indicators_df)
        
#         # 3. Unique subscribers per day 
#         if 3 in self.ilevels_dict.keys(): 
#             if 3 in self.ilevels_dict[3]:
#                 self.i3_3 = i_indicator(num = 3,  index_cols = ['day', 'region'], files_df = self.indicators_df)
#         if 3 in self.ilevels_dict.keys(): 
#             if 2 in self.ilevels_dict[3]:
#                 self.i3_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2, files_df = self.indicators_df)
        
#         # 4. Proportion of active subscribers
#         if 4 in self.ilevels_dict.keys():
#             self.i4_country = i_indicator(num = 4,  index_cols = ['day'], level = 'country', files_df = self.indicators_df)
            
#         # 5 - Connection Matrix
#         if 5 in self.ilevels_dict.keys():
#             if 3 in self.ilevels_dict[5]:
#                 self.i5_3 = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], files_df = self.indicators_df)
#             if 2 in self.ilevels_dict[5]:
#                 self.i5_2 = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 2, files_df = self.indicators_df)
#             if 'tc_harare' in self.ilevels_dict[5]:
#                 self.i5_tc_harare = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_harare', files_df = self.indicators_df)
#             if 'tc_bulawayo' in self.ilevels_dict[5]:  
#                 self.i5_tc_bulawayo = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_bulawayo', files_df = self.indicators_df)
        
#         # 6. Unique subscribers per home location
#         if 6 in self.ilevels_dict.keys():
#             self.i6_3 = i_indicator(num = 6,  index_cols = ['week', 'home_region'], files_df = self.indicators_df)
        
#         # 7. Mean and Standard Deviation of distance traveled per day (by home location)
#         if 7 in self.ilevels_dict.keys():
#             if 3 in self.ilevels_dict[7]:
#                 self.i7_3 = i_indicator(num = 7,  index_cols =['day','home_region'], time_var = 'day', region_vars = ['home_region'], level = 3, files_df = self.indicators_df)
#             if 2 in self.ilevels_dict[7]:
#                 self.i7_2 = i_indicator(num = 7,  index_cols =['day','home_region'], time_var = 'day', region_vars = ['home_region'], level = 2, files_df = self.indicators_df)
        
#         # 8. Mean and Standard Deviation of distance traveled per week (by home location)
#         if 8 in self.ilevels_dict.keys():
#             if 3 in self.ilevels_dict[8]:
#                 self.i8_3 = i_indicator(num = 8,  index_cols =['week','home_region'], time_var = 'week', region_vars = ['home_region'], level = 3, files_df = self.indicators_df)
#             if 2 in self.ilevels_dict[8]:
#                 self.i8_2 = i_indicator(num = 8,  index_cols =['week','home_region'], time_var = 'week', region_vars = ['home_region'], level = 2, files_df = self.indicators_df)
       
#         # 9. Daily locations based on Home Region with average stay time and SD of stay time
#         if 9 in self.ilevels_dict.keys(): 
#             if 3 in self.ilevels_dict[9]:
#                 self.i9_3 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 3, files_df = self.indicators_df)
#             if 2 in self.ilevels_dict[9]:
#                 self.i9_2 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 2, files_df = self.indicators_df)
#             if 'tc_bulawayo' in self.ilevels_dict[9]:
#                 self.i9_3 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 'tc_bulawayo', files_df = self.indicators_df)
#             if 'tc_harare' in self.ilevels_dict[9]:
#                 self.i9_2 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 'tc_harare', files_df = self.indicators_df)
        
#         # 10. Simple OD matrix with duration of stay
#         if 10 in self.ilevels_dict.keys():
#             if 3 in self.ilevels_dict[10]:
#                 self.i10_3 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 3, files_df = self.indicators_df)
#             if 2 in self.ilevels_dict[10]:
#                 self.i10_2 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 2, files_df = self.indicators_df)
        
#         # 11. Monthly unique subscribers per home region
#         if 11 in self.ilevels_dict.keys():
#             if 3 in self.ilevels_dict[11]:
#                 self.i11_3 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 3, files_df = self.indicators_df)
#             if 2 in self.ilevels_dict[11]:
#                 self.i11_2 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 2, files_df = self.indicators_df)
    
#     # Create comparisson panel for all loaded indicators
#     def dirty_panel(self):
#         for i in self.i_list:
#             getattr(self, i).create_panel()
#             print('Created comp. panel ' + i)
    
#     # Create clean panel for all loaded indicators
#     def clean_panel(self, outliers_df):
#         for i in self.i_list:
#             getattr(self, i).create_clean_panel(outliers_df = outliers_df)
#             print('Created clean panel ' + i)
#         # i1.create_clean_panel(outliers_df = outliers_df)
    
#     # Load other mno indicators
#     def load_other_mno(self, mno_path, mno_suffix):
#         def admin_prefix(x):
#             if x == 2:
#                 prefix = 'admin2'
#             elif x == 3:
#                 prefix = 'admin3'
#             else:
#                 prefix = x
#             return prefix
        
#         # Loop through levels dict values and load attributes
#         for i in list(self.ilevels_dict.keys()):
#             for j in range(0, len(self.ilevels_dict[i])):
#                 # print(str(i) + '_' + str(j))
#                 path = os.path.join(mno_path, 
#                                     admin_prefix(self.ilevels_dict[i][j]),
#                                     self.ind_dict[i])
#                 attr_name = 'i' + str(i) + '_' + str(self.ilevels_dict[i][j]) + mno_suffix
#                 df = pd.read_csv(path)
#                 # Create attributes
#                 print('Loading:' + attr_name + 'from ' + path)
#                 setattr(self, attr_name, df)
#     # Add other mno to panel
#     def add_other_provider(self, mno_path, mno_suffix):
#         # Load other mno data
#         self.load_other_mno(mno_path, mno_suffix)
#         # Add to panel
#         self.i1_3.add_provider(getattr(self, 'i1_3' + mno_suffix))
        
#         if 2 in self.ilevels_dict.keys():
#             self.i2_3.add_provider(getattr(self, 'i2_3' + mno_suffix))
#         if 3 in self.ilevels_dict.keys():
#             self.i3_2.add_provider(getattr(self, 'i3_2' + mno_suffix))
#             self.i3_3.add_provider(getattr(self, 'i3_3' + mno_suffix))
#         if 4 in self.ilevels_dict.keys():
#             self.i4_country.add_provider(getattr(self, 'i4_country' + mno_suffix))
#         if 5 in self.ilevels_dict.keys():
#             self.i5_2.add_provider(getattr(self, 'i5_2' + mno_suffix))
#             self.i5_3.add_provider(getattr(self, 'i5_3' + mno_suffix))
#         if 6 in self.ilevels_dict.keys():
#             self.i6_3.add_provider(getattr(self, 'i6_3' + mno_suffix))
#         if 7 in self.ilevels_dict.keys():    
#             self.i7_2.add_provider(getattr(self, 'i7_2' + mno_suffix))
#             self.i7_3.add_provider(getattr(self, 'i7_3' + mno_suffix))
#         if 8 in self.ilevels_dict.keys():
#             self.i8_2.add_provider(getattr(self, 'i8_2' + mno_suffix))
#             self.i8_3.add_provider(getattr(self, 'i8_3' + mno_suffix))
#         if 9 in self.ilevels_dict.keys():
#             self.i9_2.add_provider(getattr(self, 'i9_2' + mno_suffix))
#             self.i9_3.add_provider(getattr(self, 'i9_3' + mno_suffix))
#         if 10 in self.ilevels_dict.keys():
#             self.i10_2.add_provider(getattr(self, 'i10_2' + mno_suffix))
#             self.i10_3.add_provider(getattr(self, 'i10_3' + mno_suffix))
#         if 11 in self.ilevels_dict.keys():
#             self.i11_2.add_provider(getattr(self, 'i11_2' + mno_suffix))
#             self.i11_3.add_provider(getattr(self, 'i11_3' + mno_suffix))
        
#     # Export panel datasets for all loaded indicators
#     def export(self, path):
#         print("Saving in " + path)
#         for i in self.i_list:
#             exp_path = path + i + '.csv'
#             getattr(self, i).save(path = exp_path)
#             print('Saved ' + i + '.csv' )

