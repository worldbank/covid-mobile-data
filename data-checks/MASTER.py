#-----------------------------------------------------------------#
# DATA CHECKS MASTER
#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#### Settings

import os
import re
import pandas as pd
import numpy as np
import datetime as dt

import matplotlib.pyplot as plt 

#-----------------------------------------------------------------#
#### Set file paths

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
DATA_POC = DATA_path + "proof-of-concept/"

DATA_DB_raw_indicators = DATA_POC + "databricks-results/zw/"
DATA_dashboad_clean = DATA_POC + "/files_for_dashboard/files_clean/"

DATA_dash_clean_a2 = DATA_dashboad_clean + "adm2/"
DATA_dash_clean_a3 = DATA_dashboad_clean + "adm3/"

#---------------#
# Main indicators

# Transactions per hour
I1_path = DATA_DB_raw_indicators + "indicator 1/"
I1_Adm3_path = I1_path + "admin3/"


# Unique subcribers per hour
I2_path = DATA_DB_raw_indicators + "indicator 2/"
I2_Adm3_path = I2_path + "admin3/"
I2_towercluster_path = I2_path + "tower_cluster/"


# Unique subscribers per day
I3_path = DATA_DB_raw_indicators + "indicator 3/"
I3_Adm2_path = I3_path + "admin2/"
I3_Adm3_path = I3_path + "admin3/"

# Ratio of residents active that day based on those present 
# during baseline
I4_path = DATA_DB_raw_indicators + "indicator 4/"
I4_Adm2_path = I4_path + 'admin2/'
I4_Adm3_path = I4_path + 'admin3/'

# OD matrix
I5_path = DATA_DB_raw_indicators + "indicator 5/"
I5_Adm2_path = I5_path + "admin2/"
I5_Adm3_path = I5_path + "admin3/"

# Residents living in area
I6_path = DATA_DB_raw_indicators + "indicator 6/"
I6_Adm2_path = I6_path + "admin2/"
I6_Adm3_path = I6_path + "admin3/"

# Mean and Standard Deviation of distance 
# traveled (by home location) day
I7_path = DATA_DB_raw_indicators + "indicator 7/"
I7_Adm2_path = I7_path + "admin2/"
I7_Adm3_path = I7_path + "admin3/"

# Mean and Standard Deviation of distance 
# traveled (by home location) week
I8_path = DATA_DB_raw_indicators + "indicator 8/"
I8_Adm2_path = I5_path + "admin2/"
I8_Adm3_path = I5_path + "admin3/"

# Daily locations based on Home Region with 
# average stay time and SD of stay time
I9_path = DATA_DB_raw_indicators + "indicator 9/"
I9_Adm2_path = I5_path + "admin2/"
I9_Adm3_path = I5_path + "admin3/"


#Simple Origin Destination Matrix - trips 
# between consecutive in time regions with time
I10_path = DATA_DB_raw_indicators + "indicator 10/"
I10_Adm2_path = I5_path + "admin2/"
I10_Adm3_path = I5_path + "admin3/"

#---------------------#
# Flowminder indicators
FLOWM_path = DATA_DB_raw_indicators + "flowminder indicators/"
FLOWM_adm2_path = FLOWM_path + "admin2/"
FLOWM_adm3_path = FLOWM_path + "admin3/"

#-------------------#
# External indicators

IRESULTS = DATA_path + "Zimbabwe/Isaac-results/"

IFLOW_path = IRESULTS + "flowminder/"
ICUST_path = IRESULTS + "custom/"

# Flowminder
IFLOWM_adm2_path = IFLOW_path + "admin2/"
IFLOWM_adm3_path = IFLOW_path + "admin3/"

# Custum
ICUST_adm2_path = ICUST_path + "admin2/"
ICUST_adm3_path = ICUST_path + "admin3/"


#---------------#
# Outputs
OUT_path = DATA_POC + "outputs/"
OUT_hfcs = OUT_path + "data-checks/"

#-----------------------------------------------------------------#
# Indicator dataframes

# flow_a2_file_list = os.listdir(IFLOWM_adm2_path)
# cust_a2_file_list = os.listdir(ICUST_adm2_path)

# Create dataframe for internal flowminder indicators
flow_i_2 = pd.DataFrame({'file': os.listdir(FLOWM_adm2_path),
                        'level': 2,
                         'path':FLOWM_adm2_path}) 

flow_i_3 = pd.DataFrame({
    'indicator': 'flow',
    'file': os.listdir(FLOWM_adm3_path),
    'level': 3,
    'path':FLOWM_adm3_path}) 

# Create dataframe for internal custum indicators

#       idx filename amin    
data = [[1, 'transactions_per_hour.csv', 3, I1_Adm3_path], 
        [2, 'unique_subscribers_per_hour.csv', 3, I2_Adm3_path], 
#       i2 tower cluster
        [3, 'unique_subscribers_per_day.csv', 2, I3_Adm2_path], 
        [3, 'unique_subscribers_per_day.csv', 3, I3_Adm3_path],
#       i3 whole country
        [4, 'percent_of_all_subscribers_active_option3_per_day.csv', 2, I4_Adm2_path], 
        [4, 'percent_of_all_subscribers_active_option3_per_day.csv', 3, I4_Adm3_path],
#       i4 whole country
        [5, 'origin_destination_connection_matrix_per_day.csv', 2, I5_Adm3_path], 
        [5, 'origin_destination_connection_matrix_per_day.csv', 3, I5_Adm3_path], 
#       i5 tower cluster
        [6, 'unique_subscriber_home_locations_per_week.csv', 3, I6_Adm3_path], 
        [7, 'mean_distance_per_day.csv', 2, I7_Adm2_path], 
        [7, 'mean_distance_per_day.csv', 3, I7_Adm3_path], 
#       i7 tower cluster        
        [8, 'mean_distance_per_week.csv', 2, I8_Adm2_path], 
        [8, 'mean_distance_per_week.csv', 3, I8_Adm3_path],     
#       i8 voronoi       
        [9, 'month_home_vs_day_location_per_day.csv', 2, I9_Adm2_path], 
        [9, 'month_home_vs_day_location_per_day.csv', 2, I9_Adm2_path], 
        [9, 'month_home_vs_day_location_per_day.csv', 3, I9_Adm3_path], 
        [9, 'month_home_vs_day_location_per_day.csv', 3, I9_Adm3_path], 
        [10, 'origin_destination_matrix_time_per_day.csv', 2, I10_Adm2_path], 
        [10, 'origin_destination_matrix_time_per_day.csv', 3, I10_Adm3_path]
        ]

cust_i = pd.DataFrame(data, columns = ['indicator',
                                       'file',
                                       'level',
                                       'path'])

# Create dataframe that contains all internally created indicators
# and their paths
all_internal_idicators = cust_i.append(flow_i_3)