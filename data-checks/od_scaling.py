#-----------------------------------------------------------------#
# OD matrix scaling checks
#-----------------------------------------------------------------#


#-----------------------------------------------------------------#
# Settings

import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os

# File paths
DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/databricks-results/zw/"


# OD matrix
I5_path = DATA_path + "indicator 5/"
I5_Adm2_path = I5_path + "admin2/"
I5_Adm3_path = I5_path + "admin3/"

# Flowminder indicators
FLOWM_path = DATA_path + "flowminder indicators/"
FLOWM_adm2_path = FLOWM_path + "admin2/"
FLOWM_adm3_path = FLOWM_path + "admin3/"

#-----------------------------------------------------------------#
# Load data
od = pd.read_csv(I5_Adm3_path + 
                 "origin_destination_connection_matrix_per_day.csv")


# Number of residents
res = pd.read_csv(FLOWM_adm3_path + 
                 "home_location_counts_per_region.csv")

# Active residents
ares = pd.read_csv(FLOWM_adm3_path + 
                   "count_unique_active_residents_per_region_per_day.csv")

# Number of calls 
cal = pd.read_csv(FLOWM_adm3_path + 
                   "total_calls_per_region_per_day.csv")


#-----------------------------------------------------------------#
# Process data

# Create date variable
def convert_dates(df,date_col ='connection_date'):
    df['date'] = pd.\
        to_datetime(df[date_col]).\
            dt.date
    return(df)

od = convert_dates(od, 'connection_date')
ares = convert_dates(ares, 'visit_date')
cal = convert_dates(cal, 'call_date')

#-----------------------------------------------------------------#
# Create different scaling factors

#--------------------#
# Create new variables

# Number of active subscribers over total residents
ares = ares.merge(res.rename(columns={"subscriber_count" : "residents"}), 
                  on = 'region', 
                  how='outer')

ares = ares.rename(columns={"subscriber_count" : 'active_res'})

# Check pp > 1 !!!!
ares['p_active_res'] = ares['active_res']/ares['residents']



# Number of calls over residents
cal = cal.merge(res.rename(columns={"subscriber_count" : "residents"}), 
                  on = 'region', 
                  how='outer')

cal['p_cals'] = cal['total_calls']/cal['residents']

#------------------------------#
# Add new variables to od matrix

# Proportion of active residents in orig and dest
od = od.\
    merge(ares[['region','date', 'p_active_res']], 
          left_on= ['region_from','date'], 
          right_on= ['region', 'date'], 
          how='left').\
            rename(columns={'p_active_res' : 'p_active_res_O'}).\
            drop(columns='region').\
    merge(ares[['region','date', 'p_active_res']], 
          left_on= ['region_to','date'], 
          right_on= ['region', 'date'], 
          how='left').\
            rename(columns={'p_active_res' : 'p_active_res_D'}).\
            drop(columns='region')


# Proportion of calls per residents in orig and dest
od = od.\
    merge(cal[['region','date', 'p_cals']], 
          left_on= ['region_from','date'], 
          right_on= ['region', 'date'], 
          how='left').\
            rename(columns={'p_cals' : 'p_cals_O'}).\
            drop(columns='region').\
    merge(cal[['region','date', 'p_cals']], 
          left_on= ['region_to','date'], 
          right_on= ['region', 'date'], 
          how='left').\
            rename(columns={'p_cals' : 'p_cals_D'}).\
            drop(columns='region')


#-----------------#
# Create indicators

# Multiplication of total active residents in origin and 
# destiantion
od['w1'] = od['p_active_res_O']* od['p_active_res_D']


# Sum of calls per person in origin and destinaion
od['w2'] = od['p_cals_O'] + od['p_cals_D']


# od['p_cals_O'].isnull().sum()/od.shape[0] 
# 0.5159950493247425

#-----------------------------------------------------------------#
# Create scaled values
od['total_count_w1'] = od['total_count']*od['w1'] 



#-----------------------------------------------------------------#
# Plot DRAFT

# Set origin region
od1 = od[od['region_from'] == 'ZW192105']

od1_top_dest = ['ZW120435','ZW142513','ZW192205',
                #'ZW130720',
                'ZW170530' ]


# p1_df = od1[od1['region_to'] == 'ZW120435']
p1_df = od1[od1['region_to'].isin(od1_top_dest)]
p1_df.index = p1_df['connection_date']

# foo = p1_df.groupby('region_to').plot(x='connection_date', y='subscriber_count')

fig, ax = plt.subplots(nrows=2,ncols=2,figsize=(12,6))
fig = plt.figure()
gs = fig.add_gridspec(2, 2)

p1_df[p1_df['region_to'] == od1_top_dest[0]].\
    plot(x='connection_date', 
         y='subscriber_count', 
         ax = fig.add_subplot(gs[0, 0]))

p1_df[p1_df['region_to'] == od1_top_dest[1]].\
    plot(x='connection_date', 
         y='subscriber_count', 
         ax = fig.add_subplot(gs[0, 1]))

p1_df[p1_df['region_to'] == od1_top_dest[2]].\
    plot(x='connection_date', 
         y='subscriber_count', 
         ax = fig.add_subplot(gs[1, 0]))

p1_df[p1_df['region_to'] == od1_top_dest[3]].\
    plot(x='connection_date', 
         y='subscriber_count', 
         ax = fig.add_subplot(gs[1, 1]))

fig.savefig('C:/Users/wb519128/Desktop/full_figure.png')
