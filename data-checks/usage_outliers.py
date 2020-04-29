#-----------------------------------------------------------------#
# Outliers and towers down
#-----------------------------------------------------------------#

# This code depends on MASTER.py to run as file path objects are
# defined there


#-----------------------------------------------------------------#
# TO DO:

# Identify regions with very sparse use
 # 1. Count obs per region
 # 2. Count obs per region per day

# Identify regions with normal use and big valleys of usage, that 
# would probably indicate a tower being down

#-----------------------------------------------------------------#
# Settings

import pandas as pd

EXPORT = True

# Number of hours below avg, used as a trashold to 
# define a tower down
htrahshold = -3

#-----------------------------------------------------------------#
# Import data

# Hourly transactions per region 
i1 = pd.read_csv(I1_Adm3_path + "transactions_per_hour.csv")

# Unique subscribers per hour
# i2a3 = pd.read_csv(I2_Adm3_path + "unique_subscribers_per_hour.csv")
# i2t = pd.read_csv(I2_towercluster_path + "unique_subscribers_per_hour.csv")


#-----------------------------------------------------------------#
# Process data

i1['date'] = pd.to_datetime(i1['hour']).dt.date
i1['hour_int'] = pd.to_datetime(i1['hour']).dt.hour


#-----------------------------------------------------------------#
# Wards with very little data

# Number of observations per ward that is total number of hours
i1freq = i1.groupby('region').size()

i1freq = i1freq.reset_index()
i1freq.columns = ['region', 'freq']

# Select wards with less than 12h on average
i1_low_total_hours = i1freq[i1freq['freq'] < (12*i1.date.nunique())]

i1_low_total_hours = i1_low_total_hours\
    .rename(columns = {'freq' : 'total_hours'})
# # Proportion of wards with at least one tower down
# freq[freq < 1392].count()/len(set(i1['region']))

# # Proportion of wards with very 
# freq[freq < 700].count()
# freq[freq < 700].count()/len(set(i1['region']))

# Export
if(EXPORT):
    (i1_low_total_hours
    .to_csv(OUT_hfcs + 'wards_with_low_hours_I1.csv', 
            index = False) )

#-----------------------------------------------------------------#
# Indicator wards and days with towers down

# Number of hours with transactions per region day
hours_per_day = i1.groupby(['region', 'date']).size()

hours_per_day = hours_per_day.reset_index() # ger regions to be a column
hours_per_day.columns = ['region', 'date', 'hcount']


# Average hours per day per region
avg_hours = (hours_per_day.groupby(['region'])
    .mean()
    .rename(columns={'hcount' :'avg_hours' }))

# Create region day data set
i1_ag_df = hours_per_day.merge(avg_hours,
                                on = 'region')

# Difference from average usage per hour
i1_ag_df['h_diff'] = i1_ag_df['hcount'] - i1_ag_df['avg_hours']

# Create data only with pairs of wards and days potential 
# towers down
i1_ag_df_tower_down = i1_ag_df[i1_ag_df['h_diff'] < htrahshold]

# Read me text
readme_text = "This file contains a combinations of wards and  days that are assumed to have a tower down."
readme_text += "If a day has " + str(abs(htrahshold))  
readme_text += " hours with any calls below the daily avergage for that ward,"
readme_text += " it is considered to have a trower down at some point that day."  

# Export
if(EXPORT):
    (i1_ag_df_tower_down 
    .to_csv(OUT_hfcs + 'days_wards_with_low_hours_I1.csv', 
            index = False) )
    # Read me file
    file = open(OUT_hfcs + "days_wards_with_low_hours_I1_README.txt", "w") 
    file.write(readme_text) 
    file.close() 





