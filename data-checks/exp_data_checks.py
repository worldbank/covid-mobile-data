#-----------------------------------------------------------------#
# Exploratoiry analsis of new data
#-----------------------------------------------------------------#

import pandas as pd
import matplotlib.pyplot as plt
import os


# Hourly transactions per region 
i1 = pd.read_csv(I1_Adm3_path + "transactions_per_hour.csv")


# New data
path = 'C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/apr-22/apr20/HOURLY_SUMMARY/'
nd = pd.read_csv(path + 'HOURLY_SUMMARY.txt', sep = '|')

#-----------------------------------------------------------------#
# Processing Indicator 1

i1['date'] = pd.to_datetime(i1['hour']).dt.date
i1['hour'] = pd.to_datetime(i1['hour']).dt.hour

#-----------------------------------------------------------------#
# Processing new data

# Rename columns for easy of use
nd = nd.rename(columns={'ADMIN_REGION' : 'region', 
                        'SUBSCRIBER_COUNT' : 'subs_count', 
                        'OBSERVATION_COUNT' : 'obs_count'})


# Dates
nd['date'] = pd.to_datetime(nd['CALL_HOUR']).dt.date
nd['hour'] = pd.to_datetime(nd['CALL_HOUR']).dt.hour

# Remove duplicates

nd = nd.drop_duplicates()
# os. chdir('C:/Users/wb519128/Desktop')
# nd.sort_values(by=['CALL_HOUR', 'ADMIN_REGION'])\
#     .head(n=10000)\
#     .to_html("temp.html")
 
# Dates
# set(nd['date'])

# Regions - only wards aparently
# len(set(nd['ADMIN_REGION']))

# Check values    

#-----------------------------------------------------------------#
# Comparissons with our own data

#-----------------------------------------------------------------#
# Time series plot

# Aggregate by day for simplicity
day_data = nd.groupby(['region','date'])\
    .sum()\
    .reset_index()

# Average across all wars
avg_day_data = day_data = nd.groupby(['date'])\
    .mean()\
    .reset_index()

plt_data = avg_day_data

# plt_data[plt_data['region'] == 'ZW120105']\
p_subs = plt_data.plot(y = 'subs_count',
          x = 'date',
          rot=45,
          legend= False)

p_subs.figure.savefig(OUT_hfcs + 'sdataApr20_avg_n_subscr.png')

p_obs = plt_data.plot(y = 'obs_count',
          x = 'date',
          rot=45,
          legend= False)

p_obs.figure.savefig(OUT_hfcs + 'sdataApr20_avg_n_obs.png')

# plt.show()