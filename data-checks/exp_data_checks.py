#-----------------------------------------------------------------#
# Exploratoiry analsis of new data
#-----------------------------------------------------------------#

import pandas as pd
import matplotlib.pyplot as plt
import datetime
import os


# Hourly transactions per region 
i1 = pd.read_csv(I1_Adm3_path + "transactions_per_hour.csv")


# New data
path = 'C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/apr-22/apr20/HOURLY_SUMMARY/'
nd = pd.read_csv(path + 'HOURLY_SUMMARY.txt', sep = '|')

#-----------------------------------------------------------------#
# Processing Indicator 1

i1['date'] = pd.to_datetime(i1['hour']).dt.date

# Formated time
i1['time'] = pd.to_datetime(i1['hour']).dt.strftime('%m-%d %H')

#-----------------------------------------------------------------#
# Processing new data

# Remove duplicates
nd = nd.drop_duplicates()
# os. chdir('C:/Users/wb519128/Desktop')
# nd.sort_values(by=['CALL_HOUR', 'ADMIN_REGION'])\
#     .head(n=10000)\
#     .to_html("temp.html")


# Rename columns for easy of use
nd = nd.rename(columns={'ADMIN_REGION' : 'region', 
                        'SUBSCRIBER_COUNT' : 'subs_count', 
                        'OBSERVATION_COUNT' : 'obs_count'})

# Dates
nd['date'] = pd.to_datetime(nd['CALL_HOUR']).dt.date

# Formated time
nd['time'] = pd.to_datetime(nd['CALL_HOUR']).dt.strftime('%m-%d %H')




# Regions - only wards aparently
# len(set(nd['ADMIN_REGION']))

# Check values    


#-----------------------------------------------------------------#
# Time series plot

# Aggregate by day for simplicity
day_data = nd.groupby(['region','date'])\
    .sum()\
    .reset_index()

# Average across all wars
avg_day_data = day_data.groupby(['date'])\
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




#-----------------------------------------------------------------#
# Whole country plots

# Total number of calls (whole country)
wc_data = nd.groupby(['date'])\
    .sum()\
    .reset_index()

wc_data_i1 = i1.groupby(['date'])\
    .sum()\
    .reset_index()

wc_plot = wc_data.plot(y = 'obs_count',
          x = 'date',
          rot=45,
          legend= False)

wc_plot.figure.savefig(OUT_hfcs + 'sdataApr20_total_n_obs_zwe.png')


#-----------------------------------------------------------------#
# Comparissons with our own data


# Plot hourly calls for 27-29 of march
startdate = pd.to_datetime('2020-03-27').date()
enddate = pd.to_datetime('2020-03-30').date()

# Restrict dates
late_march = nd[(nd['date'] >= startdate) & (nd['date'] <= enddate)]
late_march_i1 = i1[(i1['date'] >= startdate) & (i1['date'] <= enddate)]

# Make sure data is comparable
pcomp_dat = late_march.drop(['subs_count'], axis=1)\
    .groupby('time')\
    .sum()\
    .rename(columns = {'obs_count' : 'new_data_count'})\
    .reset_index()
# pcomp_dat['data_source'] = 'new data'
    
pcomp_dat_i1 = late_march_i1.groupby('time').sum()\
    .rename(columns = {'count' : 'indicator 1'})\
    .reset_index()
# pcomp_dat_i1['data_source'] = 'indicator 1'

# Append data
# plot_data = pcomp_dat.append(pcomp_dat_i1)

plot_data = pcomp_dat.merge(pcomp_dat_i1, on = 'time')

# Plot
plot =  plot_data.plot(
    x='time',
    y = ['indicator 1', 'new_data_count'],
    rot=45)


plot.figure.savefig(OUT_hfcs + 'sdataApr20_i1_late_march_comp.png')


#-----------------------------------------------------------------#
# Check for district differences in comparable data

# Make sure data is comparable
comp_nd = late_march.drop(['subs_count'], axis=1)\
    .groupby(['time', 'region'])\
    .sum()\
    .rename(columns = {'obs_count' : 'new_data_count'})\
    .reset_index()
    
comp_i1 = late_march_i1\
    .groupby(['time', 'region'])\
    .sum()\
    .rename(columns = {'count' : 'i1'})\
    .reset_index()


cd = comp_nd.merge(comp_i1, on = ['time', 'region'])

# Diff wards
diff = cd[cd.new_data_count != cd.i1]

# How different is the data
diff.mean()