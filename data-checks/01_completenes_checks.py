#-----------------------------------------------------------------#
# DATA CHECKS - completeness checks
#-----------------------------------------------------------------#


EXPORT_FIGURES = True

# This file checks all files to test if all0 dates and hours are 
# present.


flow_a2_file_list = os.listdir(IFLOWM_adm2_path)
flow_a3_file_list = os.listdir(IFLOWM_adm3_path)
cust_a2_file_list = os.listdir(ICUST_adm2_path)
cust_a3_file_list = os.listdir(ICUST_adm3_path)


# Variables
timevar = 'hour'
regvar = 'region'

#-----------------------------------------------------------------#
# Load data

# Define loading functionp that depends on the existing folder 
# structure but also remove headers in the middle of the data if
# if there is any
def loadfiles(file_name, 
              admin = 3,
              ext_path = ICUST_path):
    print(file_name, admin)
    # Load external file
    ext_folder = ext_path + 'admin' + str(admin) + '/' 
    de = None
    de = pd.read_csv(ext_folder + file_name)
    # Patch cleannig of headers in the middle of the data
    c1_name = de.columns[0]
    de = de[~de[c1_name].astype(str).str.contains(c1_name)]
    return(de)


# Indicator 1
fi = loadfiles(file_name = 'transactions_per_hour.csv')

# Indicator 2
f2 = loadfiles('unique_subscribers_per_day.csv')

# Indicator 5
f5 = loadfiles('origin_destination_connection_matrix_per_day.csv')

# Indicator 9
f9 = loadfiles('week_home_vs_day_location_per_day.csv', admin = 2)


#-----------------------------------------------------------------#
# Processing data

# Remove missings
reg_missings_bol = fi['region'].isin(['99999','']) 
fi_cl = fi[~reg_missings_bol]

# Check for duplicates
# sum(fi_cl.duplicated())  
fi_cl['count'] = fi_cl['count'].astype(int)

# Date vars
fi_cl['date'] = pd.to_datetime(fi_cl['hour']).dt.date
# fi_cl['hour'] = pd.to_datetime(fi_cl[timevar]).dt.hour
# fi_cl['month'] = pd.to_datetime(fi_cl['date']).dt.month

# Make sure dates are datetime
fi_cl['hour'] = fi_cl['hour'].astype('datetime64') 


# I5
f5['date'] = pd.to_datetime(f5['connection_date']).dt.date


#-----------------------------------------------------------------#
# Create aggregated datasets

#----------------------------
# I1 - transactions per hour

# Create plots data    
f1_agg_hour = fi_cl\
    .groupby(['date', 'hour'])\
    .agg({'region' : pd.Series.nunique ,
          'count' : np.sum})\
    .reset_index()\
    .sort_values(['date', 'hour'])\
    .rename(columns = {'region' : 'n_regions'})     

f1_agg_date = fi_cl\
    .groupby('date')\
    .agg({'region' : pd.Series.nunique ,
          'count' : np.sum})\
    .reset_index()\
    .sort_values(['date'])\
    .rename(columns = {'region' : 'n_regions'})   

#----------------------------
# I5 - OD matrix per day data

f5['date'] = pd.to_datetime(f5['connection_date']).dt.date

f5_agg_date = f5\
        .groupby('date')\
        .agg({'region_from' : pd.Series.nunique ,
              'region_to' : pd.Series.nunique,
              'total_count' : np.sum})\
        .reset_index()\
        .sort_values('date')


#----------------------------
# I9 - OD matrix per day data

# f9_agg_date = f9\
#         .groupby('week')\
#         .agg({'home_region' : pd.Series.nunique ,
#               'mean_distance' : pd.Series.mean})\
#         .reset_index()\
#         .sort_values('week')



#----------------------------
# Complete dates and time

# This creates data sets with time indexes and fill blanks with 0s

def time_complete(data, timevar, timefreq = 'D'):
    data[timevar] = data[timevar].astype('datetime64')
    full_time_range = pd.date_range(data[timevar].min(),  
                                    data[timevar].max(), 
                                    freq = timefreq)
    data = data.set_index(timevar)
    data = data.reindex(full_time_range,  fill_value=0)
    return(data)

f1_agg_date = time_complete(f1_agg_date, 'date')
f1_agg_hour = time_complete(f1_agg_hour, 'hour', 'H')
f5_agg_date = time_complete(f5_agg_date, 'date')



#-----------------------------------------------------------------#
# I1 - Day Plots


# Number of regions plot
plt.figure(figsize=(12, 6))
date_plot = sns.lineplot(f1_agg_date.index,
                         f1_agg_date['n_regions'])
# Export
date_plot.figure.savefig(OUT_hfcs + "i1_dates_ward_count.png")


# Number of transactions plot
plt.figure(figsize=(12, 6))
obs_per_day_plot = sns.lineplot(
    f1_agg_date.index,
    f1_agg_date['count'])
# Export
if EXPORT_FIGURES:
    obs_per_day_plot.figure.savefig(OUT_hfcs + "i1_dates_n_obs.png")


# Number of transactions scatter
# def scatter_plot(data, var = 'count'):
#     plot = sns.scatterplot(
#         data.index,
#         data[var])
#     plot.set_xlim([data.index.min() - dt.timedelta(days=1), 
#                   data.index.max() + dt.timedelta(days=1)])
#     # plot.set(yscale="log")
#     return plot

# scatter_plot(f1_agg_date)

#-----------------------------------------------------------------#
# I1 - Hour Plots

#------------------
# Number of regions 
plt.figure(figsize=(12, 6))
hour_plot = sns.lineplot(
    f1_agg_hour.index,
    f1_agg_hour['n_regions'])

# Cosmetics
# x_ticks = list(set(fi_agg_hour['hour'].astype(str)))[0:len(fi_agg_hour):5]
# x_ticks.sort()
# hour_plot.set_xticklabels(x_ticks)

# Export
if EXPORT_FIGURES:
    hour_plot.figure.savefig(OUT_hfcs + "i1_hours_ward_count.png")

#----------------------------
# Total count of transactions
plt.figure(figsize=(12, 6))
obs_per_hour_plot = sns.lineplot(
    f1_agg_hour.index.values,
    f1_agg_hour['count'])

# Cosmetics
# x_ticks = list(set(fi_agg_hour['date'].astype(str)))[0:len(fi_agg_hour):5]
# x_ticks.sort()
# obs_per_hour_plot.set_xticklabels(x_ticks)

# Export
if EXPORT_FIGURES:
    obs_per_hour_plot.figure.savefig(OUT_hfcs + "i1_hours_n_obs.png")


# Table with hours 
# fi_obs_per_hour[fi_obs_per_hour['date'] == dt.date(2020, 4, 30)]
# apr30 = f1_agg_hour[f1_agg_hour['date'] == dt.date(2020, 4, 30)]    

# apr30.to_csv(OUT_hfcs + "i1_hour_apr30.csv",
#              index = False)


#-----------------------------------------------------------------#
# I5 - Day Plots

# plot total count
f5_plot = sns.lineplot(
    f5_agg_date['date'],
    f5_agg_date['total_count'])
# Export
if EXPORT_FIGURES:
    f5_plot.figure.savefig(OUT_hfcs + "i5_dates_total_count.png")


#-----------------------------------------------------------------#
# I9 - Week plots


# f9_plot = sns.lineplot(
#     f9_agg_date['week'],
#     f9_agg_date['mean_distance'])
# # Export
# f9_plot.figure.savefig(OUT_hfcs + "i9_week_mean_distance.png")
