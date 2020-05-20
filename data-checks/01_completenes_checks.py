#-----------------------------------------------------------------#
# DATA CHECKS - completeness checks
#-----------------------------------------------------------------#

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
f9 = loadfiles('mean_distance_per_week.csv', admin = 2)


#-----------------------------------------------------------------#
# Countrywide checks

#-----------------------------#
# Start by checking indicator 1

# Process

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

#-----------------------------------------------------------------#
# Countrywide checks

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



# Number of regions plot
plt.figure(figsize=(12, 6))
date_plot = sns.lineplot(
    f1_agg_date['date'],
    f1_agg_date['n_regions'])
# Export
date_plot.figure.savefig(OUT_hfcs + "i1_dates_ward_count.png")


# Number of transactions plot
plt.figure(figsize=(12, 6))
obs_per_day_plot = sns.lineplot(
    f1_agg_date['date'],
    f1_agg_date['count'])
# Export
obs_per_day_plot.figure.savefig(OUT_hfcs + "i1_dates_n_obs.png")




#------------------#
# Hours completeness

# Create plot 
plt.figure(figsize=(12, 6))
hour_plot = sns.lineplot(fi_agg_hour.index.values,
             fi_agg_hour['n_regions'])

# Cosmetics
x_ticks = list(set(fi_agg_hour['date'].astype(str)))[0:len(fi_agg_hour):5]
x_ticks.sort()
hour_plot.set_xticklabels(x_ticks)

# Export
hour_plot.figure.savefig(OUT_hfcs + "i1_hours_ward_count.png")


# Create plot 
plt.figure(figsize=(12, 6))
obs_per_hour_plot = sns.lineplot(
    f1_agg_hour.index.values,
    f1_agg_hour['count'])

# Cosmetics
x_ticks = list(set(fi_agg_hour['date'].astype(str)))[0:len(fi_agg_hour):5]
x_ticks.sort()
obs_per_hour_plot.set_xticklabels(x_ticks)

# Export
obs_per_hour_plot.figure.savefig(OUT_hfcs + "i1_hours_n_obs.png")


# Table with hours containing
# fi_obs_per_hour[fi_obs_per_hour['date'] == dt.date(2020, 4, 30)]
apr30 = f1_agg_hour[f1_agg_hour['date'] == dt.date(2020, 4, 30)]    

apr30.to_csv(OUT_hfcs + "i1_hour_apr30.csv",
             index = False)


#-----------------#
# Check indicator 5

f5['date'] = pd.to_datetime(f5['connection_date']).dt.date

f5_agg_date = f5\
        .groupby('date')\
        .agg({'region_from' : pd.Series.nunique ,
              'region_to' : pd.Series.nunique,
              'total_count' : np.sum})\
        .reset_index()\
        .sort_values('date')


def

# plot total count
f5_plot = sns.lineplot(
    f5_agg_date['date'],
    f5_agg_date['total_count'])
# Export
f5_plot.figure.savefig(OUT_hfcs + "i5_dates_total_count.png")


#-----------------#
# Check indicator 9
f9_agg_date = f9\
        .groupby('week')\
        .agg({'home_region' : pd.Series.nunique ,
              'mean_distance' : pd.Series.mean})\
        .reset_index()\
        .sort_values('week')

f9_plot = sns.lineplot(
    f9_agg_date['week'],
    f9_agg_date['mean_distance'])
# Export
f9_plot.figure.savefig(OUT_hfcs + "i9_week_mean_distance.png")



# def agg_function(data,
#                  group_vars,
#                  count_vars,
#                  sum_vars):
#     agg_data = data\
#         .groupby(group_vars)\
#         .agg({count_vars : pd.Series.nunique ,
#               sum_vars : np.sum})\
#         .reset_index()\
#         .sort_values(group_vars)
#     return agg_data
        
# agg_function(fi_cl,
#              group_vars =  ['date', 'hour'],
#              sum_vars = 'count',
#              count_vars = 'region')        

 
# agg_function(f5,
#              group_vars =  ['connection_date', 'region_from', 'region_to'],
#              sum_vars = 'total_count',
#              count_vars = 'region_from')       


# #-----------------------------------------------------------------#
# # DRAFT

  


# # Check if all date and hour combinations are in the data
# # fi_cl['date'].max() - fi_cl['date'].min()
# # 88
# fi_cl['date'].nunique()
# sorted(fi_cl['date'].unique())

# a.sort()

# fi_cl.groupby(regvar).nunique()


# # Get districts with lessthan 24 hours
# foo = fi_cl\
#     .groupby([regvar,'date'])\
#     ['hour'].nunique()\
#     .reset_index()
 
# foo = foo[foo['hour'] < 24].sort_values([regvar, 'date'])    
# foo.to_csv(OUT_hfcs_sheets + 'i1_wards_with_less_than_24h.csv', index = False)


# # Number of days per region
# bar = fi_cl\
#     .groupby([regvar, 'month'])\
#     ['date'].nunique()\
#     .reset_index()

# bar.to_csv(OUT_hfcs_sheets + 'i1_ndays_per_ward.csv', index = False)


# # Number of regions per date
# fob = fi_cl\
#     .groupby(['date'])\
#     [regvar].nunique()\
#     .reset_index()

# fob.to_csv(OUT_hfcs_sheets + 'i1_nwards_per_date.csv', index = False)

# #-----------------------------------------------------------------#
# # Region specific checks

# fi['date'] = pd.to_datetime(fi[timevar]).dt.date


# fob = fi_cl\
#     .groupby(['date'])\
#     [regvar].nunique()\
#     .reset_index()
# fob.to_csv(OUT_hfcs_sheets + 'foo.csv', index = False)




# fi_cl.sort_values([regvar, 'date'])


# pd.to_datetime(fi[timevar][440000:445000])
# pd.to_datetime(fi[timevar][440650:440700])
# fi[440650:440700]



# # new_data[timevar] = 
# pd.to_pydatetime(fi[timevar])

# pd.to_pydatetime('2020-02-17T19:00:00.000Z')

# pd.to_datetime('2020-02-17T19:00:00.000Z').dt.date

# pd.Timestamp('2020-02-17T19:00:00.000Z').date()

# fi.hour.dt

# pd.to_datetime(fi[timevar]).dt.hour
