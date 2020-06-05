#-----------------------------------------------------------------#
# Create Admin2 Indicator 1
#-----------------------------------------------------------------#

EXPORT = False

# import shapely
# import geojsonio
import os
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns; sns.set()


#-----------------------------------------------------------------#
# Load data

# Indicator 1 panel data
i1 = pd.read_csv( OUT_hfcs + 'Sheet comp panel/i1_admin3.csv')
i1 = i1[i1.region != '99999']
# Wards data
wards = gpd.read_file(DATA_GIS + 'wards_aggregated.geojson')
wd = wards[['ward_id', 'province_name', 'district_id', 'district_name']]

#-----------------------------------------------------------------#
# Create wards mapping into disctrics

i1 = i1.merge(wd, left_on = 'region', right_on = 'ward_id')


# Aggregate values by district
i1_agg = i1.groupby(['district_id', 'district_name', 'hour']).agg(lambda x : sum(x)).reset_index()

# Make sure hour is in date time
i1_agg['hour'] = i1_agg['hour'].astype('datetime64')
i1_agg['district_id'] = i1_agg['district_id'].astype('int')

#-----------------------------------------------------------------#
# Plots

def region_plt(reg_i,
               var = 'count_p',
               data = i1_agg,
               region = 'district_id',
               region_str = 'district_name',
               time = 'hour'):
    # Select regions to be in the plot
    plt_data = data[data[region] == reg_i].set_index(time)
    # Reshape dat to plot
    # splt_data = pd.pivot_table(plt_data.reset_index(),
    #                            index=time,
    #                            columns=region, 
    #                            values=var)
    # Plot
    title = plt_data[region_str][0]
    plot = plt_data[var].plot(color = 'navy',
                              title = title,
                              figsize=(12,8))
    #
    return plot

# Districts list
dists = list(set(i1_agg['district_id']))

# Loop over districts
for d in dists:
    print(d)
    # Export
    save_name = None
    save_name = 'i1_districts_count' + str(d) + '.png'
    region_plt(d).figure.savefig(OUT_plots + 'daily_obs_region/' + save_name,  dpi=250)


plt_i = region_plt(dists[0])
save_name = 'i1_districts_count' + str(dists[0]) + '.png'
plt_i.figure.savefig(OUT_plots + 'daily_obs_region/' + save_name,  dpi=250)

# # Loop 5 by 5 and save plots
# loop_list = list(range(5,len(dists),5))
# loop_list.append(len(dists))
# last_i = 0
# idx=0
# for i in loop_list:
#     dist_idx =range(last_i, i+1)
#     save_name = 'i1_districts_count' + str(idx) + '.png'
#     region_plt(regions = dists[last_i:i+1])[0].get_figure().savefig(OUT_hfcs + save_name,  dpi=250)
#     last_i = i
#     idx += 1


#-----------------------------------------------------------------#
# Export data 
if EXPORT:
    i1_agg.to_csv(OUT_hfcs + 'Sheet comp panel/i1_admin2.csv', index = False)


