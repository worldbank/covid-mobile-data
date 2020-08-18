#-----------------------------------------------------------------#
# Create Admin2 Indicator 1
#-----------------------------------------------------------------#

EXPORT = False

# import shapely
# import geojsonio
import os
import geopandas as gpd
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

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
# Transactions per hour by district line plot.

# Line plot function definition
def line_plot(reg_i,
               var = 'count_p',
               data = i1_agg,
               region = 'district_id',
               region_str = 'district_name',
               time = 'hour'):
    plt_data = data[data[region] == reg_i]
    fig = go.Figure()
    # Create line
    fig.add_trace(go.Scatter(x=plt_data[time], y=plt_data[var],
                    mode='lines',
                    name='lines'))
    # Additional formatting 
    title =  str(plt_data[region].iloc[0]) + plt_data[region_str].iloc[0]
    fig.update_layout(
        title=title,
        xaxis_title="Time",
        yaxis_title="Count",
        font=dict(
            # family="Courier New, monospace",
            size=18,
            color="#7f7f7f"),
        autosize=False,
        width=1200,
        height=700
        )
    return(fig)

# Districts list
dists = list(set(i1_agg['district_id']))

# region_plt(d)
# plt.show()

# Loop over districts
for d in dists:
    print(d)
    # Create plot
    plt_i = line_plot(d)
    # Export
    save_name = None
    save_name = 'i1_districts_count' + str(d) + '.png'
    plt_i.write_image(OUT_plots + 'daily_obs_region/' + save_name)


#-----------------------------------------------------------------#
# Transactions per hour by day. That is one plot per hour
i1_agg['time'] = pd.to_datetime(i1_agg['hour']).dt.hour
i1_agg['date'] = pd.to_datetime(i1_agg['hour']).dt.date


def hourly_scatter(reg_i,
               var = 'count_p',
               data = i1_agg,
               region = 'district_id',
               region_str = 'district_name',
               time = 'date',
               facets = 'time'):
    # Subset data
    plt_data = data[data[region] == reg_i]
    # Create plot
    fig = px.scatter(plt_data, 
                    x= time, 
                    y = var, 
                    facet_col = facets, 
                    facet_col_wrap = 5,
                    width=1200,
                    height=700)
    # Additional formatting 
    title =  str(plt_data[region].iloc[0]) + ' - ' + plt_data[region_str].iloc[0]
    fig.update_layout(title_text= title)
    fig.update_yaxes(matches=None)
    fig.for_each_annotation(lambda a: a.update(text=a.text.replace("time=", "")))
    # Format axis titles
    return(fig)

# Loop over districts
for d in dists:
    print(d)
    # Create plot
    plt_i = hourly_scatter(d)
    # Export
    save_name = None
    save_name = 'i1_hourly_obs_byhour' + str(d) + '.png'
    plt_i.write_image(OUT_plots + 'hourly_obs_by_hour_region/' + save_name)




#-----------------------------------------------------------------#
# Export data 
if EXPORT:
    i1_agg.to_csv(OUT_hfcs + 'Sheet comp panel/i1_admin2.csv', index = False)




#-----------------------------------------------------------------#
# DRAFT
