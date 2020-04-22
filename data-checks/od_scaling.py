#-----------------------------------------------------------------#
# OD matrix scaling checks
#-----------------------------------------------------------------#


#-----------------------------------------------------------------#
# Settings

import pandas as pd
import matplotlib.pyplot as plt
import datetime

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


#-----------------------------------------------------------------#
# Process data

# Convert dates
od['connection_date'] = pd.to_datetime(od['connection_date']).dt.date

#-----------------------------------------------------------------#
# Create different scaling factors

#-----------------------------------------------------------------#
# Plot DRAFT

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
