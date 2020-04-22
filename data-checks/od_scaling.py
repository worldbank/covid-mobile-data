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


I5_path = DATA_path + "indicator 5/"
I5_Adm2_path = I5_path + "admin2/"
I5_Adm3_path = I5_path + "admin3/"


#-----------------------------------------------------------------#
# Load data
od = pd.read_csv(I5_Adm3_path + "origin_destination_connection_matrix_per_day.csv")


#-----------------------------------------------------------------#
# Process data

# Convert dates
od['connection_date'] = pd.to_datetime(od['connection_date']).dt.date


#-----------------------------------------------------------------#
# Create different scaling factors

#-----------------------------------------------------------------#
# Plot

#-----------------------------------------------------------------#
# DRAFT

od1 = od[od['region_from'] == 'ZW192105']

# od1_top_dest = ['ZW120435','ZW142513','ZW192205',
#                 'ZW130720','ZW170530' ]

p1_df = od1[od1['region_to'] == 'ZW120435']
p1_df.index = p1_df['connection_date']

p1_df.set_index('connection_date')['subscriber_count'].plot()


