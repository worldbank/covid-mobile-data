#-----------------------------------------------------------------#
# Outliers and towers down
#-----------------------------------------------------------------#

# This code depends on MASTER.py to run as file path objects are
# defined there


#-----------------------------------------------------------------#
# TO DO:

# Identify regions with very sparse use 

# Identify regions with normal use and big valleys of usage, that 
# would probably indicate a tower being down

#-----------------------------------------------------------------#
# Settings

import pandas as pd


#-----------------------------------------------------------------#
# Import data

# Hourly transactions per region 
i1 = pd.read_csv(I1_Adm3_path + "transactions_per_hour.csv")

# Unique subscribers per hour
i2a3 = pd.read_csv(I2_Adm3_path + "unique_subscribers_per_hour.csv")
i2t = pd.read_csv(I2_towercluster_path + "unique_subscribers_per_hour.csv")


#-----------------------------------------------------------------#
# Draft code


# from IPython.display import HTML
# foo = pd.read_csv(DATA_dash_clean_a3 + "origin_destination_connection_matrix_per_day.csv")
# bar = foo[0:1000]
# HTML(bar.to_html())

