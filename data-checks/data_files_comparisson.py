#-----------------------------------------------------------------#
# CSV comparisson
#-----------------------------------------------------------------#

import os
import re
import numpy as np
import pandas as pd
from datetime import datetime


IRESULTS = DATA_path + "Zimbabwe/Isaac-results/"

IFLOW = IRESULTS + "flowminder/"
ICUST = IRESULTS + "custom/"

#-----------------------------------------------------------------#
# Load files

filenames = os.listdir(IFLOW)

file_i = filenames[0]
# Our file
d1 = pd.read_csv(FLOWM_adm3_path + file_i) 

# I's file
d2 = pd.read_csv(IFLOW + file_i) 

#-----------------------------------------------------------------#
# Make sure data is compatible

# Masure order and date formats are the same
def compat(data,
           standardize_time = False,
           timevar = time,
           regvar = 'region'):
    new_data = data
    
    # If has a date convert to standard
    if len(timevar) != 0:
        timevar = np.asscalar(np.array(timevar))
        new_data[timevar] = pd.to_datetime(new_data[timevar]).dt.date
    # Make sure order is the same
    if len(data.columns) == 2:
        new_data = new_data.sort_values( by = [new_data.columns[0], new_data.columns[1] ])
    else :
        new_data = new_data.sort_values( by = [new_data.columns[0], new_data.columns[1], new_data.columns[2] ])
    return new_data

for i in range(0, len(filenames)-1):
    file_i = filenames[i]
    # print(i)
    # print(filenames[i])
    # Our file
    d1 = pd.read_csv(FLOWM_adm3_path + file_i) 
    # I's file
    d2 = pd.read_csv(IFLOW + file_i) 
    # Set time var (GARMBIARRA WARNING)
    time = list(d1.columns[list(d1.columns.str.contains('date'))])
    # Process data to be in the same format
    d1 = compat(d1, timevar = time)
    d2 = compat(d2, timevar = time)
    # Merge dfs
    index_cols = list(d1.columns[0:-1])
    #Make sure merging columns are str
    d1[index_cols] = d1[index_cols].astype(str)
    d2[index_cols] = d2[index_cols].astype(str)
    cdf = d1.merge(d2, left_on = index_cols, right_on = index_cols)
    #--------------------#
    # Calculate differeces
    # Proportion of mismatches
    p_rows_diff = sum(cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]])/cdf.shape[0]
    p_rows_diff = str(round(p_rows_diff, 4)*100)
    # Value difference
    cdf['pdiff'] = ((cdf[cdf.columns[-1]] -
                    cdf[cdf.columns[-2]])/cdf[cdf.columns[-2]])
    # Average difference
    avg_diff = str(round(cdf['pdiff'].mean(skipna = True), 4)*100)
    # Print report
    print(i)
    print(filenames[i])
    print('N rows ours: ' + str(d1.shape[0]) )
    print("N rows Isaac's: " + str(d2.shape[0]))
    print('Of matching rows:')
    print(' - Average difference of count column: ' + avg_diff + "%")
    print(' - Percentage rows that are different: ' + p_rows_diff + "%")
    print('\n')

