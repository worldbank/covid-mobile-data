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

# Custom indicatos
# filenames = os.listdir(ICUST)

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


# Comparisson outputs function
def compare_dfs(df1,df2, filename):
    # Set time var (GARMBIARRA WARNING)
    time = list(df1.columns[list(df1.columns.str.contains('date'))])
    # Process data to be in the same format
    df1 = compat(df1, timevar = time)
    df2 = compat(df2, timevar = time)
    # Merge dfs
    index_cols = list(df1.columns[0:-1])
    #Make sure merging columns are str
    df1[index_cols] = df1[index_cols].astype(str)
    df2[index_cols] = df2[index_cols].astype(str)
    cdf = df1.merge(df2, left_on = index_cols, right_on = index_cols)
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
    print(filename)
    print('N rows ours: ' + str(df1.shape[0]) )
    print("N rows Isaac's: " + str(df2.shape[0]))
    print('Of matching rows:')
    print(' - Average difference of count column: ' + avg_diff + "%")
    print(' - Percentage rows that are different: ' + p_rows_diff + "%")
    print('\n')


# Custom indicatos csv
i1 = pd.read_csv(I1_Adm3_path + 'transactions_per_hour.csv')
i1i = pd.read_csv(ICUST + 'transactions_per_hour.csv')

i1 = compat(i1, timevar = [])
i1i = compat(i1i, timevar = [])

index_cols = list(df1.columns[0:-1])
df1[index_cols] = df1[index_cols].astype(str)
df2[index_cols] = df2[index_cols].astype(str)
cdf = df1.merge(df2, left_on = index_cols, right_on = index_cols)


compare_dfs(i1,i1i, filename = 'transactions_per_hour.csv')


# Flowminder csvs
for i in range(0, len(filenames)-1):
    file_i = filenames[i]
    # print(i)
    # print(filenames[i])
    # Our file
    d1 = pd.read_csv(FLOWM_adm3_path + file_i) 
    # I's file
    d2 = pd.read_csv(IFLOW + file_i) 
    
    # Run comparisson
    print(i)
    print(filenames[i])
    compare_dfs(d1,d2)