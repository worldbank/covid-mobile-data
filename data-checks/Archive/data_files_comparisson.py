#-----------------------------------------------------------------#
# CSV comparisson
#-----------------------------------------------------------------#

import os
import re
import numpy as np
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt 
# import seaborn as sns
from datetime import datetime


IRESULTS = DATA_path + "Isaac-results/"

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
           timevar,
           standardize_time = False,
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
def compare_dfs(df1,df2, filename = None, outputdf = False):
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
    
    if outputdf:
        return(cdf)
    else:
        # Print report
        print(filename)
        print('N rows ours: ' + str(df1.shape[0]) )
        print("N rows Isaac's: " + str(df2.shape[0]))
        print('Of matching rows:')
        print(' - Average difference of count column: ' + avg_diff + "%")
        print(' - Percentage rows that are different: ' + p_rows_diff + "%")
        print('\n')


#-----------------------------------------------------------------#
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
    
#-----------------------------------------------------------------#    
# Custom indicatos csv

# Indicator 1 #

i1 = pd.read_csv(I1_Adm3_path + 'transactions_per_hour.csv')
i1i = pd.read_csv(ICUST + 'transactions_per_hour.csv')

# i1 = compat(i1, timevar = [])
# i1i = compat(i1i, timevar = [])

cdf = compare_dfs(i1,i1i, outputdf = True)
cdf["diff_flag"] = cdf["count_x"] != cdf["count_y"]
cdf['date'] = pd.to_datetime(cdf['hour']).dt.date


foo = cdf[cdf['diff_flag']]

foo.to_csv('C:/Users/wb519128/Desktop/i1_differences.csv',
           index = False)

# Indicator 3 #

i3 = pd.read_csv(I3_Adm3_path + 'unique_subscribers_per_day.csv')
i3i = pd.read_csv(ICUST + 'unique_subscribers_per_day.csv')

cdf3 = compare_dfs(i3,i3i, outputdf = True)
cdf3["diff_flag"] = cdf3["count_x"] != cdf3["count_y"]
cdf3['day'] = pd.to_datetime(cdf3['day']).dt.date


cdf3[cdf3['day'] == dt.date(2020, 2, 3)]

foo = cdf3[cdf3['diff_flag']]

foo.to_csv('C:/Users/wb519128/Desktop/i3_differences.csv',
           index = False)



# Indicator 5 #
I5_Adm3_path

i5 = pd.read_csv(I5_Adm3_path + 'origin_destination_connection_matrix_per_day.csv')
i5i = pd.read_csv(ICUST + 'origin_destination_connection_matrix_per_day.csv')

#cdf5 = compare_dfs(i5,i5i, outputdf = True)
#cdf5["diff_flag"] = cdf5["total_count_x"] != cdf5["total_count_y"]

compare_dfs(i5,i5i, outputdf = False)

bar = i5.merge(i5i, on = ['connection_date', 'region_from', 'region_to'])
bar["diff_flag"] = bar["od_count_x"] != bar["od_count_y"]

diff_day_df = bar.groupby('connection_date').sum()
diff_day_df = diff_day_df.reset_index()

diff_day_df['day'] = pd.to_datetime(diff_day_df['connection_date']).dt.day

plt.plot('day',
         'diff',
         data = diff_day_df)

# set(bar['connection_date'])
# len(set(pd.to_datetime(foo['connection_date']).dt.date))
# len(set(foo['region_from']))

# # Absolute difference by day
# bar['diff'] = bar['od_count_x']- bar['od_count_y'] 



# foo = bar[bar['diff_flag']]

# foo['diff'] = foo['od_count_x']- foo['od_count_y'] 
# foo['diff'].mean()

export_i5_merged = bar.rename(
    columns = {
        'subscriber_count_x' : 'subscriber_count',
        'subscriber_count_y' : 'subscriber_count_isaac',
        'od_count_x': 'od_count_x',
        'od_count_y': 'od_count_isaac',
        'total_count_x' : 'total_count',
        'total_count_y' : 'total_count_isaac'})



export_i5_merged\
    .to_csv('C:/Users/wb519128/Desktop/i5_merged_with_Isaacs.csv',
           index = False)




#-----------------------------------------------------------------#    
# DRAFT

file_i = filenames[0]

d1 = pd.read_csv(FLOWM_adm3_path + file_i) 
d2 = pd.read_csv(IFLOW + file_i) 

cdf = compare_dfs(d1,d2, outputdf = True)
cdf["diff_flag"] = cdf["count_x"] != cdf["count_y"]