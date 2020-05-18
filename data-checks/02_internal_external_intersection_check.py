#-----------------------------------------------------------------#
# DATA CHECKS - Internal and external comparisons
#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
# Flowminder csvs

# load list of internal indicators
temp_path = 'C:/Users/wb519128/Desktop/'
internal_indicators = pd.read_csv(temp_path + 'int_files.csv')

#-----------------------------------------------------------------#
# Comparisson function


# Drop custom missigs
def drop_custna(data, columns):
    na_list = ['nan', '', 99999] 
    for cols in columns:
        data = data[~(data[cols].isin(na_list))]
    return(data)

# Comparisson outputs function
def compare_dfs(df1,df2, filename = None, outputdf = True):
    # Merge dfs
    index_cols = list(df1.columns[0:-1])
    # Make sure indexes are in the same format 
    df1[index_cols[1:][0]] = df1[ index_cols[1:][0]].astype(str).str.replace('.0', '', regex=True)
    df2[index_cols[1:][0]] = df2[ index_cols[1:][0]].astype(str).str.replace('.0', '', regex=True)
    # df1[index_cols[1:]] = df1[index_cols[1:]].astype(float).astype(int)
    # df2[index_cols[1:]] = df2[index_cols[1:]].astype(float).astype(int)
    #Make sure merging columns are str
    df1[index_cols] = df1[index_cols].astype(str)
    df2[index_cols] = df2[index_cols].astype(str)
    cdf = df1.merge(df2, left_on = index_cols, right_on = index_cols)
    #--------------------#
    # Calculate differeces
    # Make sure values are numeric
    cdf[cdf.columns[-1]] = cdf[cdf.columns[-1]].astype(int)
    cdf[cdf.columns[-2]] = cdf[cdf.columns[-2]].astype(int)
    # Create differences df
    diff_df = cdf[cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]]]
    # Value difference
    # Proportion of mismatches
    p_rows_diff = sum(cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]])/cdf.shape[0]
    p_rows_diff = str(round(p_rows_diff, 4)*100)
    # Return outputs
    if outputdf:
        return(diff_df)
    else:
        # Print report
        print(filename)
        # print('N rows ours: ' + str(df1.shape[0]) )
        # print("N rows Isaac's: " + str(df2.shape[0]))
        print('Of matching rows:')
        #print(' - Average difference of count column: ' + avg_diff + "%")
        print(' - Percentage rows that are different: ' + p_rows_diff + "%")
        print('\n')



#-----------------------------------------------------------------#
# Loop through all the files

# Separate a few for manual merge
sep_list = ['percent_of_all_subscribers_active_per_day.csv', # Diff coliumn names
            'origin_destination_connection_matrix_per_day.csv',
            'mean_distance_per_day.csv',
            'mean_distance_per_week.csv',
            'origin_destination_matrix_time_per_day.csv',
            'count_unique_active_residents_per_region_per_day.csv',
            'count_unique_active_residents_per_region_per_week.csv',
            'count_unique_subscribers_per_region_per_day.csv',
            'count_unique_subscribers_per_region_per_week.csv',
            'count_unique_visitors_per_region_per_day.csv',
            'count_unique_visitors_per_region_per_week.csv'] 
loop_df = internal_indicators[~internal_indicators['file'].isin(sep_list)]

remaining = [25, 26, 27, 28,
            31, 32, 33, 34, 35, 36, 37, 38]


# Load files function
def loadfiles(file_name, 
    files_df = internal_indicators):
    print(file_name)
    # Set intex
    idx = files_df[files_df['file'] == file_name].index.values[0]
    # Load internal
    d = None
    d = pd.read_csv(files_df['path'][idx] + file_name)
    # Load external
    if files_df['indicator'][idx] == 'flow':
        ext_path = IFLOW_path
    else:
        ext_path = ICUST_path
    # Load external file
    ext_folder = ext_path + 'admin' + str(files_df['level'][idx]) + '/' 
    de = None
    de = pd.read_csv(ext_folder + file_name)
    return([d, de])

# Clean function
def clean(d):
    # Remove missins
    d = d.dropna()
    # All but the last column
    index_cols = list(d.columns[0:-1])
    d = drop_custna(d, index_cols)
    return(d)


# Comparisson outputs function
def compare_dfs(df1,df2, index_cols):
    cdf = df1.merge(df2, on = index_cols)
    #--------------------#
    # Calculate differeces
    # Make sure values are numeric
    cdf[cdf.columns[-1]] = cdf[cdf.columns[-1]].astype(int)
    cdf[cdf.columns[-2]] = cdf[cdf.columns[-2]].astype(int)
    # Create differences df
    diff_df = cdf[cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]]]
    # Value difference
    # Proportion of mismatches
    p_rows_diff = sum(cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]])/cdf.shape[0]
    p_rows_diff = str(round(p_rows_diff, 4)*100)
    # Return outputs
    return(diff_df)


# Complete pipeline function
def process_pipeline(file_name, 
                     index_cols,
                     files_df = internal_indicators):
    # Laod and clean data
    d,de =  loadfiles(file_name)
    d = clean(d)
    de = clean(de)
    # Merge
    cdf_diff = compare_dfs(i1,i1i, index_cols = index_cols )
    # output
    return(cdf)

# Export
def export(diff_data, files_df, idx):
    export_prefix = 'diff_' + 'admin' + str(files_df['level'][idx]) + '_'
    export_name = export_prefix + files_df['file'][idx]
    diff_data.to_csv(OUT_hfcs_sheets + export_name,
                    index = False)



process_pipeline

i1, i1i = loadfiles('transactions_per_hour.csv')
i1 = clean(i1)
i1i = clean(i1i)
i1_diff = compare_dfs(i1,i1i, index_cols =['region', 'hour'] )


#d, de = loadfiles('transactions_per_hour.csv')


process_pipeline('transactions_per_hour.csv',
                 index_cols =['region', 'hour'])


i2, i2i = loadfiles('unique_subscribers_per_day.csv')
i2 = clean(i1)
i2i = clean(i1i)


i1_diff = compare_dfs(i2,i2i, index_cols =['region', 'hour'] )

internal_indicators['file'][3]

# 
process_pipeline(1)
process_pipeline(2)
process_pipeline(3)
process_pipeline(4)
process_pipeline(5)
process_pipeline(6)
process_pipeline(7)
process_pipeline(8)
process_pipeline(9)
process_pipeline(10)
process_pipeline(11)
process_pipeline(12)
process_pipeline(13)
process_pipeline(14)
process_pipeline(15)
process_pipeline(16)
# process_pipeline(17)




i1,i1i =  loadfiles(1)

# Export
#export_name = 'diff_' + 'admin_' + str(files_df['level'][i]) + '_' +file_name




i1, i1i = loadfiles('transactions_per_hour.csv')

filename = 
files_df = internal_indicators


internal_indicators[internal_indicators.file == 'transactions_per_hour.csv'].index