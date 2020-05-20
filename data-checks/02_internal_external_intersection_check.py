#-----------------------------------------------------------------#
# DATA CHECKS - Internal and external comparisons
#-----------------------------------------------------------------#

# Settings

EXPORT = False

#-----------------------------------------------------------------#
# F

# # load list of internal indicators
# temp_path = 'C:/Users/wb519128/Desktop/'
# internal_indicators = pd.read_csv(temp_path + 'int_files.csv')

#-----------------------------------------------------------------#
# Function definitions

# Drop custom missigs
def drop_custna(data, columns):
    na_list = ['nan', '', 99999, float("inf")] 
    for cols in columns:
        data = data[~(data[cols].isin(na_list))]
    return(data)

# Load files function
def loadfiles(file_name, 
              files_df = internal_indicators,
              admin = 3):
    print(file_name, admin)
    # Set intex
    idx = files_df[(files_df['file'] == file_name) & (files_df['level'] == admin)].index.values[0]    # Load internal
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
    # Patch cleannig of headers in the middle of the data
    c1_name = d.columns[0]
    de = de[~de[c1_name].astype(str).str.contains(c1_name)]    
    return([d, de])

# Clean function
def clean(d, index_cols):
    # Remove missins
    d = d.dropna()
    # All but the last column
    #index_cols = list(d.columns[0:-1])
    d = drop_custna(d, index_cols)
    return(d)




#-----------------------------------------------------------------#
# Check overlap of data for a few key indicators

# Set processing pipeline
def process_pipeline(d, 
                     de, 
                     index_cols,
                     do_clean = True,
                     do_export = True,                     
                     how = 'inner'):
    if do_clean:
        d = clean(d, index_cols)
        de = clean(de, index_cols)
    md = d.merge(de, 
               on = index_cols, 
               how = how,
               suffixes=('', '_ecnt'))
    return md


# Indicator 1
i1, i1i = loadfiles('transactions_per_hour.csv')
i1_index = ['hour', 'region']

i1_m = process_pipeline(i1, i1i, i1_index)

# Indicator 2
i2, i2i = loadfiles('unique_subscribers_per_day.csv')
i2_index = ['day', 'region']

i2_m = process_pipeline(i2, i2i, i2_index)

# Indicator 5
i5, i5i = loadfiles('origin_destination_connection_matrix_per_day.csv')
i5_index = ['connection_date', 'region_from', 'region_to']

i5_m = process_pipeline(i5, i5i, i5_index)


# Indicator 9
i9, i9i = loadfiles('mean_distance_per_week.csv', admin = 2)
i9_index = ['home_region', 'week']

# Fix i9 district id
i9 = clean(i9, i9_index)
i9i = clean(i9i, i9_index)
i9['home_region'] = i9['home_region'].astype(int)
i9i['home_region'] = i9i['home_region'].astype(int)

i9_m = process_pipeline(i9, i9i, i9_index, do_clean = False)

# Export 
def export(data, 
           file_name, 
           path = OUT_hfcs + 'Sheet intersections/'):
    # export_prefix = 'intersection_'
    export_prefix = ''
    export_name = export_prefix + file_name + '.csv'
    data.to_csv(path + export_name,
                        index = False)

if EXPORT:
    export(i1_m, 'i1_admin3', path = OUT_hfcs + 'Sheet intersections/')
    export(i2_m, 'i2_admin3', path = OUT_hfcs + 'Sheet intersections/')
    export(i5_m, 'i5_admin3', path = OUT_hfcs + 'Sheet intersections/')
    export(i9_m, 'i9_admin2', path = OUT_hfcs + 'Sheet intersections/')

#-----------------------------------------------------------------#
# Create differences tables

def diff_dataset(data,
                 col1,
                 col2):
    diff_data = data[data[col1] != data[col2]]
    return diff_data

i1_m_diff = diff_dataset(i1_m, 'count', 'count_ecnt')
i2_m_diff = diff_dataset(i2_m, 'count', 'count_ecnt')
i5_m_diff = diff_dataset(i5_m, 'total_count', 'total_count_ecnt')
i9_m_diff = diff_dataset(i9_m, 'mean_distance', 'mean_distance_ecnt')


if EXPORT:
    export(i1_m_diff, 'i1_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i2_m_diff, 'i2_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i5_m_diff, 'i5_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i9_m_diff, 'i9_admin2', path = OUT_hfcs + 'Sheet differences/')


#-----------------------------------------------------------------#
# DRAFT
i5_m_diff['connection_date'].nunique()
set(i5_m_diff['connection_date'])    

i9_m_diff['week'].nunique()
set(i9_m_diff['week'])   


#-----------------------------------------------------------------#
# Loop through all the files

# # Separate a few for manual merge
# sep_list = ['percent_of_all_subscribers_active_per_day.csv', # Diff coliumn names
#             'origin_destination_connection_matrix_per_day.csv',
#             'mean_distance_per_day.csv',
#             'mean_distance_per_week.csv',
#             'origin_destination_matrix_time_per_day.csv',
#             'count_unique_active_residents_per_region_per_day.csv',
#             'count_unique_active_residents_per_region_per_week.csv',
#             'count_unique_subscribers_per_region_per_day.csv',
#             'count_unique_subscribers_per_region_per_week.csv',
#             'count_unique_visitors_per_region_per_day.csv',
#             'count_unique_visitors_per_region_per_week.csv'] 
# loop_df = internal_indicators[~internal_indicators['file'].isin(sep_list)]

# remaining = [25, 26, 27, 28,
#             31, 32, 33, 34, 35, 36, 37, 38]




# # Clean function
# def clean(d):
#     # Remove missins
#     d = d.dropna()
#     # All but the last column
#     index_cols = list(d.columns[0:-1])
#     d = drop_custna(d, index_cols)
#     return(d)


# # Comparisson outputs function
# def compare_dfs(df1,df2, index_cols):
#     cdf = df1.merge(df2, on = index_cols)
#     #--------------------#
#     # Calculate differeces
#     # Make sure values are numeric
#     cdf[cdf.columns[-1]] = cdf[cdf.columns[-1]].astype(int)
#     cdf[cdf.columns[-2]] = cdf[cdf.columns[-2]].astype(int)
#     # Create differences df
#     diff_df = cdf[cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]]]
#     # Value difference
#     # Proportion of mismatches
#     p_rows_diff = sum(cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]])/cdf.shape[0]
#     p_rows_diff = str(round(p_rows_diff, 4)*100)
#     # Return outputs
#     return(diff_df)


# # Complete pipeline function
# def process_pipeline(file_name, 
#                      index_cols,
#                      files_df = internal_indicators):
#     # Laod and clean data
#     d,de =  loadfiles(file_name)
#     d = clean(d)
#     de = clean(de)
#     # Merge
#     cdf_diff = compare_dfs(i1,i1i, index_cols = index_cols )
#     # output
#     return(cdf)

# # Export
# def export(diff_data, files_df, idx):
#     export_prefix = 'diff_' + 'admin' + str(files_df['level'][idx]) + '_'
#     export_name = export_prefix + files_df['file'][idx]
#     diff_data.to_csv(OUT_hfcs_sheets + export_name,
#                     index = False)



# process_pipeline

# i1, i1i = loadfiles('transactions_per_hour.csv')
# i1 = clean(i1)
# i1i = clean(i1i)
# i1_diff = compare_dfs(i1,i1i, index_cols =['region', 'hour'] )


# #d, de = loadfiles('transactions_per_hour.csv')


# process_pipeline('transactions_per_hour.csv',
#                  index_cols =['region', 'hour'])


# i2, i2i = loadfiles('unique_subscribers_per_day.csv')
# i2 = clean(i1)
# i2i = clean(i1i)


# i1_diff = compare_dfs(i2,i2i, index_cols =['region', 'hour'] )

# internal_indicators['file'][3]

# # 
# process_pipeline(1)
# process_pipeline(2)
# process_pipeline(3)
# process_pipeline(4)
# process_pipeline(5)
# process_pipeline(6)
# process_pipeline(7)
# process_pipeline(8)
# process_pipeline(9)
# process_pipeline(10)
# process_pipeline(11)
# process_pipeline(12)
# process_pipeline(13)
# process_pipeline(14)
# process_pipeline(15)
# process_pipeline(16)
# # process_pipeline(17)




# i1,i1i =  loadfiles(1)

# # Export
# #export_name = 'diff_' + 'admin_' + str(files_df['level'][i]) + '_' +file_name




# i1, i1i = loadfiles('transactions_per_hour.csv')

# filename = 
# files_df = internal_indicators


# internal_indicators[internal_indicators.file == 'transactions_per_hour.csv'].index



# # Comparisson outputs function
# def compare_dfs(df1,df2, filename = None, outputdf = True):
#     # Merge dfs
#     index_cols = list(df1.columns[0:-1])
#     # Make sure indexes are in the same format 
#     df1[index_cols[1:][0]] = df1[ index_cols[1:][0]].astype(str).str.replace('.0', '', regex=True)
#     df2[index_cols[1:][0]] = df2[ index_cols[1:][0]].astype(str).str.replace('.0', '', regex=True)
#     # df1[index_cols[1:]] = df1[index_cols[1:]].astype(float).astype(int)
#     # df2[index_cols[1:]] = df2[index_cols[1:]].astype(float).astype(int)
#     #Make sure merging columns are str
#     df1[index_cols] = df1[index_cols].astype(str)
#     df2[index_cols] = df2[index_cols].astype(str)
#     cdf = df1.merge(df2, left_on = index_cols, right_on = index_cols)
#     #--------------------#
#     # Calculate differeces
#     # Make sure values are numeric
#     cdf[cdf.columns[-1]] = cdf[cdf.columns[-1]].astype(int)
#     cdf[cdf.columns[-2]] = cdf[cdf.columns[-2]].astype(int)
#     # Create differences df
#     diff_df = cdf[cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]]]
#     # Value difference
#     # Proportion of mismatches
#     p_rows_diff = sum(cdf[cdf.columns[-1]] != cdf[cdf.columns[-2]])/cdf.shape[0]
#     p_rows_diff = str(round(p_rows_diff, 4)*100)
#     # Return outputs
#     if outputdf:
#         return(diff_df)
#     else:
#         # Print report
#         print(filename)
#         # print('N rows ours: ' + str(df1.shape[0]) )
#         # print("N rows Isaac's: " + str(df2.shape[0]))
#         print('Of matching rows:')
#         #print(' - Average difference of count column: ' + avg_diff + "%")
#         print(' - Percentage rows that are different: ' + p_rows_diff + "%")
#         print('\n')
