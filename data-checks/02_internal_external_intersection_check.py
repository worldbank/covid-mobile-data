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
            'mean_distance_per_day.csv'] # More indexes
loop_df = internal_indicators[~internal_indicators['file'].isin(sep_list)]

for i in loop_df.index:
    print(i) 
    # Load data
    file_name = loop_df['file'][i]
    d = None
    d = pd.read_csv(loop_df['path'][i] + file_name)
    # Load external
    if loop_df['indicator'][i] == 'flow':
        ext_path = IFLOW_path
    else:
        ext_path = ICUST_path
    ext_folder = ext_path + 'admin' + str(loop_df['level'][i]) + '/' 
    de = None
    de = pd.read_csv(ext_folder + file_name)
    # Patch cleannig of headers in the middle of the data
    c1_name = d.columns[0]
    d = d[~(d[c1_name].str.contains(c1_name))]
    de = de[~de[c1_name].str.contains(c1_name)]
    # Remove missins
    d = d.dropna()
    de = de.dropna()
    # All but the last column
    index_cols = list(d.columns[0:-1])
    d = drop_custna(d, index_cols)
    de = drop_custna(de, index_cols)
    # Create differences DF
    diff_df = compare_dfs(d,de, outputdf = True)
    # Export
    export_name = 'diff_' + 'admin_' + str(files_df['level'][i]) + '_' +file_name
    diff_df.to_csv(OUT_hfcs_sheets + export_name,
                index = False)


i = 6

df1 = d
df2 = de

foo =


df1['region'].str.replace('.0', '', regex=True)

type(df1[index_cols[1:]])
type(df1['region'])
file_name = internal_indicators['file'][38]

# Load data
d = pd.read_csv(internal_indicators['path'][i] + file_name)
de = pd.read_csv(fileinternal_indicatorss_df['path_e'][i] + file_name)


# Create differences DF
diff_df = compare_dfs(d,de, outputdf = True)

# Export
export_name = 'diff_' + 'admin_' + str(files_df['level'][i]) + '_' +file_name

diff_df.to_csv(OUT_hfcs_sheets + export_name,
               index = False)







i1i = pd.read_csv(ICUST_adm3_path + file_name) 
i1 = pd.read_csv(I1_Adm3_path + file_name) 
i1 = pd.read_csv(files_df['path'][i] + file_name)
i1i = pd.read_csv(files_df['path_e'][i] + file_name)



im = i1.merge(i1i, on = ['region', 'hour'],
                how='inner')

im = i1.merge(i1i, on = ['region', 'hour'])

idff = im[im['count_x'] != im['count_x']]




#-----------------------------------------------------------------#
# Flowminder csvs
for i in range(0, len(filenames)-1):
    file_i = files_df[i]
    file_path_i = 
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