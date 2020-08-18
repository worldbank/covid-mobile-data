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
    na_list = ['nan', '', '99999',  float("inf")] 
    for cols in columns:
        data = data[~(data[cols].isin(na_list))]
    return(data)

# Load files function
def loadfiles(file_name, 
              files_df = internal_indicators,
              admin = 3):
     # Set intex
    idx = files_df[(files_df['file'] == file_name) & (files_df['level'] == admin)].index.values[0]    # Load internal
    # Custom file names for i5 and i7
    if file_name in ['mean_distance_per_day', 
                     'origin_destination_connection_matrix_per_day',
                     'mean_distance_per_week',
                     'month_home_vs_day_location_per_day',
                     'week_home_vs_day_location_per_day']:
        file_name_i = file_name + '_7day_limit.csv'
    else:
        file_name_i = file_name + '.csv'
    # External names
    file_name_e = file_name + '.csv'
    print(file_name, admin)
    # Load data
    d = None
    d = pd.read_csv(files_df['path'][idx] + file_name_i)
    # Load external
    if files_df['indicator'][idx] == 'flow':
        ext_path = IFLOW_path
    else:
        ext_path = ICUST_path
    # Load external file
    ext_folder = ext_path + 'admin' + str(files_df['level'][idx]) + '/' 
    de = None
    de = pd.read_csv(ext_folder + file_name_e)
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
i1, i1i = loadfiles('transactions_per_hour')
i1_index = ['hour', 'region']

i1_m = process_pipeline(i1, i1i, i1_index)

# Indicator 2
i2, i2i = loadfiles('unique_subscribers_per_hour')
i2_index = ['hour', 'region']

i2_m = process_pipeline(i2, i2i, i2_index)

# Indicator 3
i3, i3i = loadfiles('unique_subscribers_per_day')
i3_index = ['day', 'region']

i3_m = process_pipeline(i3, i3i, i3_index)

# Indicator 3 district
i3d, i3id = loadfiles('unique_subscribers_per_day', admin = 2)

i3_md = process_pipeline(i3d, i3id, i3_index)

# Indicator 3 country
# i3, i3i = loadfiles('unique_subscribers_per_day.csv')
# i3_index = ['day', 'region']

# i3_m = process_pipeline(i3, i3i, i3_index)

# Indicator 5
i5, i5i = loadfiles('origin_destination_connection_matrix_per_day')
i5_index = ['connection_date', 'region_from', 'region_to']

i5_m = process_pipeline(i5, i5i, i5_index)

# Indicator 5 district
# i5d, i5id = loadfiles('origin_destination_connection_matrix_per_day.csv', admin = 2)

# i5_md = process_pipeline(i5d, i5id, i5_index)

# Indicator 7 
i7,i7i = loadfiles('mean_distance_per_day')
i7_index = ['home_region', 'day']

i7_m = process_pipeline(i7, i7i, i7_index)

# Indicator 7 district level
i7d,i7id = loadfiles('mean_distance_per_day', admin = 2)

i7_md = process_pipeline(i7d, i7id, i7_index)

# Indicator 8
i8, i8i = loadfiles('mean_distance_per_week') 
i8_index = ['home_region', 'week']

i8_m = process_pipeline(i8, i8i, i8_index)

# Indicator 8 district
i8d, i8id = loadfiles('mean_distance_per_week', admin = 2) 

i8_md = process_pipeline(i8d, i8id, i8_index)

# Indicator 9
i9, i9i = loadfiles('week_home_vs_day_location_per_day', admin = 2)
i9_index = ['region', 'home_region', 'day']

# Fix i9 district id
i9 = clean(i9, i9_index)
i9i = clean(i9i, i9_index)
i9['home_region'] = i9['home_region'].astype(int)
i9i['home_region'] = i9i['home_region'].astype(int)

i9_m = process_pipeline(i9, i9i, i9_index, do_clean = False)

#-----------------------------------------------------------------#
# Export intersection

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
    #export(i5_m, 'i5_admin3', path = OUT_hfcs + 'Sheet intersections/')
    export(i7_m, 'i7_admin3', path = OUT_hfcs + 'Sheet intersections/')
    export(i7_md, 'i7_admin2', path = OUT_hfcs + 'Sheet intersections/')
    export(i8_m, 'i8_admin3', path = OUT_hfcs + 'Sheet intersections/')
    export(i8_md, 'i8_admin2', path = OUT_hfcs + 'Sheet intersections/')
    export(i9_m, 'i9_admin2', path = OUT_hfcs + 'Sheet intersections/')

#-----------------------------------------------------------------#
# Create differences tables

def diff_dataset(data,
                 col1,
                 col2,
                 col3 = None,
                 col4 = None):
    if col3 is None:
        diff_data = data[data[col1] != data[col2]]
    else: 
        diff_data = data[(data[col1] != data[col2]) | (data[col3] != data[col4])] 
    return diff_data

i1_m_diff = diff_dataset(i1_m, 'count', 'count_ecnt')
i2_m_diff = diff_dataset(i2_m, 'count', 'count_ecnt')
i5_m_diff = diff_dataset(i5_m, 'total_count', 'total_count_ecnt', 'subscriber_count', 'subscriber_count_ecnt')
i7_m_diff = diff_dataset(i7_m, 'mean_distance', 'mean_distance_ecnt')
i7_m_diffd = diff_dataset(i7_md, 'mean_distance', 'mean_distance_ecnt')
i8_m_diff = diff_dataset(i8_m, 'mean_distance', 'mean_distance_ecnt')
i8_m_diffd = diff_dataset(i8_md, 'mean_distance', 'mean_distance_ecnt')
i9_m_diff = diff_dataset(i9_m, 'count', 'count_ecnt', 'mean_duration', 'mean_duration_ecnt')


if EXPORT:
    export(i1_m_diff, 'i1_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i2_m_diff, 'i2_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i5_m_diff, 'i5_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i7_m_diff, 'i7_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i7_m_diffd,'i7_admin2', path = OUT_hfcs + 'Sheet differences/')
    export(i8_m_diff, 'i8_admin3', path = OUT_hfcs + 'Sheet differences/')
    export(i8_m_diffd, 'i8_admin2', path = OUT_hfcs + 'Sheet differences/')
    export(i9_m_diff, 'i9_admin2', path = OUT_hfcs + 'Sheet differences/')

#-----------------------------------------------------------------#
# Comparisson panel 

# This is a panel containig both data from internal and 
# external indicators. 

def comp_panel(d,
               de,
               index_cols,
               how = 'outer'):
    # Full join
    md = d.merge(de,
                 on = index_cols, 
                 how = how,
                 suffixes=('', '_ecnt'))
    d = clean(d, index_cols)
    de = clean(de, index_cols)
    # Columns that are not indexes
    variables =  list(set(d.columns) - set(index_cols))
    # Create full panel variables
    for c in variables:
        md[c + '_p'] = md[c].fillna(md[c + '_ecnt'])
    return md.sort_values(index_cols).dropna(subset= index_cols)  

i1_cpanel = comp_panel(i1, i1i, i1_index)
i2_cpanel = comp_panel(i2, i2i, i2_index)
i3_cpanel = comp_panel(i3, i3i, i3_index)
i3_cpaneld = comp_panel(i3d, i3id, i3_index)
i5_cpanel = comp_panel(i5, i5i, i5_index)

if EXPORT:
    export(i1_cpanel, 'i1_admin3', path = OUT_hfcs + 'Sheet comp panel/')
    export(i2_cpanel, 'i2_admin3', path = OUT_hfcs + 'Sheet comp panel/')
    export(i3_cpanel, 'i3_admin3', path = OUT_hfcs + 'Sheet comp panel/')
    export(i3_cpaneld,'i3_admin2', path = OUT_hfcs + 'Sheet comp panel/')
    #export(i5_cpanel, 'i5_admin3', path = OUT_hfcs + 'Sheet comp panel/')


#-----------------------------------------------------------------#
# Simple panel

# A panel with no intersection of internal and external 
# indicators and a ad hoc appending date

# Slice internal up to 15ht of april
append_date =  dt.date(2020, 3, 7)

def simp_panel(d,
               de,
               index_cols,
               countvars,
               append_date = append_date,
               timevar = None,
               how = 'outer'):
    if timevar is None:
        timevar = index_cols[0]
    # Clean
    d = clean(d, index_cols)
    de = clean(de, index_cols)
    # Join
    md = d.merge(de,
                 on = index_cols, 
                 how = how,
                 suffixes=('', '_ecnt'))
    # Replace count values with internal until the 7th of march and 
    # external after
    for var in countvars:
        md[var] = np.where(pd.to_datetime(md[timevar]).dt.date <= append_date, 
                   md[var], 
                   md[var + '_ecnt'])
    # Remove other columns
    md = md.filter(regex=r'^((?!_ecnt).)*$')
    # Return
    return md.sort_values(index_cols).dropna(subset= index_cols)


i1_panel = simp_panel(i1, i1i, i1_index, countvars = ['count'])
i2_panel = simp_panel(i2, i2i, i2_index, countvars = ['count'])
i3_panel = simp_panel(i3, i3i, i3_index, countvars = ['count'])
i5_panel = simp_panel(i5, i5i, i5_index, countvars = ['subscriber_count', 'od_count',  'od_count_seven',  'od_count_one', 'total_count'])
i7_panel  = simp_panel(i7, i7i, i7_index, countvars = ['mean_distance', 'stdev_distance'], timevar='day')
# i8_panel  = simp_panel(i8, i8i, i8_index, countvars = ['mean_distance', 'stdev_distance'], timevar='week')
i9_panel  = simp_panel(i9, i9i, i9_index, countvars = ['stdev_duration', 'mean_duration', 'count'], timevar='day')



if EXPORT:
    export(i1_panel, 'i1_admin3', path = OUT_panel)
    export(i2_panel, 'i2_admin3', path = OUT_panel)
    export(i3_panel, 'i3_admin3', path = OUT_panel)
    export(i5_panel, 'i5_admin3', path = OUT_panel)
    export(i7_panel, 'i7_admin3', path = OUT_panel)
    export(i9_panel, 'i9_admin2', path = OUT_panel)

#-----------------------------------------------------------------#
# DRAFT
