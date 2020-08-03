#-----------------------------------------------------------------#
# Panel utils
#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
# General functions

def clean(data, index_cols):
    na_list = [np.nan, '', '99999', 99999, float("inf")]
    data = data[~data[index_cols].isin(na_list).any(axis ='columns')]
    return(data)

#-----------------------------------------------------------------#
# Clean panel function

# Remove low usage outliers assuming these are towers down and 
# trims columns

def clean_columns(indicator, timevar):
    # Remove comparison columns
    keepcols = copy.deepcopy(indicator.index_cols)
    keepcols.extend(indicator.panel.filter(like='_p', axis=1).columns.to_list())
    new_df = indicator.panel[keepcols]
    # Rename columns
    new_df.columns = new_df.columns.str.strip('_p')
    # Create time variables
    new_df['date'] = pd.to_datetime(new_df[timevar]).dt.date
    return new_df

def remove_towers_down(df, region_vars, outliers_df = i1_ag_df_tower_down):
    # Process outliers file
    outliers_df = copy.deepcopy(i1_ag_df_tower_down) # created in usage_outliers.py
    outliers_df = outliers_df\
        .drop(['hcount', 'avg_hours', 'h_diff'], axis = 1)\
        .rename(columns = {'region':'region_right'})
    outliers_df['flag'] = 1
    # Merge outliers
    if len(region_vars) == 1:
        new_df = df\
            .merge(outliers_df,
                        left_on = ['date', region_vars[0]],
                        right_on = ['date', 'region_right'],
                        how = 'outer')\
            .drop(['region_right'], axis = 1)
    else:
        new_df = df\
            .merge(outliers_df,
                        left_on = ['date', region_vars[0]],
                        right_on = ['date', 'region_right'],
                        how = 'outer')\
            .drop(['region_right'], axis = 1)\
            .merge(outliers_df,
                        left_on = ['date', region_vars[1]],
                        right_on = ['date', 'region_right'],
                        how = 'outer')\
            .drop(['region_right'], axis = 1)
        # Flag if either is true
        new_df['flag'] = ((new_df['flag_x'] == 1) | (new_df['flag_y'] == 1)).astype(int)
        new_df = new_df.drop(['flag_x', 'flag_y'], axis =1)
    # Drop outliers and processual columns
    new_df = new_df[~(new_df['flag'] == 1)].drop(['flag'], axis = 1)
    return new_df

def clean_pipeline(indicator, timevar, region_vars):
    return remove_towers_down( 
                       clean_columns(indicator, 
                                     timevar = timevar), 
                       region_vars = region_vars)