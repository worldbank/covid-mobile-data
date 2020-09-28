#-----------------------------------------------------------------#
# Add provider to data panel
#-----------------------------------------------------------------#

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
TEL_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/telecel/"

i1_3_tel = pd.read_csv(TEL_path + 'admin3/transactions_per_hour.csv')
i3_2_tel = pd.read_csv(TEL_path + 'admin2/unique_subscribers_per_day.csv')
i3_3_tel = pd.read_csv(TEL_path + 'admin3/unique_subscribers_per_day.csv')

i5_2_tel = pd.read_csv(TEL_path + 'admin2/origin_destination_connection_matrix_per_day.csv')
i5_3_tel = pd.read_csv(TEL_path + 'admin3/origin_destination_connection_matrix_per_day.csv')




# Add function
def add_provider(p1, p2,
                 index_cols, 
                 suffixes = ['_ecnt', '_tel']):
    # Merge datasets
    ndf = p1.merge(p2, on = index_cols,
                   how = 'left', suffixes = suffixes)
    
    # Rename and sum columns
    cols = ndf.filter(like = suffixes[0]).columns.to_list()
    for i in range(0,len(cols)):
        p1_col = cols[i]
        p2_col = cols[i].replace(suffixes[0], '') + suffixes[1]
        final_col = cols[i].replace(suffixes[0], '')
        ndf[final_col] = ndf[p1_col].fillna(0) + ndf[p2_col].fillna(0)
    # Reorder columns
    ncols = ['date']
    ncols.extend(ndf.drop(['date'], axis = 1).columns.to_list())
    ndf = ndf[ncols]
    # Final df
    return ndf

i1_3 = add_provider(indicators.i1_3.panel, i1_3_tel, indicators.i1_3.index_cols)

i3_2 = add_provider(indicators.i3_2.panel, i3_2_tel, indicators.i3_2.index_cols)
i3_3 = add_provider(indicators.i3_3.panel, i3_3_tel, indicators.i3_3.index_cols)

i5_2 = add_provider(indicators.i5_2.panel, i5_2_tel, indicators.i5_2.index_cols)
i5_3 = add_provider(indicators.i5_3.panel, i5_3_tel, indicators.i5_3.index_cols)

# Export
OUT_PATH = DATA_panel_clean + 'mutlple-mnos/'

i1_3.to_csv(OUT_PATH + 'i1_3.csv', index = False)
i3_2.to_csv(OUT_PATH + 'i3_2.csv', index = False)
i3_3.to_csv(OUT_PATH + 'i3_3.csv', index = False)
i5_2.to_csv(OUT_PATH + 'i5_2.csv', index = False)
i5_3.to_csv(OUT_PATH + 'i5_3.csv', index = False)


ndf = indicators.i1_3.panel


indicators.i1_3.add_provider(i1_3_tel)
indicators.i5_2.add_provider(i5_2_tel)


indicators.i5_2.panel


ind_dict = {
                 1 : 'transactions_per_hour.csv',
                 2 : 'unique_subscribers_per_hour.csv',
                 3 : 'unique_subscribers_per_day.csv',
                 4 : 'percent_of_all_subscribers_active_per_day.csv',
                 5 : 'origin_destination_connection_matrix_per_day.csv',
                 6 : 'unique_subscriber_home_locations_per_week.csv',
                 7 : 'mean_distance_per_day.csv',
                 8 : 'mean_distance_per_week.csv',
                 9 : 'week_home_vs_day_location_per_day.csv',
                 10: 'origin_destination_matrix_time_per_day.csv',
                 11: 'unique_subscriber_home_locations_per_month.csv'}

levels_dict = {1: [3],
               2: [3],
               3: [2,3],
               #    4: ['country'],
               #  5: [2,3,'tc_harare', 'tc_bulawayo'],
                5: [2,3],
                6: [3],
                7: [2,3],
                8: [2,3],
                9: [2,3],
                #9: [2,3,'tc_harare', 'tc_bulawayo'],
                #    10: [2,3],
                11: [2,3]}

def admin_prefix(x):
    if x == 2:
        prefix = 'admin2'
    elif x == 3:
        prefix = 'admin3'
    else:
        prefix = x
    return prefix

admin_prefix(2)


mno_suffix = '_tel'
file_name = os.path.join(mno_path,
                         admin_prefix(default_levels_dict[i][j]),
                         ind_dict[i])

attr_name = 'i' + str(i) + '_' + str(default_levels_dict[i][j]) + mno_suffix

# Loop through levels dict values and load attributes
for i in list(levels_dict.keys()):
    for j in range(0, len(levels_dict[i])):
        # print(str(i) + '_' + str(j))
        path = os.path.join(mno_path, 
                            admin_prefix(levels_dict[i][j]),
                            ind_dict[i])
        attr_name = 'i' + str(i) + '_' + str(levels_dict[i][j]) + mno_suffix
        df = pd.read_csv(path)
        print(df.shape)

# indicators.ind_dict
indicators = panel_constructor(levels_dict, indicators_df)
indicators.dirty_panel()
indicators.clean_panel(i1_ag_df_tower_down)

indicators.add_other_provider(mno_path =  "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/telecel/")

indicators.i1_3.panel
getattr(indicators, 'i1_3' + '_tel')