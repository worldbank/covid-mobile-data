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

i1_ecnt = indicators.i1_3.panel
i3_ecnt = indicators.i3_3.panel
i5_ecnt = indicators.i5_3.panel


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
    # Final df
    return ndf

index_cols = indicators.i5_3.index_cols

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