

default_levels_dict = {1: [3], 
                       2: [2,3],
                       3: [2,3],
                       4: ['country'],
                       5: [2,3,'tower-tc_bulawayo-harare', 'tc_bulawayo'],
                       6: [3],
                       7: [2,3],
                       8: [2,3],
                       9: [2,3],
                    #    9: [2,3,'tower-cluster-harare', 'tower-cluster-bulawayo'],
                       10: [2,3],
                       11: [2,3]}


default_indicators_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]


# Define panel constructor class
class panel_constructor:
    """
    This class contains loads files for specified indicators and creates
    panel data sets combining all the files 
    """
    def __init__(self, 
                 indicators_list,
                 ilevels_dict = default_levels_dict):
        self.indicators_list = indicators_list
        self.ilevels_dict = ilevels_dict
        # Load indicators:
        # 1. Transactions per hour
        if 1 in self.indicators_list:
            self.i1 = i_indicator(num = 1,  index_cols = ['hour', 'region'])
        # 2. Unique subscribers per hour
        if 2 in self.indicators_list:
            self.i2 = i_indicator(num = 2,  index_cols = ['hour', 'region'])
        
        # 3. Unique subscribers per day 
        if (3 in self.indicators_list) & (3 in self.ilevels_dict[3]):
            self.i3 = i_indicator(num = 3,  index_cols = ['day', 'region'])
        if (3 in self.indicators_list) & (3 in self.ilevels_dict[3]):
            self.i3_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2)
        
        # 4. Proportion of active subscribers
        # if 4 in self.indicators_list:
        #     self.i4
        
        # 5 - Connection Matrix
        if (5 in self.indicators_list) & (3 in self.ilevels_dict[5]):
            self.i5 = i_indicator(num = 3,  index_cols = ['day', 'region'])
        if (5 in self.indicators_list) & (2 in self.ilevels_dict[5]):
            self.i5_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2)
        if (5 in self.indicators_list) & ('tc-harare' in self.ilevels_dict[5]):
            self.i5_tc_harare = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc-harare')
        if (5 in self.indicators_list) & ('tc_bulawayo' in self.ilevels_dict[5]):  
            self.i5_tc_bulawayo = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_bulawayo')
        
        # 6. Unique subscribers per home location
        if 6 in self.indicators_list:
            self.i6 = i_indicator(num = 6,  index_cols = ['week', 'home_region'])
        
        # 7. Mean and Standard Deviation of distance traveled per day (by home location)
        if (7 in self.indicators_list) & (3 in self.ilevels_dict[7]):
            self.i7 = i_indicator(num = 7,  index_cols =['day','home_region'], level = 3)
        if (7 in self.indicators_list) & (2 in self.ilevels_dict[7]):
            self.i7_2 = i_indicator(num = 7,  index_cols =['day','home_region'], level = 2)
        
        # 8. Mean and Standard Deviation of distance traveled per week (by home location)
        if (8 in self.indicators_list) & (3 in self.ilevels_dict[8]):
            self.i8 = i_indicator(num = 8,  index_cols =['week','home_region'], level = 3)
        if (8 in self.indicators_list) & (2 in self.ilevels_dict[8]):
            self.i8_2 = i_indicator(num = 8,  index_cols =['week','home_region'], level = 2)
        # 9. Daily locations based on Home Region with average stay time and SD of stay time
        if (9 in self.indicators_list) & (3 in self.ilevels_dict[9]):
            self.i9 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 3)
        if (9 in self.indicators_list) & (2 in self.ilevels_dict[9]):
            self.i9_2 = i_indicator(num = 9,  index_cols =['day', 'region', 'home_region'], level = 2)
        
        # 10. Simple OD matrix with duration of stay
        if (10 in self.indicators_list) & (3 in self.ilevels_dict[10]):
            self.i10 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 3)
        if (10 in self.indicators_list) & (2 in self.ilevels_dict[10]):
            self.i10_2 = i_indicator(num = 10,  index_cols =['day', 'region', 'region_lag'], level = 2)
        
        # 11. Monthly unique subscribers per home region
        if (11 in self.indicators_list) & (3 in self.ilevels_dict[11]):
            self.i11 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 3)
        if (11 in self.indicators_list) & (2 in self.ilevels_dict[11]):
            self.i11_2 = i_indicator(num = 11,  index_cols =['month', 'home_region'], level = 2)


bar = panel_constructor([5])
bar.i11.data

i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tc_bulawayo')

dic1 = {'Name': 'John', 'Time': 'morning'}
'i'.join(map('_'.join, dic1.items()))

# a = {1: [3], '2': [2,3],'4': ['country'], 5: [2,3,'harare-tc']}

# a[5]

