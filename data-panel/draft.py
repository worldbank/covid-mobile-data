

default_levels_dict = {1: [3], 
                       2: [2,3],
                       3: [2,3],
                       4: ['country'],
                       5: [2,3,'tower-cluster-harare', 'tower-cluster-bulawayo'],
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
        if 3 in self.indicators_list:
            if 3 in ilevels_dict[3]:
                self.i3 = i_indicator(num = 3,  index_cols = ['day', 'region'])
            if 2 in ilevels_dict[3]:
                self.i3_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2)
        # 4. Proportion of active subscribers
        # if 4 in self.indicators_list:
        #     self.i4
        # 5 - Connection Matrix
        if 5 in self.indicators_list:
            if 3 in ilevels_dict[5]:
                self.i5 = i_indicator(num = 3,  index_cols = ['day', 'region'])
            if 2 in ilevels_dict[5]:
                self.i5_2 = i_indicator(num = 3,  index_cols = ['day', 'region'], level = 2)
            if 'tower-cluster-harare' in ilevels_dict[5]:
                i5_tc_h = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tower-cluster-harare')
            if 'tower-cluster-bulawayo' in ilevels_dict[5]:
                i5_tc_b = i_indicator(num = 5,  index_cols = ['connection_date', 'region_from', 'region_to'], level = 'tower-cluster-bulawayo')
        if 6 in self.indicators_list:
            pass
        if 7 in self.indicators_list:
            pass
        if 8 in self.indicators_list:
            pass
        if 9 in self.indicators_list:
            pass
        if 10 in self.indicators_list:
            pass
        if 11 in self.indicators_list:
            pass


bar = panel_constructor([5])

a = {1: [3], '2': [2,3],'4': ['country'], 5: [2,3,'harare-tc']}

a[5]









# Clean panel as methods of the indicator class??


# Define indicator class that contains data for multiple extractions
class i_indicator:
    """
    This class contains information to load indicator files both
    from our original indicators and externally created ones.
    
    load() method loads both datasets
    clean() method removes missings from both datasets
    """
    def __init__(self, 
                 num, 
                 index_cols,
                 level = 3,
                 files_df = indicators_df):
        # self.file_name = file_name
        self.num = num
        self.index_cols = index_cols
        self.level = level
        self.files_df = files_df
        # Call methods when intializing
        self.load()
        self.clean()
    # Load files
    def load(self, full = True):
        idx = (self.files_df['indicator'] == str(self.num)) & (self.files_df['level'] == str(self.level))
        # Internal indicator
        folder = self.files_df['path'][idx].iat[0]
        file_name = self.files_df['file'][idx].iat[0] + '.csv'
        self.data = pd.read_csv(folder + file_name)
        # self.data = folder + file_name
        # External indicators
        if full:
            folder = self.files_df['path_ecnt'][idx].iat[0]
            file_name_03 = '2020_03_' + self.files_df['file'][idx].iat[0] + '.csv'
            file_name_04 = '2020_04_' + self.files_df['file'][idx].iat[0] + '.csv'
            file_name_05 = '2020_05_' + self.files_df['file'][idx].iat[0] + '.csv'
            file_name_06 = '2020_06_' + self.files_df['file'][idx].iat[0] + '.csv'
            self.data_e_03 = pd.read_csv(folder + file_name_03)
            self.data_e_04 = pd.read_csv(folder + file_name_04)
            self.data_e_05 = pd.read_csv(folder + file_name_05)
            self.data_e_06 = pd.read_csv(folder + file_name_06)
    # Clean indicators
    def clean(self):
        self.data = clean(self.data, self.index_cols)
        self.data_e_03 = clean(self.data_e_03, self.index_cols)
        self.data_e_04 = clean(self.data_e_04, self.index_cols)
        self.data_e_05 = clean(self.data_e_05, self.index_cols)
        self.data_e_06 = clean(self.data_e_06, self.index_cols)
    # Internal merge function
    # def out_merge(self, d1, d2, suffix, on = self.index_cols):
    #     return d1.merge(d2, on = on, how = 'outer', suffixes=('', suffix))
    
    # Create panel with other data sets being added as columns. Gambiarra braba !Arrumar!
    def create_panel(self, 
                     time_var, 
                     c_date_1 = np.datetime64(dt.date(2020, 3, 15)),
                     c_date_2 = np.datetime64(dt.date(2020, 4, 1)),
                     c_date_3 = np.datetime64(dt.date(2020, 5, 1)),
                     c_date_4 = np.datetime64(dt.date(2020, 6, 1)) ):
        # kwargs.setdefault('time_var', self.index_cols[0])
        self.panel = self.data\
            .merge(self.data_e_03,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_03'))\
            .merge(self.data_e_04,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_04'))\
            .merge(self.data_e_05,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_05'))\
            .merge(self.data_e_06,
                   on = self.index_cols,
                   how = 'outer',
                   suffixes=('', '_06'))
        # Create panel column
        d1_bol = (self.panel[time_var].astype('datetime64')  >= c_date_1)
        d2_bol = (self.panel[time_var].astype('datetime64')  >= c_date_2)
        d3_bol = (self.panel[time_var].astype('datetime64')  >= c_date_3)
        d4_bol = (self.panel[time_var].astype('datetime64')  >= c_date_4)
        countvars =  list(set(self.data.columns) - set(self.index_cols))
        for var in countvars:
            varname = var + '_p'
            # Base value as our indicator
            self.panel[varname] = self.panel[var]
            # Replace values based on dates
            self.panel.loc[d1_bol, varname] = self.panel.loc[d1_bol, var + '_03'] 
            self.panel.loc[d2_bol, varname] = self.panel.loc[d2_bol, var + '_04']
            self.panel.loc[d3_bol, varname] = self.panel.loc[d3_bol, var + '_05']
            self.panel.loc[d4_bol, varname] = self.panel.loc[d3_bol, var + '_06']
        # Make sure order is fine
        # self.panel.sort_values(self.index_cols)          