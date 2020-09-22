
# ---------------------------------------------------------
# To Do

# Number of missings in region columns?
# Plot number of regions with transactions per day
# Plot number of transactions per day
# Plot total number of transactions per hour to check for outliers
# Plot total number of movements per day
# Comparisson between pre and post lockdown stats
# Compare regions that received and sent visitors
#  Tower down outliers

# ---------------------------------------------------------
# Settings
import os
import pandas as pd
import numpy as np
import plotly.graph_objects as go


data = 'C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/telecel'

class checker:
    """
    This class contains loads aggregated indicators and runs basic
    checks.
    """
    def __init__(self,
                path,
                level = None,
                ind_dict = None):
        # Set data path
        if level is None:
            self.path = path
        else:
            self.path = path + '/' + level
        # List files in path
        self.files = os.listdir(self.path)
        # Indicator default file names
        if ind_dict is None:
            self.ind_dict = {
                 'i1' : 'transactions_per_hour.csv',
                 'i2' : 'unique_subscribers_per_hour.csv',
                 'i3': 'unique_subscribers_per_day.csv',
                 'i4' : 'percent_of_all_subscribers_active_per_day.csv',
                 'i5': 'origin_destination_connection_matrix_per_day.csv',
                 'i6' : 'unique_subscriber_home_locations_per_week.csv',
                 'i7': 'mean_distance_per_day.csv',
                 'i8' : 'mean_distance_per_week.csv',
                 'i9': 'week_home_vs_day_location_per_day.csv',
                 'i10' : 'origin_destination_matrix_time_per_day.csv',
                 'i11': 'unique_subscriber_home_locations_per_month.csv'}
        # Otherwise specify dict manually
        else:
            self.ind_dict = ind_dict
        
        # Check if files exist
        files_bol = all([os.path.isfile(self.path + '/' + self.ind_dict[key]) for key in self.ind_dict.keys()])
        assert files_bol,"Some indicators don't exist. Check defaults or set ind_dict"
        
        # Run data loading and processing methods
        self.load_indicators()
        self.run_aggregations()
    # ---------------------------------------------------------
    # Load indicator files
    def load_indicators(self):
        # Loading function
        def load(file_path, timevar = None):
            # Load data
            df = pd.read_csv(file_path)
            # Patch cleannig of headers in the middle of the data
            c1_name = df.columns[0]
            df = df[~df[c1_name].astype(str).str.contains(c1_name)]
            # Convert date vars
            if timevar is None:
                timevar = df.columns[0]
            else:
                timevar = timevar
            # Create date variable
            df['date'] = pd.to_datetime(df[timevar]).dt.date
            return df
        # Load indicators
        path = self.path + '/'
        self.i1 = load(path + self.ind_dict['i1'])
        self.i2 = load(path + self.ind_dict['i2'])
        self.i5 = load(path + self.ind_dict['i5'])
        self.i7 = load(path + self.ind_dict['i7'], 'day')
    
    # ---------------------------------------------------------
    # Aggregations    
    # Aggregated i1 versions
    def run_aggregations(self):
        # Missing region remove function
        def remove_missings(df, regionvar = 'region', missing_values = [99999, '99999']):
            return df[~df[regionvar].isin(missing_values)]
        
        # Create data sets with time indexes and fill blanks with 0s
        def time_complete(data, timevar = None, timefreq = 'D'):
            if timevar is None:
                data.columns[0]
            data[timevar] = data[timevar].astype('datetime64')
            full_time_range = pd.date_range(data[timevar].min(),  
                                            data[timevar].max(), 
                                            freq = timefreq)
            data = data.set_index(timevar)
            data = data.reindex(full_time_range,  fill_value=0)
            return(data)
        
        # Indicator 1
        self.i1_hour = remove_missings(self.i1)\
            .groupby(['date', 'hour'])\
            .agg({'region' : pd.Series.nunique ,
                'count' : np.sum})\
            .reset_index()\
            .sort_values(['date', 'hour'])\
            .rename(columns = {'region' : 'n_regions'})
        self.i1_date = remove_missings(self.i1)\
            .groupby('date')\
            .agg({'region' : pd.Series.nunique ,
                'count' : np.sum})\
            .reset_index()\
            .sort_values(['date'])\
            .rename(columns = {'region' : 'n_regions'})
        
        # Complete dates
        self.i1_date = time_complete(self.i1_date, 'date')
        self.i1_hour = time_complete(self.i1_hour, 'hour', 'H')
        
        # Indicator 5
        i5_nmissing = remove_missings(remove_missings(self.i5,'region_from' ), 'region_to')
        self.i5_date = i5_nmissing\
        .groupby('date')\
        .agg({'region_from' : pd.Series.nunique ,
              'region_to' : pd.Series.nunique,
              'total_count' : np.sum})\
        .reset_index()\
        .sort_values('date')
        
        self.i5_date = time_complete(self.i5_date, 'date')
    
    # ---------------------------------------------------------
    # Plots
    
    # ---------------------------------------------------------
    # Check pipelines
    def completeness_checks(self):
        pass
    def summary_stats(self):
        pass
    def towers_down(self):
        pass
        # self.i1 = pd.read_csv(self.)


foo = checker(path = data, level = 'admin2')

# foo.load_indicators()
# foo.run_aggregations()


import plotly.graph_objects as go
fig = go.Figure(data=go.Scatter(x=foo.i1_date.index, y=foo.i1_date['count']))
fig.show()

fig = go.Figure(data=go.Scatter(x=foo.i1_date.index, y=foo.i1_date['n_regions']))
fig.show()

fig = go.Figure(data=go.Scatter(x=foo.i5_date.index, y=foo.i5_date['total_count']))
fig.show()

foo.i1_hour