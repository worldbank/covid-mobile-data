
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
        files_bol = all([os.path.isfile(self.path + '/' + ind_names[key]) for key in ind_names.keys()])
        assert files_bol,"Some indicators don't exist. Check defaults or set ind_dict"
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
    
    def remove_missings(df, regionvar = 'region', missing_values = [99999, '99999']):
        return df[~df[regionvar].isin(missing_values)]
    
    # Create data sets with time indexes and fill blanks with 0s
    def time_complete(data, timevar = data.columns[0], timefreq = 'D'):
        data[timevar] = data[timevar].astype('datetime64')
        full_time_range = pd.date_range(data[timevar].min(),  
                                        data[timevar].max(), 
                                        freq = timefreq)
        data = data.set_index(timevar)
        data = data.reindex(full_time_range,  fill_value=0)
        return(data)
    
    # Aggregated i1 versions
    
    
    # ---------------------------------------------------------
    # Plots
    
    # ---------------------------------------------------------
    # Check pipelines
    def completeness_checks():
        pass
    def summary_stats():
        pass
    def towers_down():
        pass
        # self.i1 = pd.read_csv(self.)


foo = checker(path = data, level = 'admin2')

foo.load_indicators()

def aggregation(df, sumvars, uniquevars):
    pass

def remove_missings(df, regionvar = 'region', missing_values = [99999, '99999']):
    return df[~df[regionvar].isin(missing_values)]

remove_missings(foo.i1)


# pd.to_datetime(foo.i1['hour']).dt.date