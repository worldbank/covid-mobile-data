
# ---------------------------------------------------------
# Data checker class definition


# ---------------------------------------------------------
# Settings
import os
import pandas as pd
import numpy as np
import plotly
import plotly.graph_objects as go



class checker:
    """
    This class contains loads aggregated indicators and runs basic
    checks.
    """
    def __init__(self,
                path,
                outputs_path = None,
                level = None,
                ind_dict = None,
                col_names_dict = None,
                export_plots = True,
                htrahshold = -3):
        # Set data path
        if level is None:
            self.path = path
        else:
            self.path = path + '/' + level
        if outputs_path is None:
            self.outputs_path = self.path + '/' + 'out'
            if not os.path.exists(self.outputs_path):
                os.mkdir(self.outputs_path)
        # List files in path
        self.files = os.listdir(self.path)
        self.ind_dict_postfix = {
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
       
        # Indicator default file names
        # Set the self.ind_dict as empty dictonary first
        self.ind_dict = {}
        # For each file in path, check if the filename ends with one of vaulues in self.ind_dict_postfix dictionary
        # if matching, then update the key in self.ind_dict as the key in self.ind_dict_postfix and value as the file name 
        if ind_dict is None:
            for file in self.files:
                for key in self.ind_dict_postfix.keys():
                    if file.endswith(self.ind_dict_postfix[key]):
                        # print('Matching')
                        self.ind_dict[key] = file 
        # Otherwise specify dict manually                
        else:
            self.ind_dict = ind_dict
                         
        # Check if files exist
        files_bol = all([os.path.isfile(self.path + '/' + self.ind_dict[key]) for key in self.ind_dict.keys()])
        assert files_bol,"Some indicators don't exist. Check defaults or set ind_dict"
        
        # Indicator default column names
        # Construct column names of each indicator as key-value pairs 
        # Column names of some indicators do not have keys yet. Will continue working on it.
        if col_names_dict is None:
            self.col_names_dict = {
                    'i1_col_names': {'Time':'hour', 'Geography':'region','Count':'count'},
                    'i2_col_names': {'Time':'hour','Geography':'region','Count':'count'},
                    'i3_col_names': {'Time':'day','Geography':'region','Count':'count'},
                    'i4_col_names': ('day','count','percent_active'),
                    'i5_col_names': {'Time':'connection_date','Geography_01':'region_from','Geography_02':'region_to',
                                     'Subcrib_Count':'subscriber_count',
                                     'OD_Count':'od_count','Total_Count':'total_count'},
                    'i6_col_names': ('week','home_region','count'),
                    'i7_col_names': ('home_region','day','mean_distance','stdev_distance'),
                    'i8_col_names': ('home_region','week','mean_distance','stdev_distance'),
                    'i9_col_names': ('day','region','home_region','mean_duration','stdev_duration','count'),
                    'i10_col_names': ('day','region','region_lag','total_duration_destination',
                                      'avg_duration_destination','count_destination','stddev_duration_destination'
                                      'total_duration_origin','avg_duration_origin','count_origin',
                                      'stddev_duration_origin'),
                    'i11_col_names': ('month','home_region','count')
        }
        # Otherwise specify dict manually
        else:
            self.col_names_dict = col_names_dict
            
        # Constants
        self.missing_values = [99999, '99999', '', None]
        self.htrahshold = htrahshold
        
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
        def remove_missings(df, regionvar = self.col_names_dict['i1_col_names']['Geography'], missing_values = self.missing_values):
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
            .groupby(['date', self.col_names_dict['i1_col_names']['Time']])\
            .agg({self.col_names_dict['i1_col_names']['Geography'] : pd.Series.nunique ,
                self.col_names_dict['i1_col_names']['Count'] : np.sum})\
            .reset_index()\
            .sort_values(['date', self.col_names_dict['i1_col_names']['Time']])\
            .rename(columns = {self.col_names_dict['i1_col_names']['Geography'] : 'n_regions'})
        self.i1_date = remove_missings(self.i1)\
            .groupby('date')\
            .agg({self.col_names_dict['i1_col_names']['Geography'] : pd.Series.nunique ,
                self.col_names_dict['i1_col_names']['Count'] : np.sum})\
            .reset_index()\
            .sort_values(['date'])\
            .rename(columns = {self.col_names_dict['i1_col_names']['Geography'] : 'n_regions'})
        # Complete dates
        self.i1_date = time_complete(self.i1_date, 'date')
        self.i1_hour = time_complete(self.i1_hour, self.col_names_dict['i1_col_names']['Time'], 'H')
        
        
        # Indicator 5
        i5_nmissing = remove_missings(remove_missings(self.i5,self.col_names_dict['i5_col_names']['Geography_01']), 
                                      self.col_names_dict['i5_col_names']['Geography_02'])
        self.i5_date = i5_nmissing\
        .groupby('date')\
        .agg({self.col_names_dict['i5_col_names']['Geography_01'] : pd.Series.nunique ,
              self.col_names_dict['i5_col_names']['Geography_02'] : pd.Series.nunique,
              self.col_names_dict['i5_col_names']['Total_Count'] : np.sum})\
        .reset_index()\
        .sort_values('date')
        
        self.i5_date = time_complete(self.i5_date, 'date')
    
    # ---------------------------------------------------------
    # Plots
    
    def plot_i1_count(self, show = True):
        fig = go.Figure(data=go.Scatter(x=self.i1_date.index, 
                                        y=self.i1_date[self.col_names_dict['i1_col_names']['Count']]))
        fig.update_layout(title_text="Indicator 1: Total number of transactions.")
        
        file_name = self.outputs_path + '/' + 'i1_count.html'
        print('Saving: ' + file_name)
        plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
        
    def plot_i1_n_regions(self, show = True):
        fig = go.Figure(data=go.Scatter(x=self.i1_date.index, y=self.i1_date['n_regions']))
        fig.update_layout(title_text="Indicator 1: Number of unique regions.")
        
        file_name = self.outputs_path + '/' + 'i1_n_region.html'
        print('Saving: ' + file_name)
        plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    
    def plot_i5_count(self, show = True):
        fig = go.Figure(data=go.Scatter(x=self.i5_date.index, 
                                        y=self.i5_date[self.col_names_dict['i5_col_names']['Total_Count']]))
        fig.update_layout(title_text="Indicator 5: Total number of movements.")
        
        file_name = self.outputs_path + '/' + 'i5_count.html'
        print('Saving: ' + file_name)
        plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    def plot_i5_region_count(self, show = True):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=self.i5_date.index, 
                                 y=self.i5_date[self.col_names_dict['i5_col_names']['Geography_02']], marker=dict(color="blue"), name = 'Destination regions'))
        fig.add_trace(go.Scatter(x=self.i5_date.index, 
                                 y=self.i5_date[self.col_names_dict['i5_col_names']['Geography_01']], marker=dict(color="red"), name = 'Origin regions'))
        fig.update_layout(title_text="Indicator 5: Number of unique regions.")
        
        file_name = self.outputs_path + '/' + 'i5_region_count.html'
        print('Saving: ' + file_name)
        plotly.offline.plot(fig, filename= file_name , auto_open=False)
        if show:
            fig.show()
    
    def plot_region_missings(self, show = True):
        n_missing = self.i1[self.col_names_dict['i1_col_names']['Geography']].isin(self.missing_values).sum() 
        labels = ['Missing region','Non-missing region']
        values = [n_missing, len(self.i1) - n_missing]
        
        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.3)])
        fig.update_layout(title_text="Indicator 1: Number of transactions not mapped to region.")
        
        file_name = self.outputs_path + '/' + 'region_missings.html'
        print('Saving: ' + file_name)
        plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    # ---------------------------------------------------------
    # Check pipelines 
    def completeness_checks(self):
        self.plot_region_missings()
        self.plot_i1_count()
        self.plot_i1_n_regions()
        self.plot_i5_count()
        self.plot_i5_region_count()
        
    # USAGE OUTILERS: Indicator wards and days with towers down
    def usage_outliers(self, htrahshold = None):
        data = self.i1
        if htrahshold is None:
             htrahshold = self.htrahshold
        # Number of hours with transactions per region day
        hours_per_day = data.groupby([self.col_names_dict['i1_col_names']['Geography'], 'date']).size()
        hours_per_day = hours_per_day.reset_index() # ger regions to be a column
        hours_per_day.columns = ['region', 'date', 'hcount']
    
        # Average hours per day per region
        avg_hours = (hours_per_day.groupby(['region'])
            .mean()
            .rename(columns={'hcount' :'avg_hours' }))
        # Create region day data set
        i1_ag_df = hours_per_day.merge(avg_hours,
                                        on = 'region')
        
        # Difference from average usage per hour
        i1_ag_df['h_diff'] = i1_ag_df['hcount'] - i1_ag_df['avg_hours']
        
        # Create data only with pairs of wards and days potential 
        # towers down
        i1_ag_df_tower_down = i1_ag_df[i1_ag_df['h_diff'] < htrahshold]
        
        #----------------------------------------------------------------------
        # Export
        # Read me text
        readme_text = "This file contains a combinations of wards and  days that are assumed to have a tower down."
        readme_text += "If a day has " + str(abs(htrahshold))  
        readme_text += " hours with any calls below the daily avergage for that ward,"
        readme_text += " it is considered to have a trower down at some point that day."  
        
        file_name = self.outputs_path + '/' + 'days_wards_with_low_hours_i1.csv'
        print('Saving: ' + file_name)
        (i1_ag_df_tower_down.to_csv(file_name, index = False) )
        # Read me file
        file = open(self.outputs_path + '/' + "days_wards_with_low_hours_i1.txt", "w") 
        file.write(readme_text) 
        file.close() 






# plotly.offline.plot(fig, filename = data + '/' + 'filename.html', auto_open=False)
