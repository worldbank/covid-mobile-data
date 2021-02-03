# ---------------------------------------------------------
# Settings
import os
import pandas as pd
import numpy as np
import plotly
import plotly.graph_objects as go
import plotly.express as px
import argparse



# ---------------------------------------------------------
class checker:
    """
    This class loads aggregated indicators and runs basic checks.
    
    -------------------------------------------------------------
    Public methods
    
    completeness_checks() : Aggregates indicators on day and country resolutions and produces plots with basic checks.
    
    usage_outliers(htrahshold) : A calculates a proxy for cellphone towers outages and exports a table with occurences.
        - Calculates the average number of active hours per day for each region over the entire period.
        - Flags day-region ocurrences below average - htrashold as having a tower down.
    
    
    """
    def __init__(self,
                path,
                outputs_path = None,
                level = None,
                ind_dict = None,
                prefix = None,
                col_names_dict = None,
                htrahshold = -3):
        """
        Parameters
        ----------
        path : Folder containing CDR indicators
        outputs_path : Folder to save outputs. Defaults to path/out/
        level : Optional subfolder in case there are multiple geographic resolutions.
        self.ind_dict : Dictionary containing indicator file names.
        prefix : Optional file name prefix (e.g. "[YEAR_MONTH]_"),
        col_names_dict : Optional dictionary to specify indicaotors column names.
        htrahshold : Number of hours to qualify as tower down (see usage_outliers() method)
        """
        
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
        
        # File names
        if ind_dict is None:
            self.ind_dict = {
                    'i1' : 'transactions_per_hour.csv',
                    #  'i2' : 'unique_subscribers_per_hour.csv',
                    'i3': 'unique_subscribers_per_day.csv',
                    #  'i4' : 'percent_of_all_subscribers_active_per_day.csv',
                    'i5': 'origin_destination_connection_matrix_per_day.csv'}
                    #  'i6' : 'unique_subscriber_home_locations_per_week.csv',
                    #  'i7': 'mean_distance_per_day.csv',
                    #  'i8' : 'mean_distance_per_week.csv',
                    #  'i9': 'week_home_vs_day_location_per_day.csv',
                    #  'i10' : 'origin_destination_matrix_time_per_day.csv',
                    #  'i11': 'unique_subscriber_home_locations_per_month.csv'}
        else:
            self.ind_dict = ind_dict
        
        if prefix is None:
            pass
        else:
            self.ind_dict = {k:prefix+v for (k,v) in self.ind_dict.items()}
        
        # Check if files exist
        files_bol = all([os.path.isfile(self.path + '/' + self.ind_dict[key]) for key in self.ind_dict.keys()])
        assert files_bol,"Some indicators don't exist. Check defaults or set self.ind_dict"
        
        # Indicator default column names
        # Construct column names of each indicator as key-value pairs 
        # Column names of some indicators do not have keys yet. Will continue working on it.
        if col_names_dict is None:
            self.col_names_dict = {
                    'i1': {'Time':'hour', 
                           'Geography':'region',
                           'Count':'count'},
                    'i3': {'Time':'day',
                           'Geography':'region',
                           'Count':'count'},
                    'i5': {'Time':'connection_date',
                           'Geography_from':'region_from',
                           'Geography_to':'region_to',
                           'Count':'total_count'} }
        # Otherwise specify dict manually
        else:
            self.col_names_dict = col_names_dict
            
        # Constants
        self.missing_values = [99999, '99999', '', None]
        self.htrahshold = htrahshold
        
        # Run data loading and processing methods
        print('Loading data...')
        self.load_indicators()
        print('Processing data...')
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
        if 'i1' in self.ind_dict:
            self.i1 = load(path + self.ind_dict['i1'], timevar = self.col_names_dict['i1']['Time'])
        if 'i3' in self.ind_dict:
            self.i3 = load(path + self.ind_dict['i3'], timevar = self.col_names_dict['i3']['Time'])
        if 'i5' in self.ind_dict:
            self.i5 = load(path + self.ind_dict['i5'], timevar = self.col_names_dict['i5']['Time'])
    
    
    def run_aggregations(self):
        # Missing region remove function
        def remove_missings(df, regionvar, missing_values = self.missing_values):
            return df[~df[regionvar].isin(missing_values)]
        
        # Create data sets with time indexes and fill blanks with 0s
        def time_complete(data, timevar = None, timefreq = 'D'):
            if timevar is None:
                data.columns[0]
            data[timevar] = data[timevar].astype('datetime64[D]')
            full_time_range = pd.date_range(data[timevar].min(),  
                                            data[timevar].max(), 
                                            freq = timefreq)
            data = data.set_index(timevar)
            data = data.reindex(full_time_range,  fill_value=0)
            return(data)
        
        # Indicator 1
        if 'i1' in self.ind_dict:
            # self.i1_hour = remove_missings(self.i1, regionvar = self.col_names_dict['i1']['Geography'])\
            #     .groupby(['date', self.col_names_dict['i1']['Time']])\
            #     .agg({self.col_names_dict['i1']['Geography'] : pd.Series.nunique ,
            #         self.col_names_dict['i1']['Count'] : np.sum})\
            #     .reset_index()\
            #     .sort_values(['date', self.col_names_dict['i1']['Time']])\
            #     .rename(columns = {self.col_names_dict['i1']['Geography'] : 'n_regions'})
            
            self.i1_date = remove_missings(self.i1, regionvar = self.col_names_dict['i1']['Geography'])\
                .groupby('date')\
                .agg({self.col_names_dict['i1']['Geography'] : pd.Series.nunique ,
                    self.col_names_dict['i1']['Count'] : np.sum})\
                .reset_index()\
                .sort_values(['date'])\
                .rename(columns = {self.col_names_dict['i1']['Geography'] : 'n_regions'})
            
            self.i1_date = time_complete(self.i1_date, 'date')
        
        # Indicator 3
        if 'i3' in self.ind_dict:
            self.i3_date = remove_missings(self.i3, regionvar = self.col_names_dict['i3']['Geography'])\
                .groupby('date')\
                .agg({self.col_names_dict['i3']['Geography'] : pd.Series.nunique ,
                    self.col_names_dict['i3']['Count'] : np.sum})\
                .reset_index()\
                .sort_values(['date'])\
                .rename(columns = {self.col_names_dict['i3']['Geography'] : 'n_regions'})
            
            self.i3_date = time_complete(self.i3_date, 'date')
        
        # Indicator 5
        if 'i5' in self.ind_dict:
            i5_nmissing = remove_missings(remove_missings(self.i5, self.col_names_dict['i5']['Geography_from']), 
                                        self.col_names_dict['i5']['Geography_to'])
            self.i5_date = i5_nmissing\
            .groupby('date')\
            .agg({self.col_names_dict['i5']['Geography_from'] : pd.Series.nunique ,
                self.col_names_dict['i5']['Geography_to'] : pd.Series.nunique,
                self.col_names_dict['i5']['Count'] : np.sum})\
            .reset_index()\
            .sort_values('date')
            
            self.i5_date = time_complete(self.i5_date, 'date')
            
            # Remove first day for plots since it doesn't have movements from the day before
            # so it is biased by definition.
            self.i5_date = self.i5_date[~(self.i5_date.index == self.i5_date.index.min())]
    
     # ---------------------------------------------------------
    # Plots
    
    def plot_i1_hist(self, show = True, export = True):
        count = self.col_names_dict['i1']['Count']
        fig = px.histogram(self.i1[count].clip(0, self.i1[count].quantile(0.95)), 
                           x=count, 
                           title='Indicator 1: Hourly calls distribution.<br>(Censored at 95th percentile.)', 
                           labels = {count : 'Number of calls per hour.'})
        print("Plotting indicator 1 histogram...")
        if export:
            file_name = self.outputs_path + '/' + 'i1_hist.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    
    def plot_i1_count(self, show = True, export = True):
        fig = go.Figure(data=go.Scatter(x=self.i1_date.index, 
                                        y=self.i1_date[self.col_names_dict['i1']['Count']]))
        fig.update_layout(title_text="Indicator 1: Total number of transactions.")
        
        print("Plotting indicator 1 daily count series...")

        if export:
            file_name = self.outputs_path + '/' + 'i1_count.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
        
    def plot_i1_n_regions(self, show = True, export = True):
        fig = go.Figure(data=go.Scatter(x=self.i1_date.index, y=self.i1_date['n_regions']))
        fig.update_layout(title_text="Indicator 1: Number of unique regions.")
        
        print("Plotting indicator 1 daily region count...")
        
        if export:
            file_name = self.outputs_path + '/' + 'i1_n_region.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    
    def plot_i3_count(self, show = True, export = True):
        fig = go.Figure(data=go.Scatter(x=self.i3_date.index, 
                                        y=self.i3_date[self.col_names_dict['i3']['Count']]))
        fig.update_layout(title_text="Indicator 3: Total number of daily active subscribers.")
        
        print("Plotting indicator 3 histogram...")
        if export:
            file_name = self.outputs_path + '/' + 'i3_count.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    
    def plot_i3_hist(self, show = True, export = True):
        count = self.col_names_dict['i3']['Count']
        fig = px.histogram(self.i3[count], 
                           x=count, 
                           title='Indicator 3: Active subscribers distribution', 
                           labels = {count : 'Number of active subscribers per day and region.'})
        print("Plotting indicator 3 daily counts...")
        if export:
            file_name = self.outputs_path + '/' + 'i3_hist.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    
    def plot_i5_count(self, show = True, export = True):
        fig = go.Figure(data=go.Scatter(x=self.i5_date.index, 
                                        y=self.i5_date[self.col_names_dict['i5']['Count']]))
        fig.update_layout(title_text="Indicator 5: Total number of movements.")
        print("Plotting indicator 5 daily movement counts...")
        if export:
            file_name = self.outputs_path + '/' + 'i5_count.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    def plot_i5_region_count(self, show = True, export = True):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=self.i5_date.index, 
                                 y=self.i5_date[self.col_names_dict['i5']['Geography_to']], marker=dict(color="blue"), name = 'Destination regions'))
        fig.add_trace(go.Scatter(x=self.i5_date.index, 
                                 y=self.i5_date[self.col_names_dict['i5']['Geography_from']], marker=dict(color="red"), name = 'Origin regions'))
        fig.update_layout(title_text="Indicator 5: Number of unique regions.")
        
        if export:
            file_name = self.outputs_path + '/' + 'i5_region_count.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename= file_name , auto_open=False)
        if show:
            fig.show()
    
    def plot_region_missings(self, show = True, export = True):
        n_missing = self.i1[self.col_names_dict['i1']['Geography']].isin(self.missing_values).sum() 
        labels = ['Missing region','Non-missing region']
        values = [n_missing, len(self.i1) - n_missing]
        
        fig = go.Figure(data=[go.Pie(labels=labels, values=values, hole=.3)])
        fig.update_layout(title_text="Indicator 1: Number of transactions not mapped to region.")
        
        if export:
            file_name = self.outputs_path + '/' + 'region_missings.html'
            print('Saving: ' + file_name)
            plotly.offline.plot(fig, filename = file_name, auto_open=False)
        if show:
            fig.show()
    
        # ---------------------------------------------------------
    # Check pipelines 
    def completeness_checks(self, export = True):
        if 'i1' in self.ind_dict:
            self.plot_i1_hist(export = export)
            self.plot_region_missings(export = export)
            self.plot_i1_count(export = export)
            self.plot_i1_n_regions(export = export)
        if 'i3' in self.ind_dict:
            self.plot_i3_hist(export = export)
            self.plot_i3_count(export = export)
        if 'i5' in self.ind_dict:
            self.plot_i5_count(export = export)
            # self.plot_i5_region_count(export = export)
    
     # USAGE OUTILERS: Indicator wards and days with towers down
    def usage_outliers(self, htrahshold = None):
        data = self.i1
        if htrahshold is None:
             htrahshold = self.htrahshold
        # Number of hours with transactions per region day
        hours_per_day = data.groupby([self.col_names_dict['i1']['Geography'], 'date']).size()
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


# ---------------------------------------------------------
# Run script from the terminal

if __name__ == "__main__":
    
    # Initializ parser
    parser = argparse.ArgumentParser()
    
    # Adding optional argument
    parser.add_argument("-p", "--Path")
    parser.add_argument("--Prefix")
    parser.add_argument("--Output")
    
    # Read arguments from command line
    args = parser.parse_args()
    
    # Create checker instance
    indicators_checker = checker(path = args.Path, prefix = args.Prefix, outputs_path = args.Output)

    #------------------------------------------------------------------------------
    # Export completeness plots
    indicators_checker.completeness_checks()

    #------------------------------------------------------------------------------
    # Export towers down sheet
    indicators_checker.usage_outliers()
