
import os
import pandas as pd
import numpy as np
import datetime as dt
import time

from bokeh.plotting import figure, output_file, show
from bokeh.models import Span
from bokeh.io import export_png

#-----------------------------------------------------------------#
# Folder structure
DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
DATA_POC = DATA_path + "proof-of-concept/"
DATA_Panel = DATA_POC + "panel_indicators/"
OUT_path = DATA_POC + "outputs/"


#-----------------------------------------------------------------#
# Load data

i5 = pd.read_csv(DATA_Panel + 'i5_admin2.csv')


# org_path = 'C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/isaac-results/indicator-5/admin2/'
# org_data = org_path + '2020_04_origin_destination_connection_matrix_per_day.csv'
# i5 = pd.read_csv(org_data)

#-----------------------------------------------------------------#
# Process data
i5 = i5[['connection_date', 'region_from', 'region_to', 'od_count_p', 'subscriber_count_p', 'total_count_p']]

i5['date'] = pd.to_datetime(i5['connection_date']).dt.date
i5['month'] = pd.to_datetime(i5['connection_date']).dt.month


i5_agg = i5\
        .groupby('date')\
        .agg({'region_from' : pd.Series.nunique ,
              'region_to' : pd.Series.nunique,
              'subscriber_count_p' : np.sum,
              'total_count_p' : np.sum})\
        .reset_index()\
        .sort_values('date')

i5_agg_month = i5\
        .groupby('month')\
        .agg({'subscriber_count_p' : np.sum,
              'total_count_p' : np.sum})\
        .reset_index()\
        .sort_values('month')

#-----------------------------------------------------------------#
# Plot

p = figure(title="Total Daily Movement Between Districts on a Given Day",
           plot_width=800, 
           plot_height=500,
           x_axis_type='datetime')
p.circle(i5_agg['date'], 
         i5_agg['subscriber_count_p'])

# Add lockdown dates vertical line

vline1 = Span(location= dt.date(2020, 3, 27), 
             dimension='height', 
             line_color='black',
             line_dash='dashed')
vline2 = Span(location= dt.date(2020, 3, 30), 
             dimension='height', 
             line_color='black',
             line_dash='dashed')

p.renderers.extend([vline1, vline2])

# Additional formatting
p.left[0].formatter.use_scientific = False
p.toolbar.logo = None
p.toolbar_location = None
p.xaxis.axis_label = "Date"
p.yaxis.axis_label = "Movement Day"
p.title.text_font_size = '15pt'
p.xaxis.axis_label_text_font_size = "12pt"
p.yaxis.axis_label_text_font_size = "12pt"
p.yaxis.major_label_text_font_size = "10pt"
p.xaxis.major_label_text_font_size = "10pt"

# Display plot
show(p)

# Export
export_png(p, 
           filename= OUT_path + "all_movement.png")
