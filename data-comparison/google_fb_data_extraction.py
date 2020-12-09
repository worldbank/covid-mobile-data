###### Draft script

import pandas as pd
import numpy as np
import os



def google_data_extraction(data, country_variable_name, country, time_variable_name, month):
    data['month'] = pd.to_datetime(data[time_variable_name]).dt.month
    
    return data[(data[country_variable_name] == country) & (data['month'].isin(month))]


def fb_data_extraction(path, country_variable_name, country, time_variable_name, month):
    files = os.listdir(path)
    
    file0 = pd.read_csv(path + '/' + files[0])
    temp = pd.DataFrame(columns = file0.columns.values.tolist()) 
    for file in files:
        df = pd.read_csv(path + '/' + file)
        currt = google_data_extraction(df, country_variable_name, country, time_variable_name, month)
        combine = pd.concat([temp,currt], ignore_index=True)
        temp = combine
    
    return combine
    

# ---------- google mobility data

mobility = pd.read_csv('Global_Mobility_Report.csv')

Uganda_04 = google_data_extraction(mobility, 'country_region' ,'Uganda', 'date' ,[4])
Uganda_06 = google_data_extraction(mobility, 'country_region' ,'Uganda', 'date' ,[6])
#Uganda_04_06 = google_data_extraction(mobility, 'Uganda', [4,6])

Zimbabwe_04 = google_data_extraction(mobility,'country_region','Zimbabwe','date', [4])
Zimbabwe_06 = google_data_extraction(mobility,'country_region','Zimbabwe','date', [6])

Uganda_04.to_csv('Uganda_Google_04.csv', index = False)
Uganda_06.to_csv('Uganda_Google_06.csv', index = False)
Zimbabwe_04.to_csv('Zimbabwe_Google_04.csv', index = False)
Zimbabwe_06.to_csv('Zimbabwe_Google_06.csv', index = False)


# ---------- facebook data
path = "/Users/ruiwenzhang/Desktop/data_comparison/Population Admin Level"
Uganda_fb_04 = fb_data_extraction(path, 'country','UG','date_time', [4])
Uganda_fb_06 = fb_data_extraction(path, 'country','UG','date_time', [6])

Uganda_fb_04.to_csv('Uganda_FB_04.csv', index = False)
Uganda_fb_06.to_csv('Uganda_FB_06.csv', index = False)

#Uganda_fb_04_06 = combine.to_csv('Uganda_FB_04_06.csv', index = False)











    
    

