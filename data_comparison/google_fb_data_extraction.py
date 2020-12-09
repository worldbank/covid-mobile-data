###### Draft script

import pandas as pd
import numpy as np
import os



def data_extraction(data, country_variable_name, country, time_variable_name, month):
    data['month'] = pd.to_datetime(data[time_variable_name]).dt.month
    
    return data[(data[country_variable_name] == country) & (data['month'].isin(month))]




mobility = pd.read_csv('Global_Mobility_Report.csv')

Uganda_04 = data_extraction(mobility, 'country_region' ,'Uganda', 'date' ,[4])
Uganda_06 = data_extraction(mobility, 'country_region' ,'Uganda', 'date' ,[6])

#Uganda_04_06 = data_extraction(mobility, 'Uganda', [4,6])

Zimbabwe_04 = data_extraction(mobility,'country_region','Zimbabwe','date', [4])
Zimbabwe_06 = data_extraction(mobility,'country_region','Zimbabwe','date', [6])

Uganda_04.to_csv('Uganda_Google_04.csv', index = False)
Uganda_06.to_csv('Uganda_Google_06.csv', index = False)
Zimbabwe_04.to_csv('Zimbabwe_Google_04.csv', index = False)
Zimbabwe_06.to_csv('Zimbabwe_Google_06.csv', index = False)




path = "/Users/ruiwenzhang/Desktop/data_comparison/Population Admin Level"
files = os.listdir(path)

file0 = pd.read_csv(path + '/' + files[0])
temp = pd.DataFrame(columns = file0.columns.values.tolist()) 
for file in files:
    df = pd.read_csv(path + '/' + file)
    currt = data_extraction(df, 'country','UG','date_time', [4,6])
    combine = pd.concat([temp,currt], ignore_index=True)
    temp = combine
    


Uganda_fb_04_06 = combine.to_csv('Uganda_FB_04_06.csv', index = False)






#pop_admin_level_uganda_04 = data_extraction(file, 'country','UG','date_time', [4])
#file['month'] = pd.to_datetime(file['date_time']).dt.month
#pop_admin_level_uganda_04 = file[(file['country'] == 'UG') & (file['month'] == 4)]









    
    

