###### Draft script
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import os



def google_data_extraction(data, country_variable_name, country, time_variable_name, month):
    data['month'] = pd.to_datetime(data[time_variable_name]).dt.month
    df = data[(data[country_variable_name] == country) & (data['month'].isin(month))]
    
    return df.sort_values(by = [time_variable_name]).drop('month', axis = 1)


def fb_data_extraction(path, country_variable_name, country, time_variable_name, month):
    files = os.listdir(path)
    
    file0 = pd.read_csv(path + '/' + files[0])
    temp = pd.DataFrame(columns = file0.columns.values.tolist()) 
    for file in files:
        df = pd.read_csv(path + '/' + file)
        currt = google_data_extraction(df, country_variable_name, country, time_variable_name, month)
        combine = pd.concat([temp,currt], ignore_index=True)
        temp = combine
    
    return combine.sort_values(by = [time_variable_name])
    


# ---------- google mobility data

mobility = pd.read_csv('Global_Mobility_Report.csv')

### Uganda
Uganda_google_mobility_04 = google_data_extraction(mobility, 'country_region' ,'Uganda', 'date' ,[4])
Uganda_google_mobility_06 = google_data_extraction(mobility, 'country_region' ,'Uganda', 'date' ,[6])
Uganda_google_mobility_04.to_csv('Uganda_google_mobility_04.csv', index = False)
Uganda_google_mobility_06.to_csv('Uganda_google_mobility_06.csv', index = False)

### Zimbabwe
Zimbabwe_google_mobility_04 = google_data_extraction(mobility,'country_region','Zimbabwe','date', [4])
Zimbabwe_google_mobility_06 = google_data_extraction(mobility,'country_region','Zimbabwe','date', [6])
Zimbabwe_google_mobility_04.to_csv('Zimbabwe_google_mobility_04.csv', index = False)
Zimbabwe_google_mobility_06.to_csv('Zimbabwe_google_mobility_06.csv', index = False)


# ------------ facebook data

#### Uganda
## Population Admin Level

path = "/Users/ruiwenzhang/Desktop/data_comparison/Population Admin Level"
Uganda_fb_population_admin_04 = fb_data_extraction(path, 'country','UG','date_time', [4])
Uganda_fb_population_admin_06 = fb_data_extraction(path, 'country','UG','date_time', [6])
Uganda_fb_population_admin_04.to_csv('Uganda_fb_population_admin_04.csv', index = False)
Uganda_fb_population_admin_06.to_csv('Uganda_fb_population_admin_06.csv', index = False)

## Population Tile level

path = "/Users/ruiwenzhang/Desktop/data_comparison/Population Tile Level"
Uganda_fb_population_tile_04 = fb_data_extraction(path, 'country','UG','date_time', [4])
Uganda_fb_population_tile_06 = fb_data_extraction(path, 'country','UG','date_time', [6])
Uganda_fb_population_tile_04.to_csv('Uganda_fb_population_tile_04.csv', index = False)
Uganda_fb_population_tile_06.to_csv('Uganda_fb_population_tile_06.csv', index = False)


## Movement Tile level 
path = "/Users/ruiwenzhang/Desktop/data_comparison/Movement Tile Level"
Uganda_fb_movement_title_04 = fb_data_extraction(path, 'country','UG','date_time', [4])
Uganda_fb_movement_title_06 = fb_data_extraction(path, 'country','UG','date_time', [6])
Uganda_fb_movement_title_04.to_csv('Uganda_fb_movement_title_04.csv', index = False)
Uganda_fb_movement_title_06.to_csv('Uganda_fb_movement_title_06.csv', index = False)


#### Zimbabwe
### Population Admin
path = "/Users/ruiwenzhang/Desktop/data_comparison/Population Administrative Regions"
Zimbabwe_fb_population_admin_04 = fb_data_extraction(path, 'country','ZW','date_time', [4])
Zimbabwe_fb_population_admin_06 = fb_data_extraction(path, 'country','ZW','date_time', [6])
Zimbabwe_fb_population_admin_04.to_csv('Zimbabwe_fb_population_admin_04.csv', index = False)
Zimbabwe_fb_population_admin_06.to_csv('Zimbabwe_fb_population_admin_06.csv', index = False)


### Movement Tile
path = "/Users/ruiwenzhang/Desktop/data_comparison/Movement Tiles"
Zimbabwe_fb_movement_tile_04 = fb_data_extraction(path, 'country','ZW','date_time', [4])
Zimbabwe_fb_movement_tile_06 = fb_data_extraction(path, 'country','ZW','date_time', [6])
Zimbabwe_fb_movement_tile_04.to_csv('Zimbabwe_fb_movement_tile_04.csv', index = False)
Zimbabwe_fb_movement_tile_06.to_csv('Zimbabwe_fb_movement_tile_06.csv', index = False)



#------------------------------------------------------
########## has a problem: 
########## the format of missing value of fb population tiles for Zimbabwe cannot be encoded. 
########## need to do preprocessing before extraction. Not done yet

### Population Tile
path = "/Users/ruiwenzhang/Desktop/data_comparison/Population Tiles"
Zimbabwe_fb_population_tile_04 = fb_data_extraction(path, 'country','ZW','date_time', [4])
Zimbabwe_fb_population_tile_06 = fb_data_extraction(path, 'country','ZW','date_time', [6])

Zimbabwe_fb_population_tile_04.to_csv('Zimbabwe_fb_population_tile_04.csv', index = False)
Zimbabwe_fb_population_tile_06.to_csv('Zimbabwe_fb_population_tile_06.csv', index = False)











    
    

