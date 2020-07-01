#-----------------------------------------------------------------#
# CDR vs FB comparisson
#-----------------------------------------------------------------#
# TO DO 

# Load all files

# Aggregate them by day, summing everything

# Match with our files by str IDs

# Ask rob for documentation!

#-----------------------------------------------------------------#
# Settings

import os
import pandas as pd
import numpy as np
import glob

base_path = 'C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/'

fb_data = base_path + 'Facebook Zimbabwe Data/'
cdr_path = base_path + 'panel_indicators/'

doc_path = base_path + 'documentation/'


data_pop = fb_data + 'Population Administrative Regions/'
data_mov = fb_data + 'Movement Admin Regions/'

# prefix = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Movement between Administrative Regions__'
prefix_pop = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Facebook Population (Administrative Regions)__'
prefix_mov = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Movement between Administrative Regions__'


#-----------------------------------------------------------------#
# Load FB data

# Population - Load all csv files in the folder
files = glob.glob(data_pop + prefix_pop + "*.csv")
fpop = pd.concat([pd.read_csv(f, encoding='latin1') for f in files], ignore_index=True)

# df1 = pd.read_csv(data_pop + prefix + '2020-06-24 0000.csv')
# df2 = pd.read_csv(data_pop + prefix + '2020-06-24 0800.csv')
# df3 = pd.read_csv(data_pop + prefix + '2020-06-24 1600.csv')

#-----------------------------------------------------------------#
# Load CDR data

# Using i3, Count of unique subscribers, for now. Not sure how this
# fb indicator was calculated, so it might make sense to use another
# indicator
cpop = pd.read_csv(cdr_path + 'i3_admin2.csv')

# load and merge keys for str name matching
a2_keys = pd.read_csv(doc_path + 'keys_districts.csv')

cp = cpop.merge(a2_keys[['id2', 'name2']],
                  left_on= 'region',
                  right_on = 'id2')

# proces cdr
cp['date'] = pd.to_datetime(cp['day']).dt.date

cp = cp[['date', 'id2', 'name2','count_p']]\
            .rename(columns = {'name2' : 'name',
                               'count_p' : 'count'})

#-----------------------------------------------------------------#
# Process FB data

# Set fb processing function
def process(df, time, region, count):
    # Remove other countried
    df = df.loc[df['country'] == 'ZW']
    # Aggregate by date
    df['date'] = pd.to_datetime(df[time]).dt.date
    agg = df\
        .groupby(['date', region])\
        .agg({count : np.sum})\
        .reset_index()
    
    return agg

fp = process(fpop, 'date_time', 'polygon_name', 'n_crisis')\
        .rename(columns = {'polygon_name' : 'name',
                           'n_crisis' : 'count'})
#-----------------------------------------------------------------#
# Merge

def agg_rank(df):
    df = df.groupby('name').agg('mean')
    df["rank"] = df["count"].rank(ascending = False) 
    return df.sort_values('rank')

agg_rank(cp)
agg_rank(fp)


# naive matching unsing full periods (not intersections)


cp_a = cp.groupby('name').agg('mean')
gp_a = fp.groupby('name').agg('mean')


np.sort(cp['name'].unique())
np.sort(fp['name'].unique())

# 'Hwedza' ? 
# 'Bulilimamangwe' > Bulilima (North)?
# 'Mangwe (South)' ?

cp.merge(fp, on = 'name',
         suffixes = ('_cdr', '_fb')).shape