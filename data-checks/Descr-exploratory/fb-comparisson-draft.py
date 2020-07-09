#-----------------------------------------------------------------#
# CDR vs FB comparisson
#-----------------------------------------------------------------#
# TO DO 


# Read documentation

# Do same process for  movement data

# Look at the results

# Do the merging  with only overlaping dates

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

OUT_path = base_path + '/outputs/data-checks/'

data_pop = fb_data + 'Population Administrative Regions/'
data_mov = fb_data + 'Movement Admin Regions/'

# prefix = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Movement between Administrative Regions__'
prefix_pop = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Facebook Population (Administrative Regions)__'
prefix_mov = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Movement between Administrative Regions__'


#-----------------------------------------------------------------#
# Load FB data

# Population - Load all csv files in the folder
files_pop = glob.glob(data_pop + prefix_pop + "*.csv")
files_mov = glob.glob(data_mov + prefix_mov + "*.csv")
fpop = pd.concat([pd.read_csv(f, encoding='latin1') for f in files_pop], ignore_index=True)
fmov = pd.concat([pd.read_csv(f, encoding='latin1') for f in files_mov], ignore_index=True)


# df1 = pd.read_csv(data_pop + prefix + '2020-06-24 0000.csv')
# df2 = pd.read_csv(data_pop + prefix + '2020-06-24 0800.csv')
# df3 = pd.read_csv(data_pop + prefix + '2020-06-24 1600.csv')

#-----------------------------------------------------------------#
# Load CDR data

# Using i3, Count of unique subscribers, for now. Not sure how this
# fb indicator was calculated, so it might make sense to use another
# indicator
cpop = pd.read_csv(cdr_path + 'i3_admin2.csv')


# i5
cmov = pd.read_csv(cdr_path + 'i5_admin2.csv')

# load and merge keys for str name matching
a2_keys = pd.read_csv(doc_path + 'keys_districts.csv')
a2_keys = a2_keys[['id2', 'name2']]

# process cdr population
cp = cpop.merge(a2_keys,
                left_on= 'region',
                right_on = 'id2')

cp['date'] = pd.to_datetime(cp['day']).dt.date

cp = cp[['date', 'id2', 'name2','count_p']]\
            .rename(columns = {'name2' : 'name',
                               'count_p' : 'count'})

# process cdr movement

cmov['date'] = pd.to_datetime(cmov['connection_date']).dt.date


cm = cmov\
    .merge(a2_keys,
           left_on = 'region_from',
           right_on= 'id2')\
    .rename(columns = {'name2' : 'st_name'})\
    .merge(a2_keys,
           left_on = 'region_to',
           right_on= 'id2')\
    .rename(columns = {'name2' : 'ed_name',
                       'total_count_p' : 'count'})\
    [['date', 'st_name','ed_name', 'count']]
    

#-----------------------------------------------------------------#
# Process FB data

def process(df, time, group_by, count):
    # Remove other countried
    df = df.loc[df['country'] == 'ZW']
    # Date var
    df['date'] = pd.to_datetime(df[time]).dt.date
    # Group by
    gby = ['date']
    gby.extend(group_by)
    # Aggregate 
    agg = df\
        .groupby(gby)\
        .agg({count : np.sum})\
        .reset_index()
    
    return agg


fp = process(fpop, 'date_time', ['polygon_name'], 'n_crisis')\
        .rename(columns = {'polygon_name' : 'name',
                           'n_crisis' : 'count'})
fm = process(fmov, 'date_time', ['start_polygon_name', 'end_polygon_name'], 'n_crisis')\
        .rename(columns = {'start_polygon_name' : 'st_name',
                           'end_polygon_name' : 'ed_name',
                           'n_crisis' : 'count'})

#-----------------------------------------------------------------#
# Merge

# Make sure I'm comparing same period
overlapping_dates = set(cp['date']).intersection(set(fp['date']))

fp = fp[fp['date'].isin(overlapping_dates)]
cp = cp[cp['date'].isin(overlapping_dates)]

# String matching corrections
fp['name'].loc[fp['name'] == 'Hwedza'] = 'Wedza'
fp['name'].loc[fp['name'] == 'Chirumanzu'] = 'Chirumhanzu'
fp['name'].loc[fp['name'] == 'Bulilimamangwe'] = 'Bulilima (North)'



# def agg_rank(df, gby = 'name'):
#     df = df.groupby(gby).agg('mean').reset_index()
#     df["rank"] = df["count"].rank(ascending = False) 
#     return df.sort_values('rank')

# foo


# full_period_comp = cp\
#     .merge(fp, 
#            on = ['name', 'date'], 
#            how = 'outer',
#            suffixes=('', '_fb'))\
#     .sort_values('rank')




#-----------------------------------------------------------------#
# Aggregated merge

# Create full period ranking
def agg_rank(df, gby = 'name'):
    df = df.groupby(gby).agg('mean').reset_index()
    df["rank"] = df["count"].rank(ascending = False) 
    return df.sort_values('rank')

cp_rank = agg_rank(cp)
fp_rank = agg_rank(fp)



full_period_comp = cp_rank\
    .merge(fp_rank, 
           on = 'name', 
           how = 'outer',
           suffixes=('', '_fb'))\
    .sort_values('rank')
    
#-----------------------------------------------------------------#
# Export

# full_period_comp.to_csv(OUT_path + 'i3_fb_comp.csv',
#                         index = False)


# agg_rank(fm, ['st_name', 'ed_name'])
# agg_rank(cm, ['st_name', 'ed_name'])

fp_rank.sort_values('name')