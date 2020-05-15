#-----------------------------------------------------------------#
# DATA CHECKS - completeness checks
#-----------------------------------------------------------------#

# This file checks all files to test if all0 dates and hours are 
# present.


flow_a2_file_list = os.listdir(IFLOWM_adm2_path)
flow_a3_file_list = os.listdir(IFLOWM_adm3_path)
cust_a2_file_list = os.listdir(ICUST_adm2_path)
cust_a3_file_list = os.listdir(ICUST_adm3_path)


# Variables
timevar = 'hour'
regvar = 'region'

#-----------------------------------------------------------------#
# Load data

# Start by checking indicator 1
file_name = 'transactions_per_hour.csv'
fi = pd.read_csv(ICUST_adm3_path + file_name) 
# fi = pd.read_csv(I1_Adm3_path + file_name) 

# Process

# Patch cleaning - Removing duplicated header
c1_name = fi.columns[1]
fi_cl = fi[~fi[c1_name].str.contains(c1_name)]

# Remove missings
reg_missings_bol = fi_cl[regvar].isin(['99999','']) 

fi_cl = fi_cl[~reg_missings_bol]


# Date vars
fi_cl['date'] = pd.to_datetime(fi_cl[timevar]).dt.date
fi_cl['hour'] = pd.to_datetime(fi_cl[timevar]).dt.hour

#-----------------------------------------------------------------#
# Countrywide checks


foo = fi_cl\
    .groupby([regvar,'date'])\
    .count()\
    .reset_index()





#-----------------------------------------------------------------#
# Region specific checks


#-----------------------------------------------------------------#
# DRAFT


pd.to_datetime(fi[timevar][440000:445000])
pd.to_datetime(fi[timevar][440650:440700])
fi[440650:440700]



# new_data[timevar] = 
pd.to_pydatetime(fi[timevar])

pd.to_pydatetime('2020-02-17T19:00:00.000Z')

pd.to_datetime('2020-02-17T19:00:00.000Z').dt.date

pd.Timestamp('2020-02-17T19:00:00.000Z').date()

fi.hour.dt

pd.to_datetime(fi[timevar]).dt.hour
