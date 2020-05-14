#-----------------------------------------------------------------#
# DATA CHECKS MASTER
#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#### Settings


#-----------------------------------------------------------------#
#### Set file paths

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
DATA_POC = DATA_path + "proof-of-concept/"

DATA_DB_raw_indicators = DATA_POC + "databricks-results/zw/"
DATA_dashboad_clean = DATA_POC + "/files_for_dashboard/files_clean/"

DATA_dash_clean_a2 = DATA_dashboad_clean + "adm2/"
DATA_dash_clean_a3 = DATA_dashboad_clean + "adm3/"

#---------------#
# Main indicators

# Transactions per hour
I1_path = DATA_DB_raw_indicators + "indicator 1/"
I1_Adm3_path = I1_path + "admin3/"


# Unique subcribers per hour
I2_path = DATA_DB_raw_indicators + "indicator 2/"
I2_Adm3_path = I2_path + "admin3/"
I2_towercluster_path = I2_path + "tower_cluster/"


# Unique subscribers per day
I3_path = DATA_DB_raw_indicators + "indicator 3/"
I3_Adm3_path = I3_path + "admin3/"


# OD matrix
I5_path = DATA_DB_raw_indicators + "indicator 5/"
I5_Adm2_path = I5_path + "admin2/"
I5_Adm3_path = I5_path + "admin3/"

#---------------------#
# Flowminder indicators
FLOWM_path = DATA_DB_raw_indicators + "flowminder indicators/"
FLOWM_adm2_path = FLOWM_path + "admin2/"
FLOWM_adm3_path = FLOWM_path + "admin3/"


#---------------#
# Outputs
OUT_path = DATA_POC + "outputs/"
OUT_hfcs = OUT_path + "data-checks/"