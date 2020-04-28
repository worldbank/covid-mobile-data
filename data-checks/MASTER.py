#-----------------------------------------------------------------#
# DATA CHECKS MASTER
#-----------------------------------------------------------------#

#-----------------------------------------------------------------#
#### Settings


#-----------------------------------------------------------------#
#### Set file paths

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/"

DATA_DB_raw_indicators = DATA_path + "databricks-results/zw/"
DATA_dashboad_clean = DATA_path + "/files_for_dashboard/files_clean/"


DATA_dash_clean_a2 = DATA_dashboad_clean + "adm2/"
DATA_dash_clean_a3 = DATA_dashboad_clean + "adm3/"

#---------------#
# Main indicators

# Transactions per hour
I1_path = DATA_DB_raw_indicators + "indicator 1/"
I1_Adm3_path = I1_path + "admin3/"

# OD matrix
I5_path = DATA_DB_raw_indicators + "indicator 5/"
I5_Adm2_path = I5_path + "admin2/"
I5_Adm3_path = I5_path + "admin3/"

# Flowminder indicators
FLOWM_path = DATA_DB_raw_indicators + "flowminder indicators/"
FLOWM_adm2_path = FLOWM_path + "admin2/"
FLOWM_adm3_path = FLOWM_path + "admin3/"