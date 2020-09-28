#-----------------------------------------------------------------#
# CREATE PANEL
#-----------------------------------------------------------------#

# This code creates panel datasets combinig different versions of 
# indicator files. 

from utils import *
from panel_constructor import *

#-----------------------------------------------------------------#
# Settings 

EXPORT = False

#-----------------------------------------------------------------#
# Folder structure

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
CODE_path = "C:/Users/wb519128/GitHub/covid-mobile-data/data-panel/"

DATA_POC = DATA_path + "proof-of-concept/"
DATA_panel = DATA_POC + "panel_indicators/"
# DATA_panel_raw = DATA_panel + 'raw/'
DATA_panel_comp = DATA_panel + 'comparisson/'
DATA_panel_clean = DATA_panel + 'clean/'

OUT_hfcs = DATA_POC + "outputs/data-checks/"

#-------------------#
# Indicator dataframe

# Load list of indicators to make it easier to bulk load files
indicators_df = pd\
    .read_csv(DATA_POC + 'documentation/indicators_list.csv')

# Since sheet contains relative paths add path global
# to have absolute paths    
indicators_df['path'] = DATA_path + indicators_df['path']   
indicators_df['path_ecnt'] = DATA_path + indicators_df['path_ecnt']


#-------------------#
# Set default values
# levels_dict = { 1: [3],
#                 2: [3],
#                 3: [2,3],
#                #    4: ['country'],
#                #  5: [2,3,'tc_harare', 'tc_bulawayo'],
#                 5: [2,3],
#                 6: [3],
#                 7: [2,3],
#                 8: [2,3],
#                 9: [2,3],
#                 #9: [2,3,'tc_harare', 'tc_bulawayo'],
#                 10: [2,3],
#                 11: [2,3]}

levels_dict = { 1: [3],
                10: [2,3]}

#-----------------------------------------------------------------#
# Load indicators and create comparisson "dirty" panel

indicators = panel_constructor(levels_dict, indicators_df)

# Create class instance
# If no levels dictionary is provided, it will use the default, which is all of them!
# indicators = panel_constructor()

# Run panel creation
indicators.dirty_panel()

#-----------------------------------------------------------------#
# Load usage outliers file

# This file is created in data-checks
# i1_ag_df_tower_down = pd.read_csv("Path/to/usage-outliers/file")
i1_ag_df_tower_down = pd.read_csv("C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/outputs/data-checks/days_wards_with_low_hours_I1_panel.csv")

#-----------------------------------------------------------------#
# Export comparison panel

if EXPORT:
    indicators.export(DATA_panel_comp)

#-----------------------------------------------------------------#
# Create clean panel

# This replaces the old panel attribute with the clean version, with
# standardized column names

indicators.clean_panel(i1_ag_df_tower_down)

#-----------------------------------------------------------------#


indicators.add_other_provider(mno_path =  "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/telecel/", 
                              mno_suffix = '_tel')

indicators.i10_2.add_provider(indicators.i10_2_tel)
indicators.i10_3.add_provider(indicators.i10_3_tel)



#-----------------------------------------------------------------#
# Export
if EXPORT:
    indicators.export(DATA_panel_clean + 'mutlple-mnos/')
