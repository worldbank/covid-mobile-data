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

#-------------------#
# Indicator dataframe

# Load list of indicators to make it easier to bulk load files
indicators_df = pd.read_csv('path/to/indicators_list.csv')


#-------------------#
# Set default values
levels_dict = { 1: [3],
                2: [3],
                3: [2,3],
                4: ['country'],
                5: [2,3,'tc_harare', 'tc_bulawayo'],
                5: [2,3],
                6: [3],
                7: [2,3],
                8: [2,3],
                9: [2,3],
                #9: [2,3,'tc_harare', 'tc_bulawayo'],
                10: [2,3],
                11: [2,3]}


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
i1_ag_df_tower_down = pd.read_csv("/path/to/usage-outliers/file")

#-----------------------------------------------------------------#
# Export comparison panel

if EXPORT:
    indicators.export('/export/path/')

#-----------------------------------------------------------------#
# Create clean panel

# This replaces the old panel attribute with the clean version, with
# standardized column names

indicators.clean_panel(i1_ag_df_tower_down)

#-----------------------------------------------------------------#


indicators.add_other_provider(mno_path =  "/path/to/other/mno/indicator/folder", 
                              mno_suffix = '_mno')


#-----------------------------------------------------------------#
# Export
if EXPORT:
    indicators.export('/export/path/')
