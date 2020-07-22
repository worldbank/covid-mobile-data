
#-----------------------------------------------------------------#
# Exploratory analysis
#-----------------------------------------------------------------#

# Load indicators 1, 5 and 7

# Average distance travelled before and after
# Number of visited wards  

# Choropleth maps???

#-----------------------------------------------------------------#
#### Settings

import os
import re
import pandas as pd
import numpy as np
import datetime as dt

import seaborn as sns; sns.set()
from matplotlib import rcParams
import matplotlib.pyplot as plt

#-----------------------------------------------------------------#
#### Set file paths

DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
DATA_POC = DATA_path + "proof-of-concept/"

DATA_GIS = DATA_POC + 'geo_files/'
DATA_panel = DATA_POC + 'panel_indicators/'

#-----------------------------------------------------------------#
#### Load data

i1 = pd.read_csv(DATA_panel + 'i1_admin3.csv')
i3 = pd.read_csv(DATA_panel + 'i3_admin3.csv')
i5 = pd.read_csv(DATA_panel + 'i5_admin3.csv')
i7 = pd.read_csv(DATA_panel + 'i7_admin3.csv')