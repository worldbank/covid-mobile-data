#-----------------------------------------------------------------#
# DATA CHECKS - Globals
#-----------------------------------------------------------------#

# This file contains settings and globals used across data checks 
# files

# LIBRARIES
import os
import re
import pandas as pd
import numpy as np
import datetime as dt

import seaborn as sns; sns.set()
from matplotlib import rcParams
import matplotlib.pyplot as plt

# GLOBALS
DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
INDICATORS_path = DATA_path + 'Zimbabwe/isaac-results/Archive/e_23_07_2020_converage_23_05_to_30_06/'
OUT_path = DATA_path + 'proof-of-concept/outputs/'
