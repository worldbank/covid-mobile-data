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

from bokeh.plotting import figure, output_file, show
from bokeh.models import Span
from bokeh.io import export_png


# GLOBALS

# File paths
DATA_path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/"
OUT_path = DATA_path + 'proof-of-concept/outputs/'

# Default values
missing_values = ['99999','']