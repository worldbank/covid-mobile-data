# Imports necessary packages and sets some global vars
### spark etc
# import rarfile

import os, pyspark, time, sys
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark import *
from pyspark.sql import *
from pyspark.rdd import *
from pyspark.ml import *
from pyspark.sql.types import ArrayType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import FloatType

### data wrangling
import pandas as pd
import glob
import shutil
pd.options.display.float_format = '{:,.0f}'.format
# pd.set_option("display.max_rows", 100)
pd.options.display.max_columns = None
import datetime as dt
import numpy as np
from random import sample, seed
seed(510)
# timezone = dt.timezone(offset = -dt.timedelta(hours=5), name = "America/Bogota")
timezone = dt.timezone(offset = -dt.timedelta(hours=0), name = "Africa/Harare")
import re
#import fiona
#import geopandas as gpd
import copy
from collections import Counter
from shapely import wkt

### plotting
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
#import folium
#import gif
#from folium.plugins import HeatMap, DualMap, Fullscreen
#from folium.features import DivIcon
#from branca.element import Template, MacroElement
import locale
from matplotlib.ticker import FuncFormatter
import matplotlib.lines as mlines
font = {'family' : 'Calibri',
        'weight' : 'normal',
        'size'   : 18}
import matplotlib
