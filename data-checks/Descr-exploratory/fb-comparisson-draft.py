
import os
import pandas as pd
import glob

base_path = 'C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/Facebook Zimbabwe Data/'
data = base_path + 'Population Administrative Regions/'

# prefix = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Movement between Administrative Regions__'
prefix = 'Zimbabwe Coronavirus Disease Prevention Map Apr 16 2020 Id  Facebook Population (Administrative Regions)__'

# Load all the csv files in the folder
files = glob.glob(data + prefix + "*.csv")

df = pd.concat([pd.read_csv(f, encoding='latin1') for f in files], ignore_index=True)


df1 = pd.read_csv(data + prefix + '2020-06-24 0000.csv')
df2 = pd.read_csv(data + prefix + '2020-06-24 0800.csv')
df3 = pd.read_csv(data + prefix + '2020-06-24 1600.csv')

df1.to_clipboard()
df2.to_clipboard()
df3.to_clipboard()

# Load all files
# Aggregate them by day, summing everything
# Match with our files by str IDs

qgrid.show_grid(df1)

os.listdir(path)