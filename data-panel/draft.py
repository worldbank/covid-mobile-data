#-----------------------------------------------------------------#
# CREATE PANEL
#-----------------------------------------------------------------#

# This code creates panel datasets combinig different versions of 
# indicator files. 

from utils import *
from panel_constructor_simp import *


#-------------------#
# Indicator dataframe

# Load list of indicators to make it easier to bulk load files
indicators_df = pd.read_csv('C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/documentation/indicators_list_uga.csv')


#-------------------#
# Set default values
levels_dict = { 1: [3],
                2: [3],
                3: [2,3],
                4: ['country'],
                # 5: [2,3,'tc_harare', 'tc_bulawayo'],
                5: [2,3],
                6: [3],
                7: [2,3],
                8: [2,3],
                9: [2,3],
                #9: [2,3,'tc_harare', 'tc_bulawayo'],
                10: [2,3],
                11: [2,3]}


# i1_3 = i_indicator(num = 1,  index_cols = ['hour', 'region'], files_df = indicators_df)

# i1_3.create_panel()
# i1_3.create_clean_panel()

indicators = panel_constructor(levels_dict, indicators_df)

indicators.dirty_panel()

foo = indicators.i1_3.panel
foo['count'].max()
foo[foo['count'] == foo['count'].max()]
data = foo[foo['region'] == 'UG100801']

data = indicators.i1_3.panel
data['date'] = pd.to_datetime(data['hour']).dt.date

foo = data.groupby('date').agg({'count' : np.sum}).reset_index()
import plotly.express as px
fig = px.line(foo, x = 'date', y = 'count', labels=dict(date="Date", count="Number of transactions (calls, messages or data"))
fig.show()

path = 'C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Uganda/mtn/admin3/'
i1_02 = pd.read_csv(path + '202002_transactions_per_hour.csv')
i1_03 = pd.read_csv(path + '202003_transactions_per_hour.csv')
i1_04 = pd.read_csv(path + '202004_transactions_per_hour.csv')
i1_07 = pd.read_csv(path + '202007_transactions_per_hour.csv')

i1_07[i1_07['region'] == 'UG100801'].sort_values('hour').to_clipboard()

def fun(data):
    data['date'] = pd.to_datetime(data['hour']).dt.date
    data  = data.groupby('date').agg({'count' : np.sum})
    fig = px.line(data)
    fig.show()

fun(i1_04)