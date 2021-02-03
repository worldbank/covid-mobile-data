# Temp

from checker import checker

chl = checker(path = "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results/Zimbabwe/telecel/admin3")
# chl.i1_date
# chl.i5_date

# i1 histogram

chl.plot_i1_hist()



import plotly.express as px

def hist_censored(df, count = 'count'):
    df = df[count].clip(0, df[count].quantile(0.95))
    fig = px.histogram(df, x=count, title='Hourly calls distribution.<br>(Censored at 95th percentile.)', labels = {'count' : 'Number of calls per hour.'})
    return(fig)

hist_censored(chl.i1).show()



import plotly.figure_factory as ff
import numpy as np
np.random.seed(1)

x = np.random.randn(1000)
hist_data = [x]
group_labels = ['distplot'] # name of the dataset

# fig = ff.create_distplot(hist_data, group_labels)
foo = [chl.i1['count'].tolist()]
fig = ff.create_distplot(foo, group_labels)

fig.show()


# i5 remove first day

chl.i5_date[~(chl.i5_date.index == chl.i5_date.index.min())]

chl.plot_i5_count()


# i1 and i5 percent change from period average
