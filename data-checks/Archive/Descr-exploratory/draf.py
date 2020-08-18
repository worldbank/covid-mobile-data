# Indicator 1 panel data
i1 = pd.read_csv( OUT_hfcs + 'Sheet comp panel/i1_admin3.csv')
i1 = i1[i1.region != '99999']

i3 = pd.read_csv( OUT_hfcs + 'Sheet comp panel/i3_admin3.csv')
i3 = i3[i3.region != '99999']

i1['date'] = pd.to_datetime(i1['hour']).dt.date
i3['date'] = pd.to_datetime(i3['day']).dt.date


# Number of calls per day
i1_day = i1.groupby(['date', 'region'])['count_p'].sum().reset_index()

# Merge 
i13 = i1_day.merge(i3[['date', 'count_p', 'region']].rename(columns = {'count_p' : 'subscribers'}), 
                   on = ['date', 'region'])

np.mean(i13['count_p']/i13['subscribers'])