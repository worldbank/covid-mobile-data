
from checker import *

# Uganda indicator check(indicator 1 and indicator 5)
indicators_checker = checker(path = "/Users/ruiwenzhang/Desktop/data_quality_check/indicators_prefixes")

#------------------------------------------------------------------------------
# Export completeness plots
indicators_checker.completeness_checks()

#------------------------------------------------------------------------------
# Export towers down sheet
indicators_checker.usage_outliers()




# UoT indicator check(indicator 1 and indicator 5)
path_uot = "/Users/ruiwenzhang/Desktop/data_quality_check/UoT"

# Indicators are from Covid-19 Results/Mozambique/uot-results/MZ_indicators_MarApr2020
# Chose the files from adm3
ind_dict_uot = {'i1':'indicator_01_02_adm3_hour_result.csv',
                'i3':'indicator_03_adm3_day_result.csv',
                'i4':'indicator_04_adm3_day_result.csv',
                'i5':'indicator_05_adm3_day_result.csv',
                'i7':'indicator_07_home_adm3_day_result.csv',
                'i8':'indicator_08_home_adm3_week_result.csv',
                'i10':'indicator_10_adm3_day_result.csv',
                'i11':'indicator_11_adm3_month_result.csv'}

col_names_dict_uot = {'i1_col_names':{'Date':'pdate', 'Time':'hour', 'Geography': 'adm3',
                                     'Count':'total'},
                       'i5_col_names':{'Time':'pdate', 'Geography_01':'adm3','Total_Count':'totalOD',
                                       'Geography_02': 'N_adm3'}
                     }


indicators_checker_uot = checker(path = path_uot, ind_dict = ind_dict_uot, col_names_dict = col_names_dict_uot)

# Export completeness plots
indicators_checker_uot.completeness_checks()
# Export towers down sheet
indicators_checker_uot.usage_outliers()