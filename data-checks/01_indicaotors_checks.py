
from checker import *

indicators_checker = checker(path = "Path/to/cdr/indicators", level = 'admin2')

#------------------------------------------------------------------------------
# Export completeness plots
indicators_checker.completeness_checks()

#------------------------------------------------------------------------------
# Export towers down sheet
indicators_checker.usage_outliers()
