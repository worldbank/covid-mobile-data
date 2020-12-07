
from checker_mod1 import *

indicators_checker = checker(path = "/Users/ruiwenzhang/Desktop/data_quality_check/indicators_prefixes")

#------------------------------------------------------------------------------
# Export completeness plots
indicators_checker.completeness_checks()

#------------------------------------------------------------------------------
# Export towers down sheet
indicators_checker.usage_outliers()