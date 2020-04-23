
# This files counts wards that have days without data. Probably due to 
# tower being down.

# DEPENS ON: covid-mobile-data/dashboard-dataviz/zimbabwe_dashboard/_master.R

#----------------------------------------------------------------------#
# Laod unique subscribers data

# A3
sc_a3_d <- fread(file.path(RAW_DATA_ADM3_PATH,
                           "count_unique_subscribers_per_region_per_day.csv"))
# sc_a3_w <- fread(file.path(A3_flow,
#                            "count_unique_subscribers_per_region_per_week.csv"))


#----------------------------------------------------------------------#
# Create df with only wards that have fewer unique dates than the max
# in the data, which is 58 (as of Apr 20 2020)

w_missing_days <- 
  sc_a3_d %>% group_by(region) %>% 
    summarise(ndays = n_distinct(visit_date)) %>% 
    ungroup() %>% 
    mutate(n_of_missing_days = abs(ndays-max(ndays))
           ) %>%
    subset(n_of_missing_days > 0)

#----------------------------------------------------------------------#
# Check sudden valleys in the number of subscribers

# Quick and dirty way to flag valleys

sc_a3_d <- 
  sc_a3_d %>% group_by(region) %>% 
    mutate(avg_count = mean(subscriber_count),
           sd_count = sd(subscriber_count),
           zscore = (subscriber_count - avg_count)/sd_count,
           neg_outlier = ifelse(zscore < -6,
                                1,0)) %>% 
    ungroup()

wards_with_valleys <- sc_a3_d$region[sc_a3_d$neg_outlier == 1]


foo <- sc_a3_d %>% subset(region %in% wards_with_valleys)
foo <- sc_a3_d %>% subset(neg_outlier == 1)

#----------------------------------------------------------------------#
# Export output

















