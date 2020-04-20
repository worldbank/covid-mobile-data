
# This files counts wards that have days without data. Probably due to 
# tower being down.

# DEPENS ON: covid-mobile-data/dashboard-dataviz/zimbabwe_dashboard/_master.R

#----------------------------------------------------------------------#
# File paths

ADM3   <- file.path(PROJECT_PATH, 
                    "proof-of-concept", 
                    "databricks-results", 
                    "zw", 
                    "admin3")

A2_flow   <- file.path(ADM2,
                       "flowminder")
A2_cust   <- file.path(ADM2,
                       "custom")


A3_flow   <- file.path(ADM3,
                       "flowminder")
A3_cust   <- file.path(ADM3,
                       "custom")

#----------------------------------------------------------------------#
# Laod unique subscribers data

# A3
sc_a3_d <- fread(file.path(A3_flow,
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
# Export output

















