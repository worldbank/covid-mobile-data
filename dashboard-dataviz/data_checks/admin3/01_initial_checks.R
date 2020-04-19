# Master Script for Checks on Data

# Load Data --------------------------------------------
admin3 <- read_sf(file.path(zw_admin3_data_file_path, "zimbabwe_admin3.geojson")) %>% as("Spatial")

# *** By Region Function =======================================================
by_region_checks_i <- function(region, df, values_var, dates_var){
  
  values <- df[[values_var]][df$region %in% region]
  dates  <- df[[dates_var]][df$region %in% region]
  
  deviation <- values %>% scale() %>% abs()
  
  out_df <- data.frame(region = region,
                       max_dev = deviation %>% max(),
                       N_obs = length(values),
                       N_zeros = sum(values == 0),
                       dates_zero = dates[values == 0] %>% paste(collapse=";"),
                       dates_high_deviation = dates[deviation > 4] %>% paste(collapse=";"))
  return(out_df)
}

by_region_checks <- function(df, values_var, dates_var){
  lapply(unique(df$region), by_region_checks_i, df, values_var, dates_var) %>% bind_rows()
}

region_output <- function(original_df, values_var, dates_var, region_var){
  
  original_df$values_var <- original_df[[values_var]]
  original_df$dates_var <- original_df[[dates_var]]
  original_df$region <- original_df[[region_var]]
  
  original_df <- original_df[,c("region", "dates_var", "values_var")]
  
  #### Date Variable
  original_df$dates_var <- original_df$dates_var %>% as.character() %>% substring(1,10) %>% as.Date()
  
  #### Complete
  original_df <- original_df %>%
    complete(dates_var, region,
             fill = list(values_var = 0))
  
  #### Region DF
  region_df <- by_region_checks(original_df, "values_var", "dates_var")
  
  #### Number of Regions with Zeros
  zeros_table <- region_df$N_zeros %>% 
    table() %>%
    as.data.frame() %>%
    mutate(Proportion = (Freq/sum(Freq)) %>% round(4)) %>%
    dplyr::rename("Number of Zeros" = ".",
                  "Number of Regions" = "Freq")
  
  #### Deviation Histogram
  dev_hist <- ggplot() +
    geom_histogram(data=region_df, aes(x=max_dev), color="black", fill="dodgerblue2") +
    labs(x="Maximum Scaled Distance to Mean", y="Number of Regions",
         title = "Within Each Region, Maximum Scaled Distance to Mean") +
    theme_minimal()
  
  #### Examples
  region_high_devs <- region_df$region[(region_df$N_zeros %in% 0) & (region_df$max_dev > 4)] %>% as.character()
  
  example_outliers <- ggplot() +
    geom_line(data=original_df %>%
                filter(region %in% region_high_devs[1:6]), 
              aes(x=dates_var, y=values_var, group=region, color=region)) +
    labs(x="",
         y="",
         title = "Example regions with High Deviations") +
    theme_minimal() 
  
  return(list(zeros_table=zeros_table,
              dev_hist=dev_hist,
              example_outliers=example_outliers))
}


# *** Daily Calls ==============================================================
calls_daily <- read.csv(file.path(zw_admin3_data_file_path, "total_calls_per_region_per_day.csv"))
calls_daily_out <- region_output(calls_daily, "total_calls", "call_date", "region")

calls_daily_out$zeros_table
calls_daily_out$dev_hist
calls_daily_out$example_outliers

# Prep Data --------------------------------------------------------------------



#### Complete
calls_daily <- calls_daily %>%
  complete(call_date, region,
           fill = list(total_calls = 0))

#### Fix Date
calls_daily$call_date <- calls_daily$call_date %>% as.character() %>% substring(1,10) %>% as.Date()

# Total Daily ------------------------------------------------------------------
calls_daily_sum <- calls_daily %>%
  group_by(call_date) %>%
  summarise(total_calls = sum(total_calls))

ggplot() +
  geom_line(data=calls_daily_sum, aes(x=call_date, y=total_calls))

# Outliers by Region -----------------------------------------------------------
calls_daily_region_df <- by_region_checks(calls_daily, "total_calls", "call_date")

#### Number of Regions with Zeros
calls_daily_region_df$N_zeros %>% 
  table() %>%
  as.data.frame() %>%
  mutate(Proportion = (Freq/sum(Freq)) %>% round(4)) %>%
  dplyr::rename("Number of Zeros" = ".",
                "Number of Regions" = "Freq")

#### Non-Zero Outliers
calls_daily_region_high_devs <- calls_daily_region_df$region[(calls_daily_region_df$N_zeros %in% 0) & (calls_daily_region_df$max_dev > 4)] %>% as.character()

p <- ggplot() +
  geom_line(data=calls_daily %>%
              filter(region %in% calls_daily_region_high_devs[1:6]), 
            aes(x=call_date, y=total_calls, group=region, color=region)) +
  labs(x="",
       title = "Example regions with High Deviations: Daily Calls") +
  theme_minimal() 
ggplotly(p)

# *** Unique Subscribers =======================================================

# Prep Data --------------------------------------------------------------------
subscribers_daily <- read.csv(file.path(zw_admin3_data_file_path, "count_unique_subscribers_per_region_per_day.csv"))

#### Complete
subscribers_daily <- subscribers_daily %>%
  complete(visit_date, region,
           fill = list(subscriber_count = 0))

#### Fix Date
subscribers_daily$visit_date <- subscribers_daily$visit_date %>% as.character() %>% substring(1,10) %>% as.Date()

# Total Daily ------------------------------------------------------------------
subscribers_daily_sum <- subscribers_daily %>%
  group_by(visit_date) %>%
  summarise(subscriber_count = sum(subscriber_count))

ggplot() +
  geom_line(data=subscribers_daily_sum, aes(x=visit_date, y=subscriber_count))

# Outliers by Region -----------------------------------------------------------
subscribers_daily_region_df <- by_region_checks(subscribers_daily, "subscriber_count", "visit_date")

#### Number of Regions with Zeros
subscribers_daily_region_df$N_zeros %>% 
  table() %>%
  as.data.frame() %>%
  mutate(Proportion = (Freq/sum(Freq)) %>% round(4)) %>%
  dplyr::rename("Number of Zeros" = ".",
                "Number of Regions" = "Freq")

#### Non-Zero Outliers
subscribers_daily_region_high_devs <- subscribers_daily_region_df$region[(subscribers_daily_region_df$N_zeros %in% 0) & (subscribers_daily_region_df$max_dev > 4)] %>% as.character()

p <- ggplot() +
  geom_line(data=subscribers_daily %>%
              filter(region %in% subscribers_daily_region_high_devs[1:6]), 
            aes(x=visit_date, y=subscriber_count, group=region, color=region)) +
  labs(x="",
       title = "Example regions with High Deviations: Daily Subscribers") +
  theme_minimal() 
ggplotly(p)

