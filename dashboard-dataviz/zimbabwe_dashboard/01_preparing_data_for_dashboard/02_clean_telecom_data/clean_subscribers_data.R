# Clean Subscribers Data

EXPORT <- T

for(unit in c("district", "ward")){
  
  # Set parameters -------------------------------------------------------------
  if(unit %in% "district"){
    CUSTOM_DATA_PATH <- CUSTOM_DATA_ADM2_PATH
    RAW_DATA_PATH    <- RAW_DATA_ADM2_PATH
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
  }
  
  if(unit %in% "ward"){
    CUSTOM_DATA_PATH <- CUSTOM_DATA_ADM3_PATH
    RAW_DATA_PATH    <- RAW_DATA_ADM3_PATH
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
  }
  
  # Daily ----------------------------------------------------------------------
  print("day")
  
  df_day <- read.csv(file.path(RAW_DATA_PATH, 
                           "count_unique_subscribers_per_region_per_day.csv"), 
                 stringsAsFactors=F)
  
  df_day_clean <- df_day %>% 
    
    tp_standardize_vars("visit_date", "region", "subscriber_count") %>%
    
    # Clean datset
    tp_clean_date() %>%
    tp_fill_regions(admin_sp) %>%
    tp_complete_date_region() %>%
    tp_add_polygon_data(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = T) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Percent change
    tp_add_baseline_comp_stats() %>%
    tp_add_percent_change() %>%

    # Add labels
    tp_add_label_level(timeunit = "day", OD = F) %>%
    tp_add_label_baseline(timeunit = "day", OD = F) %>%
    
    # Add density
    mutate(density = value / area)
  
  if(EXPORT){
    saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH,
                                "count_unique_subscribers_per_region_per_day.Rds"))
    write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, 
                                  "count_unique_subscribers_per_region_per_day.csv"), 
              row.names=F)
  }

  # Weekly ---------------------------------------------------------------------
  print("week")
  
  df_week <- read.csv(file.path(RAW_DATA_PATH, 
                               "count_unique_subscribers_per_region_per_week.csv"), 
                     stringsAsFactors=F)
  
  df_week_clean <- df_week %>% 
    
    tp_standardize_vars("visit_week", "region", "subscriber_count") %>%
    
    # Clean datset
    tp_clean_week() %>%
    tp_fill_regions(admin_sp) %>%
    tp_complete_date_region() %>%
    tp_add_polygon_data(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = T) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Percent change
    tp_add_baseline_comp_stats() %>%
    tp_add_percent_change() %>%
    
    # Add labels
    tp_add_label_level(timeunit = "week", OD = F) %>%
    tp_add_label_baseline(timeunit = "week", OD = F) %>%
    
    # Add density
    mutate(density = value / area)
  
  if(EXPORT){
    saveRDS(df_week_clean, file.path(CLEAN_DATA_PATH,
                                "count_unique_subscribers_per_region_per_week.Rds"))
    write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, 
                                  "count_unique_subscribers_per_region_per_week.csv"), 
              row.names=F)
  }
  
}

