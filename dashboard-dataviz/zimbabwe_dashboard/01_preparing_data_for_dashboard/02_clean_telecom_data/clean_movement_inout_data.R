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
  df_day <- read.csv(file.path(CUSTOM_DATA_PATH, 
                               "origin_destination_connection_matrix_per_day.csv"), 
                     stringsAsFactors=F)
  
  df_day_clean <- df_day %>% 
    
    tp_standardize_vars_od("connection_date", "region_from", "region_to", "total_count") %>%
    
    # Clean datset
    tp_clean_date() %>%
    tp_complete_date_region_od() %>%
    tp_add_polygon_data_od(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = T) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Percent change
    tp_add_baseline_comp_stats() %>%
    tp_add_percent_change() %>%
    
    # Add labels
    tp_add_label_level(timeunit = "day", OD = T) %>%
    tp_add_label_baseline(timeunit = "day", OD = T) 
  
  if(EXPORT){
    saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH,
                                "origin_destination_connection_matrix_per_day.Rds"))
    write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, 
                                  "count_unique_subscribers_per_region_per_day.Rds"), 
              row.names=F)
  }

  # Weekly ---------------------------------------------------------------------
  df_day <- read.csv(file.path(CUSTOM_DATA_PATH, 
                               "origin_destination_connection_matrix_per_day.csv"), 
                     stringsAsFactors=F)
  
  df_week_clean <- df_day %>% 
    
    tp_standardize_vars_od("connection_date", "region_from", "region_to", "total_count") %>%
    
    # Clean datset
    tp_clean_week() %>%
    tp_agg_day_to_week_od() %>%
    tp_complete_date_region_od() %>%
    tp_add_polygon_data_od(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = T) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Percent change
    tp_add_baseline_comp_stats() %>%
    tp_add_percent_change() %>%
    
    # Add labels
    tp_add_label_level(timeunit = "week", OD = T) %>%
    tp_add_label_baseline(timeunit = "week", OD = T) 
  
  if(EXPORT){
    saveRDS(df_week_clean, file.path(CLEAN_DATA_PATH,
                                    "origin_destination_connection_matrix_per_week.Rds"))
    write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, 
                                      "count_unique_subscribers_per_region_per_week.Rds"), 
              row.names=F)
  }
  
}


