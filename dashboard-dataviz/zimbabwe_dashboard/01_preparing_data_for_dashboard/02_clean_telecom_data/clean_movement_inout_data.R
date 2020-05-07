# Clean Subscribers Data


EXPORT <- T

# Ward Level ===================================================================

for(unit in c("ward", "district")){
  
  print(paste(unit, "--------------------------------------------------------"))
  
  # Set parameters -------------------------------------------------------------
  if(unit %in% "district"){
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "indicator 5", "admin2")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
  }
  
  if(unit %in% "ward"){
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "indicator 5", "admin3")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
  }
  
  #### Load Data
  df_day <- read.csv(file.path(RAW_DATA_PATH, 
                               "origin_destination_connection_matrix_per_day.csv"), 
                     stringsAsFactors=F)
  
  df_day <- df_day %>%
    dplyr::rename(date = connection_date) %>%
    mutate(date = date %>% substring(1,10))
  
  if(unit %in% "ward"){
    towers_down <- read.csv(file.path(PROOF_CONCEPT_PATH, 
                                      "outputs", 
                                      "data-checks", 
                                      "days_wards_with_low_hours_I1.csv"))
    
    #### Towers Down
    # If tower seems down, make the value NA
    # Here, we just remove the observation -- which is equivilent to making it NA
    towers_down <- towers_down %>%
      dplyr::select(region, date) %>%
      mutate(tower_down = T)
    
    df_day <- df_day %>%
      
      left_join(towers_down, by=c("region_from" = "region", 
                                  "date" = "date")) %>%
      dplyr::rename(tower_down_from = tower_down) %>%
      
      left_join(towers_down, by=c("region_to" = "region", 
                                  "date" = "date")) %>%
      dplyr::rename(tower_down_to = tower_down) 
    
    df_day <- df_day[is.na(df_day$tower_down_from),]
    df_day <- df_day[is.na(df_day$tower_down_to),]
    
  }
  
  #### Remove small observations
  # If less than 15, make NA. Doing this now removes some region-pairs. For 
  # example, if a o-d pair has a value less than 15 for every time period, 
  # we don't considered here and helps improve code speed both here and in
  # the script to prepare data for dashboard.
  df_day <- df_day[df_day$total_count > 15,]
  
  #### Process data for dashboard
  df_day_clean <- df_day %>% 
    
    tp_standardize_vars_od("date", "region_from", "region_to", "total_count") %>%
    
    # Clean datset
    tp_clean_date() %>%
    tp_complete_date_region_od() %>%
    tp_add_polygon_data_od(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = F) %>% 
    #tp_replace_zeros(NAs_as_zero = T) %>%
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
    # write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, 
    #                                    "origin_destination_connection_matrix_per_day.csv"), 
    #           row.names=F)
  }
  
  # Weekly ---------------------------------------------------------------------
  print("week")
  
  df_week_clean <- df_day_clean %>% 
    
    dplyr::select(date, region_origin, region_dest, value) %>%
    
    tp_standardize_vars_od("date", "region_origin", "region_dest", "value") %>%
    
    # Clean datset
    tp_clean_week() %>%
    tp_agg_day_to_week_od() %>%
    tp_complete_date_region_od() %>%
    tp_add_polygon_data_od(admin_sp) %>%
    
    # Interpolate/Clean Values
    #tp_interpolate_outliers(NAs_as_zero = F) %>%
    #tp_replace_zeros(NAs_as_zero = T) %>%
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
    #write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, 
    #                                   "origin_destination_connection_matrix_per_week.csv"), 
    #          row.names=F)
  }
  
}






