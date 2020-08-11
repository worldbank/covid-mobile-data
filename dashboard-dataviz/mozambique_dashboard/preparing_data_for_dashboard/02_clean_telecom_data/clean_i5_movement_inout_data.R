# Clean i5 Data for Dashboard

EXPORT <- T

# Ward Level ===================================================================
unit = "adm2"
for(unit in c("adm2", "adm3")){
  
  # Load Data / Set Paths ------------------------------------------------------
  df_day <- read.csv(file.path(RAW_INDICATORS, paste0("indicator_05_",unit,"_day_result.csv")),
                     stringsAsFactors=F)
  admin_sp <- readRDS(file.path(GEO_PATH, paste0(unit, ".Rds")))
  
  if(unit %in% "adm2"){
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    
    df_day <- clean_moz_names(df_day, 
                              name = "adm2", 
                              name_higher = "adm1", 
                              type = "adm2")
    
    df_day <- clean_moz_names(df_day, 
                              name = "N_adm2", 
                              name_higher = "N_adm1", 
                              type = "adm2")
    
  } 
  
  if(unit %in% "adm3"){
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    
    df_day <- clean_moz_names(df_day, 
                              name = "adm3", 
                              name_higher = "adm2", 
                              type = "adm3")
    
    df_day <- clean_moz_names(df_day, 
                              name = "N_adm3", 
                              name_higher = "N_adm2", 
                              type = "adm3")
    
  }
  
  #### Remove small observations
  # If less than 15, make NA. Doing this now removes some region-pairs. For 
  # example, if a o-d pair has a value less than 15 for every time period, 
  # we don't considered here and helps improve code speed both here and in
  # the script to prepare data for dashboard.
  df_day <- df_day[df_day$totalOD > 15,]
  
  # Daily ----------------------------------------------------------------------
  #### Process data for dashboard
  df_day_clean <- df_day %>% 
    
    tp_standardize_vars_od("pdate", 
                           unit, 
                           paste0("N_", unit), 
                           "totalOD") %>%
    
    # Clean datset
    tp_clean_date() %>%
    tp_complete_date_region_od() %>%
    tp_add_polygon_data_od(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = F) %>% 
    #tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Percent change
    tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, "i5_daily_base.csv"),
                               baseline_date = BASELINE_DATE) %>%
    tp_add_percent_change() %>%
    
    # Add labels
    tp_add_label_level(timeunit = "day", OD = T) %>%
    tp_add_label_baseline(timeunit = "day", OD = T) 
  
  ## Export
  saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH, "i5_daily.Rds"))
  write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, "i5_daily.csv"), row.names=F)
  
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
    tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, "i5_weekly_base.csv"),
                               type = "weekly",
                               baseline_date = BASELINE_DATE) %>%
    tp_add_percent_change() %>%
    
    # Add labels
    tp_add_label_level(timeunit = "week", OD = T) %>%
    tp_add_label_baseline(timeunit = "week", OD = T) 
  
  ## Export
  saveRDS(df_week_clean, file.path(CLEAN_DATA_PATH, "i5_weekly.Rds"))
  write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, "i5_weekly.csv"), row.names=F)
  
}






