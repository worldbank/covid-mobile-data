# Clean i3 for Dashboard

unit <- "adm2"
for(unit in c("adm2", "adm3")){
  
  # Load Data / Set Paths ------------------------------------------------------
  df_day <- read.csv(file.path(RAW_INDICATORS, paste0("indicator_03_",unit,"_day_result.csv")),
                     stringsAsFactors=F)
  admin_sp <- readRDS(file.path(GEO_PATH, paste0(unit, ".Rds")))
  
  if(unit %in% "adm2"){
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
  } 
  if(unit %in% "adm3"){
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
  }
  
  # Daily ----------------------------------------------------------------------
  df_day_clean <- df_day %>% 
    
    tp_standardize_vars("pdate", unit, "totalimei") %>%

    # Clean datset
    tp_clean_date() %>%
    tp_fill_regions(admin_sp) %>%
    tp_complete_date_region() %>%
    tp_add_polygon_data(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = T, outlier_sd=3) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Percent change
    tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, "i3_daily_base.csv"),
                               baseline_date = BASELINE_DATE) %>%
    tp_add_percent_change() %>%
    
    # Add labels
    tp_add_label_level(timeunit = "day", OD = F) %>%
    tp_add_label_baseline(timeunit = "day", OD = F) %>%
    
    # Add density
    mutate(density = value / area) 
  
  ## Export
  saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH, "i3_daily.Rds"))
  write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, "i3_daily.csv"), row.names=F)
  
  # Weekly ---------------------------------------------------------------------
  print("week")
  
  df_week_clean <- df_day_clean %>% 
    
    tp_standardize_vars("date", "region", "value") %>%
    
    # Clean datset
    tp_clean_week() %>%
    tp_agg_day_to_week(fun = "mean") %>%
    tp_complete_date_region() %>%
    tp_add_polygon_data(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = T) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Percent change
    tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, "i3_weekly_base.csv"),
                               type = "weekly",
                               baseline_date = BASELINE_DATE) %>%
    tp_add_percent_change() %>%
    
    # Add labels
    tp_add_label_level(timeunit = "week", OD = F) %>%
    tp_add_label_baseline(timeunit = "week", OD = F) %>%
    
    # Add density
    mutate(density = value / area)
  
  ## Export
  saveRDS(df_week_clean, file.path(CLEAN_DATA_PATH, "i3_weekly.Rds"))
  write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, "i3_weekly.csv"), row.names=F)
  
}


