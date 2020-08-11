# Clean Subscribers Data

unit = "adm2"
metric = "avg_dist"
for(unit in c("adm2", "adm3")){
  for(metric in c("avg_dist", "stddev")){
    
    print(paste(unit, metric,  "---------------------------------------------"))
    
    # Load Data / Set Paths ------------------------------------------------------
    df_day <- read.csv(file.path(RAW_INDICATORS, paste0("indicator_07_home_",unit,"_day_result.csv")),
                       stringsAsFactors=F)
    admin_sp <- readRDS(file.path(GEO_PATH, paste0(unit, ".Rds")))
    
    if(unit %in% "adm2"){
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
      
      df_day <- clean_moz_names(df_day, 
                                name = "H_adm2", 
                                name_higher = "H_adm1", 
                                type = "adm2")
      
    } 
    if(unit %in% "adm3"){
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
      
      df_day <- clean_moz_names(df_day, 
                                name = "H_adm3", 
                                name_higher = "H_adm2", 
                                type = "adm3")
      
    }
    
    # Daily ----------------------------------------------------------------------
    print("day")
    
    df_day_clean <- df_day %>% 
      
      tp_standardize_vars("pdate", paste0("H_", unit), metric) %>%
      
      # Clean datset
      tp_clean_date() %>%
      tp_fill_regions(admin_sp) %>%
      tp_complete_date_region() %>%
      tp_add_polygon_data(admin_sp) %>%
      
      # Interpolate/Clean Values
      tp_interpolate_outliers(NAs_as_zero = T, outlier_replace="both") %>%
      tp_replace_zeros(NAs_as_zero = T) %>%
      tp_less15_NA(threshold = 0) %>%
      
      # Percent change
      tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, paste0("i7_",metric,"_daily_base.csv"))) %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = "day", OD = F) %>%
      tp_add_label_baseline(timeunit = "day", OD = F)
    
    ## Export
    saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH, paste0("i7_daily_",metric,".Rds")))
    write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, paste0("i7_daily_",metric,".csv")), row.names=F)
    
    
    # Weekly ---------------------------------------------------------------------
    print("week")
    
    df_week_clean <- df_day_clean %>% 
      
      dplyr::select(date, region, value) %>%
      
      tp_standardize_vars("date", "region", "value") %>%
      
      # Clean datset
      tp_clean_week() %>%
      tp_agg_day_to_week(fun="mean") %>%
      tp_fill_regions(admin_sp) %>%
      tp_complete_date_region() %>%
      tp_add_polygon_data(admin_sp) %>%
      
      # Interpolate/Clean Values
      #tp_interpolate_outliers(NAs_as_zero = T) %>%
      #tp_replace_zeros(NAs_as_zero = T) %>%
      #tp_less15_NA() %>%
      
      # Percent change
      tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, paste0("i7_",metric,"_weekly_base.csv")),
                                 type = "weekly") %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = "week", OD = F) %>%
      tp_add_label_baseline(timeunit = "week", OD = F) 
    
    
    ## Export
    saveRDS(df_week_clean, file.path(CLEAN_DATA_PATH,
                                     paste0("i7_weekly_",metric,".Rds")))
    write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, 
                                       paste0("i7_weekly_",metric,".csv")), 
              row.names=F)
    
    
  }
}

