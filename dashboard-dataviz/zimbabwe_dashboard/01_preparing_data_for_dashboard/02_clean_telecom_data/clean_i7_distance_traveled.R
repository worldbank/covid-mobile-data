# Clean Subscribers Data

EXPORT <- T

for(unit in c("district", "ward")){
  for(metric in c("mean_distance", "stdev_distance")){
    
    print(paste(unit, metric,  "---------------------------------------------"))
    
    # Set parameters -------------------------------------------------------------
    if(unit %in% "district"){
      RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "indicator 7", "admin2")
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
      admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
    }
    
    if(unit %in% "ward"){
      RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "indicator 7", "admin3")
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
      admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
    }
    
    # Daily ----------------------------------------------------------------------
    print("day")
    
    df_day <- read.csv(file.path(RAW_DATA_PATH, 
                                 "mean_distance_per_day.csv"), 
                       stringsAsFactors=F)
    
    ## For wards, remove if tower is down
    if(unit %in% "ward"){
      df_day$day <- df_day$day %>% substring(1,10) %>% as.Date()
      
      towers_down <- read.csv(file.path(PROOF_CONCEPT_PATH, 
                                        "outputs", 
                                        "data-checks", 
                                        "days_wards_with_low_hours_I1.csv"))
      
      towers_down <- towers_down %>%
        dplyr::select(region, date) %>%
        mutate(tower_down = T) %>%
        mutate(date = date %>% as.character %>% as.Date())
      
      df_day <- df_day %>%
        left_join(towers_down, 
                  by = c("day" = "date",
                         "home_region" = "region"))
      
      df_day[[metric]][df_day$tower_down %in% TRUE] <- NA
    }
    
    
    df_day_clean <- df_day %>% 
      
      tp_standardize_vars("day", "home_region", metric) %>%
      
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
      tp_add_baseline_comp_stats() %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = "day", OD = F) %>%
      tp_add_label_baseline(timeunit = "day", OD = F)
    
    if(EXPORT){
      saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH,
                                      paste0("indicator_7_day_",metric,".Rds")))
      write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, 
                                        paste0("indicator_7_day_",metric,".csv")), 
                row.names=F)
    }
    
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
      tp_add_baseline_comp_stats() %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = "week", OD = F) %>%
      tp_add_label_baseline(timeunit = "week", OD = F) 
    
    if(EXPORT){
      saveRDS(df_week_clean, file.path(CLEAN_DATA_PATH,
                                       paste0("indicator_7_week_",metric,".Rds")))
      write.csv(df_week_clean, file.path(CLEAN_DATA_PATH, 
                                         paste0("indicator_7_week_",metric,".csv")), 
                row.names=F)
    }
    
  }
}

