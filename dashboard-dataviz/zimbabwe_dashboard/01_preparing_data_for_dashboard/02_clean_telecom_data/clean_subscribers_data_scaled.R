# Clean Subscribers Data

# First processing is a bit dfferent for district/ward so do here

EXPORT <- T

#### Load / Prep Percent Active
#percent_active_df <- read.csv(file.path(DATABRICKS_PATH, "indicator 4", "admin2", "percent_of_all_subscribers_active_option3_per_day.csv"),
#                              stringsAsFactors=F)
#percent_active_df <- percent_active_df %>% 
#  dplyr::rename(region = home_region) %>%
#  dplyr::select(region, day, percent_active)
  
unit <- "district"
for(unit in c("ward", "district")){
  
  # Set parameters -------------------------------------------------------------
  
  #### Districts
  if(unit %in% "district"){
    
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "scaled indicators", "admin2")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
    
    df_day <- read.csv(file.path(RAW_DATA_PATH, "unique_subscribers_per_day.csv"),
                       stringsAsFactors=F)
    df_day <- df_day[!(df_day$region %in% 99999),]
    
    #df_day <- merge(df_day, percent_active_df, by=c("region", "day"), all.x=T, all.y=F)
    
    #df_day_clean <- df_day %>% 
    #  tp_standardize_vars("day", "region", "weighted_count_population_scale")
  }
  
  #### Wards
  if(unit %in% "ward"){
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "scaled indicators", "admin3")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
    
    #### Load Data
    df_day <- read.csv(file.path(RAW_DATA_PATH, 
                                 "unique_subscribers_per_day.csv"), 
                       stringsAsFactors=F)
    
    #### Account for towers being down
    # If tower is down
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
                       "region" = "region"))
    
    # If the tower is down, make the value NA
    df_day$count[df_day$tower_down %in% TRUE] <- NA
    df_day$weighted_count_population_scale[df_day$tower_down %in% TRUE] <- NA
    
    #### Merge in district_id
    # Percent active only at the district level
    #admin_sp_sub <- admin_sp@data %>%
    #  dplyr::select(region, district_id)
    
    #df_day <- merge(df_day, admin_sp_sub, by="region")
    
    #### Merge in percent_active
    #percent_active_df_ward <- percent_active_df %>%
    #  dplyr::rename(district_id = region) %>%
    #  mutate(day = day %>% substring(1,10))
      
    #df_day <- merge(df_day, percent_active_df_ward, by=c("district_id", "day"),
    #                all.x=T,
    #                all.y=F)
    
    #### Prep
    #df_day_clean <- df_day %>% 
    #  tp_standardize_vars("day", "region", "subscriber_count")
    
  }
  
  # Daily ----------------------------------------------------------------------
  print("day")
  
  df_day_clean <- df_day %>%
    
    # Prep Data
    tp_standardize_vars("day", "region", "count") %>%
    dplyr::rename(value_scale = weighted_count_population_scale) %>%
    
    # Clean datset
    tp_clean_date() %>%
    tp_fill_regions(admin_sp) %>%
    tp_complete_date_region(keep_other_vars = "value_scale") %>% 
    tp_add_polygon_data(admin_sp) %>%
    
    # Interpolate/Clean - Values
    tp_interpolate_outliers(NAs_as_zero = T) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
      
    # Interpolate/Clean - value_scale
    tp_interpolate_outliers(newvar_name = "value_scale",
                            value_var = "value_scale",
                            NAs_as_zero = T) %>%
    tp_replace_zeros(newvar_name = "value_scale",
                     value_var = "value_scale",
                     NAs_as_zero = T) %>%
    tp_less15_NA(value_var = "value_scale") %>%
    
    # Create adjusted value to use for percent change and zscore
    #mutate(value_scale = value / percent_active) %>%
    
    # Percent change
    tp_add_baseline_comp_stats() %>%
    tp_add_percent_change() %>% 
    #dplyr::rename(value_base = value_scale_base,
    #              value_perchange_base = value_scale_perchange_base,
    #              value_zscore_base = value_scale_zscore_base,
    #              value_lag = value_scale_lag,
    #              value_perchange = value_scale_perchange) %>%
    
    # Add labels
    tp_add_label_level(timeunit = "day", OD = F) %>%
    tp_add_label_baseline(timeunit = "day", OD = F) %>%
    
    # Use scaled value for counts 
    # Keep unscaled value
    dplyr::rename(value_not_scaled = value,
                  value = value_scale) %>%

    # Add density
    mutate(density = value / area) 
  
  
  if(EXPORT){
    saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH,
                                    "count_unique_subscribers_per_region_per_day_scaled.Rds"))
    write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, 
                                      "count_unique_subscribers_per_region_per_day_scaled.csv"), 
              row.names=F)
  }
  
  # Weekly ---------------------------------------------------------------------
  if(F){
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
  
  # Merge Scaled and Raw -------------------------------------------------------
  ## Load raw data
  df_day_clean_orig <- readRDS(file.path(CLEAN_DATA_PATH,
                                         "count_unique_subscribers_per_region_per_day.Rds"))
  
  ## Merge vars
  merge_vars <- c("region", "date", "name", "province")
  df_day_clean$area <- NULL
  
  names(df_day_clean)[!(names(df_day_clean) %in% merge_vars)] <-
    paste0(names(df_day_clean)[!(names(df_day_clean) %in% merge_vars)], "_s")
  
  df_all <- merge(df_day_clean_orig, df_day_clean, by=merge_vars)
  
  write.csv(df_all, file.path(CLEAN_DATA_PATH, 
                              "count_unique_subscribers_per_region_per_week_orig_and_scaled.csv"), 
            row.names=F)
  
  file.path(CLEAN_DATA_PATH, 
            "count_unique_subscribers_per_region_per_week_orig_and_scaled.csv") %>% print()
  
}

adm2_df <- read.csv("C:/Users/wb521633/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/files_for_dashboard/files_clean/adm2/count_unique_subscribers_per_region_per_day_scaled.csv")
adm3_df <- read.csv("C:/Users/wb521633/WBG/Sveta Milusheva - COVID 19 Results/proof-of-concept/files_for_dashboard/files_clean/adm3/count_unique_subscribers_per_region_per_day_scaled.csv")
