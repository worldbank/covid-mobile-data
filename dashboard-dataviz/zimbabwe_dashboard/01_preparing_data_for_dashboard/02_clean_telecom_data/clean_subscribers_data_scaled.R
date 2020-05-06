# Clean Subscribers Data

# First processing is a bit dfferent for district/ward so do here

EXPORT <- T

#### Load / Prep Percent Active
percent_active_df <- read.csv(file.path(DATABRICKS_PATH, "indicator 4", "admin2", "percent_of_all_subscribers_active_option3_per_day.csv"),
                              stringsAsFactors=F)
percent_active_df <- percent_active_df %>% 
  dplyr::rename(region = home_region) %>%
  dplyr::select(region, day, percent_active)
  
unit <- "district"
for(unit in c("ward", "district")){
  
  # Set parameters -------------------------------------------------------------
  if(unit %in% "district"){
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "scaled indicators")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
    
    df_day <- read.csv(file.path(RAW_DATA_PATH, "unique_subscribers_per_day.csv"),
                       stringsAsFactors=F)
    df_day <- df_day[!(df_day$region %in% 99999),]
    df_day <- merge(df_day, percent_active_df, by=c("region", "day"), all.x=T, all.y=F)
    
    df_day_clean <- df_day %>% 
      
      tp_standardize_vars("day", "region", "weighted_count_population_scale")
  }
  
  if(unit %in% "ward"){
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "flowminder indicators", "admin3")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
    
    #### Load Data
    df_day <- read.csv(file.path(RAW_DATA_PATH, 
                                 "count_unique_subscribers_per_region_per_day.csv"), 
                       stringsAsFactors=F)
    
    #### Account for towers
    df_day$visit_date <- df_day$visit_date %>% substring(1,10) %>% as.Date()
    
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
                by = c("visit_date" = "date",
                       "region" = "region"))
    
    df_day$subscriber_count[df_day$tower_down %in% TRUE] <- NA
    
    #### Merge in district_id
    admin_sp_sub <- admin_sp@data %>%
      dplyr::select(region, district_id)
    
    df_day <- merge(df_day, admin_sp_sub, by="region")
    
    #### Merge in percent_active
    percent_active_df_ward <- percent_active_df %>%
      dplyr::rename(district_id = region,
                    visit_date = day) %>%
      mutate(visit_date = visit_date %>% substring(1,10))
      
    df_day <- merge(df_day, percent_active_df_ward, by=c("district_id", "visit_date"),
                    all.x=T,
                    all.y=F)
    
    #### Prep
    df_day_clean <- df_day %>% 
      
      tp_standardize_vars("visit_date", "region", "subscriber_count")
    
  }
  
  # Daily ----------------------------------------------------------------------
  print("day")
  
  df_day_clean <- df_day_clean %>%
    
    # Clean datset
    tp_clean_date() %>%
    tp_fill_regions(admin_sp) %>%
    tp_complete_date_region(keep_other_vars = "percent_active") %>% 
    tp_add_polygon_data(admin_sp) %>%
    
    # Interpolate/Clean Values
    tp_interpolate_outliers(NAs_as_zero = T) %>%
    tp_replace_zeros(NAs_as_zero = T) %>%
    tp_less15_NA() %>%
    
    # Create adjusted value to use for percent change and zscore
    mutate(value_scale = value / percent_active) %>%
    
    # Percent change
    tp_add_baseline_comp_stats(value_var = "value_scale") %>%
    tp_add_percent_change() %>% 
    dplyr::rename(value_base = value_scale_base,
                  value_perchange_base = value_scale_perchange_base,
                  value_zscore_base = value_scale_zscore_base) %>%
    
    # Add labels
    tp_add_label_level(timeunit = "day", OD = F) %>%
    tp_add_label_baseline(timeunit = "day", OD = F) %>%
    
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



