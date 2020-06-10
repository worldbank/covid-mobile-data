# Clean Subscribers Data

# Clean subscriber data in a format for the dashboard. Standardizes variable names,
# adds additional variables needed for dashboard (percent change, baseline, etc),
# and checks for and replaces outliers.

#### PARAMETERS

# Export data
EXPORT <- T 

# For wards, on days when tower is down, replace with NA. Uses 
# days_wards_with_low_hours_I1.csv which indicates when a tower is likely town
TOWER_DOWN_REPLACE_NA <- T

for(unit in c("district", "ward")){
  
  # Set parameters -------------------------------------------------------------
  if(unit %in% "district"){
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "flowminder indicators", "admin2")
    ISAAC_DATA_PATH <- file.path(PROJECT_PATH, "Zimbabwe", "Isaac-results", "Isaac_apr_may", "admin2_flowminder")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
  }
  
  if(unit %in% "ward"){
    RAW_DATA_PATH <- file.path(DATABRICKS_PATH, "flowminder indicators", "admin3")
    ISAAC_DATA_PATH <- file.path(PROJECT_PATH, "Zimbabwe", "Isaac-results", "Isaac_apr_may", "admin3_flowminder")
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
  }
  
  # Daily ----------------------------------------------------------------------
  print("day")
  
  #### 1. Load Data
  ## Use Feb 16 from our data
  df_day_feb16 <- read.csv(file.path(RAW_DATA_PATH, 
                               "count_unique_subscribers_per_region_per_day.csv"), 
                     stringsAsFactors=F) %>%
    mutate(visit_date = visit_date %>% substring(1,10)) %>%
    filter(visit_date == "2020-02-16")

  ## Load Isaac data and add Feb 16
  df_day <- read.csv(file.path(ISAAC_DATA_PATH, 
                               "count_unique_subscribers_per_region_per_day.csv"), 
                     stringsAsFactors=F)
  df_day <- df_day[df_day$visit_date != "visit_date",]
  df_day$subscriber_count <- df_day$subscriber_count %>% as.numeric()
  df_day <- bind_rows(df_day, df_day_feb16)
  df_day$region <- df_day$region %>% as.character()
  
  #### 2. For wards, remove if tower is down
  if((unit %in% "ward") & TOWER_DOWN_REPLACE_NA){
    df_day$visit_date <- df_day$visit_date %>% substring(1,10) %>% as.Date()
    
    towers_down <- read.csv(file.path(PROOF_CONCEPT_PATH, 
                                      "outputs", 
                                      "data-checks", 
                                      "Archive",
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
  }
  
  #### 3. Clean data
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
  
  #### 1. Load Data
  ## Original Data
  #df_week <- read.csv(file.path(RAW_DATA_PATH, 
  #                              "count_unique_subscribers_per_region_per_week.csv"), 
  #                    stringsAsFactors=F)
  
  ## Isaac Data
  df_week <- read.csv(file.path(ISAAC_DATA_PATH, 
                               "count_unique_subscribers_per_region_per_week.csv"), 
                     stringsAsFactors=F)
  df_week <- df_week[df_week$visit_week != "visit_week",]

  #### 2. Load Data
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

