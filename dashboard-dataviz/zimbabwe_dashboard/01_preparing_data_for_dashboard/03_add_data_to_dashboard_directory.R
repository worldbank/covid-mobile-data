# Move Data into Dashboard Directory

# This script converts the cleaned data files into a different format for the
# dashboard. In particular, much of the processing is done here to be avoided
# in the shiny server. The goal is to create files that are 
# (1) Small so can be quickly loaded and 
# (2) Require minimal manipulation to avoid processing time in the server
# (3) Use faster file types when relevant (data.tables, not data.frames)

##### Settings ##### -----------------------------------------------------------

# Number of cores for parallel processing
N_CORES <- 1

# Select which datasets to process
PROCESS_DENSITY_DATA      <- T
PROCESS_DISTANCE_DATA     <- T
PROCESS_MOVEMENT_DATA     <- T
PROCESS_MOVEMENT_NET_DATA <- T

# Delete previous files before running?
# Set to TRUE if redo naming convetions
REMOVE_PREVIOUS_FILES <- F

##### Remove Previous Files ##### ----------------------------------------------
if(REMOVE_PREVIOUS_FILES){
  temp <- list.files(DASHBOARD_DATA_PATH, 
                     full.names = T, 
                     pattern = "*.Rds") %>%
    lapply(file.remove)
}

##### Functions ##### ----------------------------------------------------------
#df <- df_movement
#timeunit <- "Daily"
#date_i <- "2020-03-20"
#od_type <- "origin"

sum_with_change_vars <- function(df, date_i, od_type){

  df$region_type <- df[[paste0("region_", od_type)]]
  df$name_type <- df[[paste0("name_", od_type)]]
  
  df <- df %>%
    group_by(name_type, region_type, date, month, dow) %>%
    summarise(value_count = sum(value_count)) %>%
    ungroup() %>%
    
    group_by(region_type, dow) %>%
    mutate(values_var_dow_feb_mean = mean(value_count[month == 2], na.rm=T),
           values_var_dow_feb_sd   = sd(  value_count[month == 2], na.rm=T)) %>%
    ungroup() 
    
    ### Percent change
  df$values_var_dow_feb_mean[is.na(df$values_var_dow_feb_mean)] <- 0
  df$values_var_dow_feb_mean[df$values_var_dow_feb_mean < 15] <- 15
  
  df <- df %>%
    mutate(value_perchange_feb = (value_count - values_var_dow_feb_mean)/(values_var_dow_feb_mean + 1)*100)
  
  ### Z-Score
  min_sd <- min(df$values_var_dow_feb_sd[df$values_var_dow_feb_sd > 0], na.rm=T)
  df$values_var_dow_feb_sd[df$values_var_dow_feb_sd < min_sd] <- min_sd
  df$value_zscore_feb <- (df$value_count - df$values_var_dow_feb_mean) / df$values_var_dow_feb_sd
  
  ### Restrict to date_i
  df <- df[as.character(df$date) %in% as.character(date_i),]
  
  ### Add o/d original variable names back
  df[[paste0("region_", od_type)]] <- df$region_type
  df[[paste0("name_", od_type)]]   <- df$name_type
  
  return(df)
}























##### *** PREP SPEICIF DATASETS *** ##### ======================================

##### Totals Data #### ---------------------------------------------------------
# Creates data.frames for total subscribes and observation figures

#### Subscribers
subs_adm2 <- read.csv(file.path(RAW_DATA_ADM2_PATH, 
                                "count_unique_subscribers_per_region_per_day.csv"))

subs_adm2 <- subs_adm2 %>%
  mutate(date = visit_date %>% substring(1,10) %>% as.Date()) %>%
  group_by(date) %>%
  summarise(Subscribers = sum(subscriber_count)) %>%
  dplyr::rename(Date = date)

saveRDS(subs_adm2, file.path(DASHBOARD_DATA_PATH,"subscribers_total.Rds"))

#### Observations
obs_adm2 <- read.csv(file.path(RAW_DATA_ADM2_PATH, 
                               "total_calls_per_region_per_day.csv"))

obs_adm2 <- obs_adm2 %>%
  mutate(date = call_date %>% substring(1,10) %>% as.Date()) %>%
  group_by(date) %>%
  summarise(Observations = sum(total_calls)) %>%
  dplyr::rename(Date = date)

saveRDS(obs_adm2, file.path(DASHBOARD_DATA_PATH,"observations_total.Rds"))

##### Ward Shapefile ##### -----------------------------------------------------
# Create an ordered ward shapefile that requires only needed variables. We order
# the shapefile by region so that the other datasets can similarly be sorted
# to avoided merging in the server in some cases.

ward_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "ward_aggregated_clean.Rds"))
ward_sp <- ward_sp[order(ward_sp$region),]

#### Export Ward Shapfile for dashboard
ward_sp_dash <- ward_sp
ward_sp_dash@data <- ward_sp@data %>%
  dplyr::select(name, province, region) %>%
  dplyr::rename(name_id = name)
ward_sp_dash <- ward_sp_dash[order(ward_sp_dash$region),]
saveRDS(ward_sp_dash, file.path(DASHBOARD_DATA_PATH, "ward_shp.Rds"))

##### Ward dataframe for cleaning
# The dataframe is used in the below cleaning steps, for merging movement
# data. Movement data may not contain all units, so need this to expand
ward_sp_regiononly_df <- ward_sp[,c("region", "province")]@data

##### District Shapefile ##### -------------------------------------------------
# Create an ordered district shapefile that requires only needed variables. We order
# the shapefile by region so that the other datasets can similarly be sorted
# to avoided merging in the server in some cases.

district_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "district.Rds"))
district_sp <- district_sp[order(district_sp$region),]

#### Export Ward Shapfile for dashboard
district_sp_dash <- district_sp
district_sp_dash@data <- district_sp_dash@data %>%
  dplyr::select(name, province, region) %>%
  dplyr::rename(name_id = name)
district_sp_dash <- district_sp_dash[order(district_sp_dash$region),]
saveRDS(district_sp_dash, file.path(DASHBOARD_DATA_PATH, "district_shp.Rds"))

##### Ward dataframe for cleaning
# The dataframe is used in the below cleaning steps, for merging movement
# data. Movement data may not contain all units, so need this to expand

district_sp_regiononly_df <- district_sp[,c("region", "province")]@data






##### *** FUNCTIONS TO PREP MAIN DATASETS *** ##### ============================

check_inputs <- function(unit, timeunit){
  try(if(!(unit %in% c("Districts", "Wards"))) stop("unit must be Districts or Wards"))
  try(if(!(timeunit %in% c("Daily", "Weekly"))) stop("unit must be Daily or Weekly"))
}

##### Density ##### ------------------------------------------------------------
prep_density_date_i <- function(date_i, df, unit, timeunit, varname){
  
  # DESCRIPTION: Subsets dataset to a specific date and exports. Dataset is 
  # sorted by region. Sorting by region is used for mapping, so in server
  # data can be directly imported then plotted in leaflet, without having to
  # merge with the spatial layer.
  
  # SERVER USE: (1) Map and (2) Summary Table
  
  # INPUTS
  # date_i:   Specific date
  # df:       Dataset
  # unit:     Districts or Wards (used for naming .Rds file)
  # timeunit: Daily or Weekly (used for naming .Rds file)
  # varname:  Variable name, used for naming .Rds file (Density, Median Distance Traveled)
  
  check_inputs(unit, timeunit)
  
  if(is.null(df$density)) df$density <- NA
  
  df <- df %>%
    filter(date %in% date_i) %>%
    arrange(region) %>%
    dplyr::select(name, province, 
                  value_count, density, value_perchange_feb, value_zscore_feb, 
                  label_change, label_count) 

  saveRDS(df, file.path(DASHBOARD_DATA_PATH,
                    paste0(unit, "_",varname,"_",timeunit,"_", date_i, ".Rds")))
}

prep_density_admin_i <- function(name_i, df, unit, timeunit, varname){
  
  # DESCRIPTION: Subsets dataset to a specific admin unit and exports. 
  
  # SERVER USE: (1) Line graph by time
  
  # INPUTS
  # name_i:   Name of admin unit
  # df:       Dataset
  # unit:     Districts or Wards (used for naming .Rds file)
  # timeunit: Daily or Weekly (used for naming .Rds file)
  # varname:  Variable name, used for naming .Rds file (Density, Median Distance Traveled)
  
  check_inputs(unit, timeunit)
  
  if(is.null(df$density)) df$density <- NA
  
  df <- df %>%
    filter(name %in% name_i) %>%
    dplyr::select(date, value_count, dow) 
  
  saveRDS(df, file.path(DASHBOARD_DATA_PATH,
                          paste0(unit, "_",varname,"_",timeunit,"_", name_i, ".Rds")))
}

##### Movement ##### -----------------------------------------------------------



prep_movement_date_i <- function(date_i, df, unit, timeunit, admin_df){
  
  # DESCRIPTION: Restrict to date_i and sum across all units. Creates a dataset
  # both for movement in and movement out.
  
  # SERVER USE: Summary table
  
  # INPUTS
  # date_i:   Specific date
  # df:       Dataset
  # unit:     Districts or Wards (used for naming .Rds file)
  # timeunit: Daily or Weekly (used for naming .Rds file)
  # admin_df: Admin unit dataframe, to merge in other variables
  
  check_inputs(unit, timeunit)
  print(date_i)
  
  #df <- df[df$date %in% date_i,]

  #### Out of Admin Unit
  df_out_of <- sum_with_change_vars(df,
                       date_i,
                       "origin")
  
  df_out_of <- merge(admin_df,  
                     df_out_of, 
                     by.x = "region",
                     by.y = "region_origin",
                     all.x = T) %>%
    arrange(desc(value_count)) %>%
    dplyr::rename(name = name_origin) %>%
    dplyr::select(name, value_count, value_zscore_feb, value_perchange_feb, province)
  
  saveRDS(df_out_of, file.path(DASHBOARD_DATA_PATH,
                               paste0(unit, "_Movement Out of_",timeunit,"_", date_i, ".Rds")))
  
  #### Into Admin Unit
  df_out_of <- sum_with_change_vars(df,
                                    date_i,
                                    "dest")
  
  df_out_of <- merge(admin_df,  
                     df_out_of, 
                     by.x = "region",
                     by.y = "region_dest",
                     all.x = T) %>%
    arrange(desc(value_count)) %>%
    dplyr::rename(name = name_dest) %>%
    dplyr::select(name, value_count, value_zscore_feb, value_perchange_feb, province)
  
  saveRDS(df_out_of, file.path(DASHBOARD_DATA_PATH,
                               paste0(unit, "_Movement Into_",timeunit,"_", date_i, ".Rds")))
  
}


prep_movement_adminname_i <- function(name_i, df, unit, timeunit){
  
  # DESCRIPTION: Subsets dataset to a specific date and exports. Dataset is 
  # sorted by region. Sorting by region is used for mapping, so in server
  # data can be directly imported then plotted in leaflet, without having to
  # merge with the spatial layer.
  
  # SERVER USE: Line graph over time
  
  # INPUTS
  # name_i:   Admin Unit Name
  # df:       Dataset
  # unit:     Districts or Wards (used for naming .Rds file)
  # timeunit: Daily or Weekly (used for naming .Rds file)
  
  check_inputs(unit, timeunit)
  print(name_i %>% as.character())
  
  #### Out of Ward
  df_clean <- df %>%
    filter(name_origin %in% name_i) %>%
    group_by(date, dow) %>%
    summarise(value_count = sum(value_count)) 
  
  saveRDS(df_clean, file.path(DASHBOARD_DATA_PATH,
                              paste0(unit, "_Movement Out of_",timeunit,"_", name_i, ".Rds")))
  
  #### Into Ward
  df_clean <- df %>%
    filter(name_dest %in% name_i) %>%
    group_by(date, dow) %>%
    summarise(value_count = sum(value_count))
  
  saveRDS(df_clean, file.path(DASHBOARD_DATA_PATH,
                              paste0(unit, "_Movement Into_",timeunit,"_", name_i, ".Rds")))
  
}

prep_movement_adminname_i_date_i <- function(date_i, name_i, df, unit, timetype, orig_dest, admin_df, admin_sp){
  
  # DESCRIPTION: Subsets dataset to a specific date and admin unit and exports.
  # The data is merged with the spatial layer and sorted in the same way
  # to ensure the length and order of the data is the same as the shapefile.
  # This makes it was data can be directly loaded and plotted in leaflet
  # without additional processing.
  
  # SERVER USE: Map
  
  # INPUTS
  # date_i:    Date i
  # name_i:    Admin Unit Name
  # df:        Dataset - ** Should already be subsetted to by origin or destination 
  #            name in order to increase speed of the function.
  # unit:      Districts or Wards (used for naming .Rds file)
  # timeunit:  Daily or Weekly (used for naming .Rds file)
  # orig_dest: "in" or "out" for movement in/movement out
  # admin_df:  Dataframe from spatial layer
  # admin_sp:  Spatial layer with all variables
  
  if(orig_dest %in% "out"){
    
    #### Out of Ward
    df_clean <- df %>%
      #filter(ward_name_origin %in% ward_i) %>%
      filter(date %in% date_i) 
    
    df_clean <- merge(admin_df, df_clean,
                      by.x = "region",
                      by.y = "region_dest",
                      all.x=T) %>%
      arrange(region) 
    
    df_clean$label_count[is.na(df_clean$value_count)] <- "15 or fewer <br> or information not available"
    df_clean$label_change[is.na(df_clean$value_count)] <- "15 or fewer <br> or information not available"
    
    # Origin/Destination Label
    df_clean$value_count[is.na(df_clean$value_count)] <- 0
    df_clean$value_zscore_feb[is.na(df_clean$value_count)] <- 0
    df_clean$value_perchange_feb[is.na(df_clean$value_count)] <- 0
    
    df_clean$value_count[is.na(df_clean$value_count_baseline)] <- 0
    df_clean$value_zscore_feb[is.na(df_clean$value_count_baseline)] <- 0
    df_clean$value_perchange_feb[is.na(df_clean$value_count_baseline)] <- 0
    
    # Leave NA for O/D Orig/Dist
    df_clean$value_count[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_zscore_feb[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_perchange_feb[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    
    df_clean$value_count[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$label_count[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                      "<br>",
                                                                                                      "Origin ",
                                                                                                      
                                                                                                      # Remove S at end
                                                                                                      substr(unit, 
                                                                                                             1, 
                                                                                                             nchar(unit)-1))
    
    df_clean$label_change[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                    "<br>",
                                                                                                    "Origin ",
                                                                                                    
                                                                                                    # Remove S at end
                                                                                                    substr(unit, 
                                                                                                           1, 
                                                                                                           nchar(unit)-1))
    
    saveRDS(df_clean[,c("value_count", 
                        "value_zscore_feb",
                        "value_perchange_feb",
                        "label_count",
                        "label_change")], file.path(DASHBOARD_DATA_PATH,
                                                           paste0(unit, "_Movement Out of_",timetype,"_", name_i,"_",date_i,".Rds")))
    
  }
  
  if(orig_dest %in% "in"){
    #### Into Ward
    df_clean <- df %>%
      #filter(ward_name_dest %in% ward_i) %>%
      filter(date %in% date_i) 
    
    df_clean <- merge(admin_df, df_clean,
                      by.x = "region",
                      by.y = "region_origin",
                      all.x=T) %>%
      arrange(region) 
    
    df_clean$label_count[is.na(df_clean$value_count)] <- "15 or fewer <br> or information not available"
    df_clean$label_change[is.na(df_clean$value_count)] <- "15 or fewer <br> or information not available"
    
    df_clean$value_count[is.na(df_clean$value_count)] <- 0
    df_clean$value_zscore_feb[is.na(df_clean$value_count)] <- 0
    df_clean$value_perchange_feb[is.na(df_clean$value_count)] <- 0
    
    df_clean$value_count[is.na(df_clean$value_count_baseline)] <- 0
    df_clean$value_zscore_feb[is.na(df_clean$value_count_baseline)] <- 0
    df_clean$value_perchange_feb[is.na(df_clean$value_count_baseline)] <- 0
    
    # Leave NA for O/D Orig/Dist
    df_clean$value_count[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_zscore_feb[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_perchange_feb[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    
    df_clean$label_count[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                      "<br>",
                                                                                                      "Destination ",
                                                                                                      
                                                                                                      # Remove S at end
                                                                                                      substr(unit, 
                                                                                                             1, 
                                                                                                             nchar(unit)-1))
    
    df_clean$label_change[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                     "<br>",
                                                                                                     "Destination ",
                                                                                                     
                                                                                                     # Remove S at end
                                                                                                     substr(unit, 
                                                                                                            1, 
                                                                                                            nchar(unit)-1))
    
    saveRDS(df_clean[,c("value_count", 
                        "value_zscore_feb",
                        "value_perchange_feb",
                        "label_count",
                        "label_change")], file.path(DASHBOARD_DATA_PATH,
                                                           paste0(unit, "_Movement Into_",timetype,"_", name_i,"_",date_i,".Rds")))
    
  }
}






##### *** PREP MAIN DATASETS *** ##### =========================================

unit <- "Districts"
timeunit <- "Weekly"
for(unit in c("Districts", "Wards")){ # "Wards", "Districts"
  for(timeunit in c("Daily", "Weekly")){
    
    print(paste(unit, timeunit, "--------------------------------------------"))
    
    # Load Data ------------------------------------------------------------------
    if(unit %in% "Districts"){
      CLEAN_DATA_PATH <- CLEAN_DATA_ADM2_PATH
      admin_df <- district_sp_regiononly_df # For prep_movement_adminname_i_date_i 
      admin_sp <- district_sp # For prep_movement_adminname_i_date_i 
      
    }
    
    if(unit %in% "Wards"){
      CLEAN_DATA_PATH <- CLEAN_DATA_ADM3_PATH
      admin_df <- ward_sp_regiononly_df # For prep_movement_adminname_i_date_i 
      admin_sp <- ward_sp # For prep_movement_adminname_i_date_i 
    }
    
    # Short name for time unit, needed for file paths
    if(timeunit %in% "Daily") timeunit_short <- "day"
    if(timeunit %in% "Weekly") timeunit_short <- "week"
    
    ## Non-OD
    df_density <- readRDS(file.path(CLEAN_DATA_PATH, 
                                    paste0("count_unique_subscribers_per_region_per_",
                                           timeunit_short,
                                           ".Rds")))
    
    df_distance <- readRDS(file.path(CLEAN_DATA_PATH, 
                                    paste0("median_distance_per_",
                                           timeunit_short,
                                           ".Rds")))
    
    df_movement_net <- readRDS(file.path(CLEAN_DATA_PATH, 
                                     paste0("origin_destination_connection_matrix_net_per_",
                                            timeunit_short,
                                            ".Rds")))
    
    ## O-D
    df_movement  <- readRDS(file.path(CLEAN_DATA_PATH, 
                                      paste0("origin_destination_connection_matrix_per_",
                                             timeunit_short,
                                             ".Rds")))

    # Process Density Data -----------------------------------------------------
    if(PROCESS_DENSITY_DATA){
      
      ### prep_density_date_i
      temp <- lapply(unique(df_density$date),
                     prep_density_date_i,
                     df_density,
                     unit,
                     timeunit,
                     "Density")
      
      ### prep_density_admin_i
      temp <- lapply(unique(df_density$name),
                     prep_density_admin_i,
                     df_density,
                     unit,
                     timeunit,
                     "Density")
    
    }
    
    # Process Distance Data ----------------------------------------------------
    if(PROCESS_DISTANCE_DATA){
      ### prep_density_date_i
      temp <- lapply(unique(df_distance$date),
                     prep_density_date_i,
                     df_distance,
                     unit,
                     timeunit,
                     "Median Distance Traveled")
      
      ### prep_density_admin_i
      temp <- lapply(unique(df_distance$name),
                     prep_density_admin_i,
                     df_distance,
                     unit,
                     timeunit,
                     "Median Distance Traveled")
    }
    
    # Process Net Movement Data ------------------------------------------------
    if(PROCESS_MOVEMENT_NET_DATA){
      ### prep_density_date_i
      temp <- lapply(unique(df_movement_net$date),
                     prep_density_date_i,
                     df_movement_net,
                     unit,
                     timeunit,
                     "Net Movement")
      
      ### prep_density_admin_i
      temp <- lapply(unique(df_movement_net$name),
                     prep_density_admin_i,
                     df_movement_net,
                     unit,
                     timeunit,
                     "Net Movement")
    }

    # Process Movement Data ----------------------------------------------------
    if(PROCESS_MOVEMENT_DATA){
      ### prep_movement_date_i
      temp <- lapply(unique(df_movement$date),  
                     prep_movement_date_i, 
                     df_movement,  
                     unit,
                     timeunit,
                     admin_df)
      
      ### prep_movement_adminname_i
      temp <- lapply(unique(unique(df_movement$name_dest),
                            unique(df_movement$name_origin)),  
                     prep_movement_adminname_i, 
                     df_movement,  
                     unit,
                     timeunit)
      
      ### prep_movement_adminname_i_date_i
      # Loop through units and dates; apply function separately for moving in 
      # and out
      i <- 1
      t <- Sys.time()
      for(name_i in unique(unique(df_movement$name_dest),
                           unique(df_movement$name_origin))){
        
        print(paste(i, unit, timeunit))
        
        temp <- mclapply(unique(df_movement$date), 
                         prep_movement_adminname_i_date_i, 
                         name_i, 
                         df_movement[df_movement$name_origin %in% name_i,], 
                         unit, 
                         timeunit,
                         "out", 
                         admin_df,
                         admin_sp,
                         mc.cores = N_CORES)
        
        temp <- mclapply(unique(df_movement$date), 
                         prep_movement_adminname_i_date_i, 
                         name_i, 
                         df_movement[df_movement$name_dest %in% name_i,], 
                         unit, 
                         timeunit,
                         "in", 
                         admin_df,
                         admin_sp,
                         mc.cores = N_CORES)
      
        i <- i + 1
        difftime(Sys.time(), t, units="secs") %>% print()
        t <- Sys.time()
      }
    
    }

  }
}

