# Move Data into Dashboard Directory

# This script converts the cleaned data files into a different format for the
# dashboard. In particular, much of the processing is done here to be avoided
# in the shiny server. The goal is to create files that are 
# (1) Small so can be quickly loaded and 
# (2) Require minimal manipulation to avoid processing time in the server
# (3) Use faster file types when relevant (data.tables, not data.frames)

##### Settings ##### -----------------------------------------------------------

#### Number of cores for parallel processing
N_CORES <- 1

#### Select which datasets to process
# Non-OD
PROCESS_DENSITY_DATA       <- T
PROCESS_MOVEMENT_NET_DATA  <- T
PROCESS_DISTANCE_MEAN_DATA <- T
PROCESS_DISTANCE_STD_DATA  <- T

# OD
PROCESS_MOVEMENT_DATA      <- T

#### Delete previous files before running? Useful if change naming conventions
# of files, so need to get rid of old files. Otherwise will just add or overwrite.
REMOVE_PREVIOUS_FILES <- F

##### Remove Previous Files ##### ----------------------------------------------
# THIS IS NOT STABLE. Will also ignore district and ward polygon files,
# and other files not added here. should ignore these other files

if(REMOVE_PREVIOUS_FILES){
  temp <- list.files(DASHBOARD_DATA_ONEDRIVE_PATH, 
                     full.names = T, 
                     pattern = "*.Rds") %>%
    lapply(file.remove)
}

##### Load/Prep Spatial Data ##### ---------------------------------------------
# Need unit level data from shapefiles. Used in ensuring all regions are in
# telecom agg data.

ward_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
ward_sp_regiononly_df <- ward_sp[,c("region")]@data

district_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
district_sp_regiononly_df <- district_sp[,c("region")]@data

##### Prep Telecom Data ##### --------------------------------------------------

unit <- "Wards"
timeunit <- "Daily"
for(unit in c("Districts", "Wards")){ # "Wards", "Districts"
  for(timeunit in c("Weekly", "Daily")){
    
    print(paste(unit, timeunit, "--------------------------------------------"))
    
    # Set Parameters -----------------------------------------------------------
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
    if(timeunit %in% "Daily") timeunit_short <- "daily"
    if(timeunit %in% "Weekly") timeunit_short <- "weekly"
    
    ##### Process Non-OD Data ##### --------------------------------------------
    
    # Process Density Data -----------------------------------------------------
    if(PROCESS_DENSITY_DATA){
      
      ### Load Data
      df_density <- readRDS(file.path(CLEAN_DATA_PATH, 
                                      paste0("i3_",
                                             timeunit_short,
                                             ".Rds")))
      
      ### make_sparkline
      temp <- make_sparkline(df_density,
                             unit = unit,
                             timeunit = timeunit,
                             varname = "Density")
      
      ### prep_density_date_i
      temp <- lapply(unique(df_density$date),
                     prep_nonod_date_i,
                     df = df_density,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Density",
                     vars_include = "density")
      
      ### prep_density_admin_i
      temp <- lapply(unique(df_density$name),
                     prep_nonod_admin_i,
                     df = df_density,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Density",
                     vars_include = "density")
      
    }
    
    # Process Mean Distance Data -----------------------------------------------
    if(PROCESS_DISTANCE_MEAN_DATA){
      
      ## Load data
      df_distance_mean <- readRDS(file.path(CLEAN_DATA_PATH, 
                                            paste0("i7_",
                                                   timeunit_short,
                                                   "_mean_distance",
                                                   ".Rds")))
      
      ### make_sparkline
      temp <- make_sparkline(df_distance_mean,
                             unit = unit,
                             timeunit = timeunit,
                             varname = "Density")
      
      ### prep_density_date_i
      temp <- lapply(unique(df_distance_mean$date),
                     prep_nonod_date_i,
                     df = df_distance_mean,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Mean Distance Traveled")
      
      ### prep_density_admin_i
      temp <- lapply(unique(df_distance_mean$name),
                     prep_nonod_admin_i,
                     df = df_distance_mean,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Mean Distance Traveled")
    }
    
    # Process Standard Dev Distance Data ---------------------------------------
    if(PROCESS_DISTANCE_STD_DATA){
      
      ## Load data
      df_distance_stdev <- readRDS(file.path(CLEAN_DATA_PATH, 
                                             paste0("i7_",
                                                    timeunit_short,
                                                    "_stdev_distance",
                                                    ".Rds")))
      
      ### make_sparkline
      temp <- make_sparkline(df_distance_stdev,
                             unit = unit,
                             timeunit = timeunit,
                             varname = "Density")
      
      ### prep_density_date_i
      temp <- lapply(unique(df_distance_stdev$date),
                     prep_nonod_date_i,
                     df = df_distance_stdev,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Std Dev Distance Traveled")
      
      ### prep_density_admin_i
      temp <- lapply(unique(df_distance_stdev$name),
                     prep_nonod_admin_i,
                     df = df_distance_stdev,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Std Dev Distance Traveled")
    }
    
    # Process Net Movement Data ------------------------------------------------
    if(PROCESS_MOVEMENT_NET_DATA){
      
      ## Load Data
      df_movement_net <- readRDS(file.path(CLEAN_DATA_PATH, 
                                           paste0("i5_net_",
                                                  timeunit_short,
                                                  ".Rds")))
      
      ### make_sparkline
      temp <- make_sparkline(df_movement_net,
                             unit = unit,
                             timeunit = timeunit,
                             varname = "Density")
      
      ### prep_density_date_i
      temp <- lapply(unique(df_movement_net$date),
                     prep_nonod_date_i,
                     df = df_movement_net,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Net Movement")
      
      ### prep_density_admin_i
      temp <- lapply(unique(df_movement_net$name),
                     prep_nonod_admin_i,
                     df = df_movement_net,
                     unit = unit,
                     timeunit = timeunit,
                     varname = "Net Movement")
    }
    
    ##### Process OD Data ##### ------------------------------------------------
    
    # Process Movement Data ----------------------------------------------------
    if(PROCESS_MOVEMENT_DATA){
      
      #### Load OD Datasets to Process
      df_movement  <- readRDS(file.path(CLEAN_DATA_PATH, 
                                        paste0("i5_",
                                               timeunit_short,
                                               ".Rds")))
      df_movement <- df_movement[!is.na(df_movement$date),]
      
      ### prep_movement_date_i
      temp <- lapply(unique(df_movement$date),  
                     prep_od_date_i, 
                     df_movement,  
                     unit,
                     timeunit,
                     admin_sp)
      
      
      ### prep_movement_adminname_i
      temp <- lapply(unique(unique(df_movement$name_dest),
                            unique(df_movement$name_origin)),  
                     prep_od_adminname_i, 
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
                         prep_od_adminname_i_date_i, 
                         name_i, 
                         df_movement[df_movement$name_origin %in% name_i,], 
                         unit, 
                         timeunit,
                         "out", 
                         admin_df,
                         admin_sp,
                         mc.cores = N_CORES)
        
        temp <- mclapply(unique(df_movement$date), 
                         prep_od_adminname_i_date_i, 
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

