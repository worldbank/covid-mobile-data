# Functions Used in Preparing Data for Dashboard

#### Helper Functions ##### ----------------------------------------------------
check_inputs <- function(unit, timeunit){
  #try(if(!(unit %in% c("Districts", "Wards"))) stop("unit must be Districts or Wards"))
  #try(if(!(timeunit %in% c("Daily", "Weekly"))) stop("unit must be Daily or Weekly"))
}

##### Density ##### ------------------------------------------------------------
prep_nonod_date_i <- function(date_i, 
                              df, 
                              unit, 
                              timeunit, 
                              varname,
                              vars_include = NULL){
  
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
  # vars_include: Vector of additional variable (names) to include in output
  
  check_inputs(unit, timeunit)
  print(date_i)
  
  keep_vars <- c("name", "province",
                 "value", "value_perchange_base", "value_zscore_base", 
                 "label_base", "label_level")
  keep_vars <- c(keep_vars, vars_include)
  
  df <- df %>%
    filter(date %in% date_i) %>%
    mutate(region = region %>% as.character()) %>%
    arrange(region) %>%
    dplyr::select(keep_vars) 
  
  saveRDS(df, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
                        paste0(unit, "_",varname,"_",timeunit,"_", date_i, ".Rds")))
}

prep_nonod_admin_i <- function(name_i, 
                               df, 
                               unit, 
                               timeunit, 
                               varname,
                               vars_include = NULL){
  
  # DESCRIPTION: Subsets dataset to a specific admin unit and exports. 
  
  # SERVER USE: (1) Line graph by time
  
  # INPUTS
  # name_i:   Name of admin unit
  # df:       Dataset
  # unit:     Districts or Wards (used for naming .Rds file)
  # timeunit: Daily or Weekly (used for naming .Rds file)
  # varname:  Variable name, used for naming .Rds file (Density, Median Distance Traveled)
  # vars_include: Vector of additional variable (names) to include in output
  
  check_inputs(unit, timeunit)
  print(name_i)
  
  #### Add day of week
  if(grepl("^20[[:digit:]][[:digit:]]", df$date[1])){
    df$dow <- df$date %>% substring(1,10) %>% as.Date() %>% wday()
  } else{
    df$dow <- 1 # dummy week variable
  }
  
  keep_vars <- c("date", "value", "dow")
  keep_vars <- c(keep_vars, vars_include)
  
  df <- df %>%
    filter(name %in% name_i) %>%
    dplyr::select(keep_vars) 
  
  if(timeunit %in% "Daily"){
    print(head(df$date))
    df$date <- df$date %>% as.Date()
  }
  
  saveRDS(df, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
                        paste0(unit, "_",varname,"_",timeunit,"_", name_i, ".Rds")))
}

##### Movement ##### -----------------------------------------------------------

prep_od_date_i <- function(date_i, df, unit, timeunit, admin_df){
  
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
  
  #### Out of Admin Unit
  df_out_of <- df %>%
    group_by(date, region_origin) %>%
    dplyr::summarise(value = sum(value, na.rm=T)) %>%
    ungroup() %>%
    tp_less15_NA(threshold = 0) %>% # summarize makes all NA 0, so back to NA here
    
    tp_standardize_vars("date", "region_origin", "value") %>%
    tp_add_baseline_comp_stats() %>%
    tp_add_polygon_data(admin_sp) %>%
    
    filter(date == date_i) %>%
    arrange(desc(value)) %>%
    dplyr::select(name, value, value_zscore_base, value_perchange_base, province)
  
  saveRDS(df_out_of, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
                               paste0(unit, "_Movement Out of_",timeunit,"_", date_i, ".Rds")))
  
  #### Into Admin Unit
  df_into <- df %>%
    group_by(date, region_dest) %>%
    dplyr::summarise(value = sum(value, na.rm=T)) %>%
    ungroup() %>%
    tp_less15_NA(threshold = 0) %>% # summarize makes all NA 0, so back to NA here
    
    tp_standardize_vars("date", "region_dest", "value") %>%
    tp_add_baseline_comp_stats() %>%
    tp_add_polygon_data(admin_sp) %>%
    
    filter(date == date_i) %>%
    arrange(desc(value)) %>%
    dplyr::select(name, value, value_zscore_base, value_perchange_base, province)
  
  saveRDS(df_into, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
                             paste0(unit, "_Movement Into_",timeunit,"_", date_i, ".Rds")))
  
}


prep_od_adminname_i <- function(name_i, df, unit, timeunit){
  
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

  df <- as.data.table(df)
  
  #### Out of Ward
  df_origin <- df[df$name_origin == name_i,]
  
  ## Add day of week
  if(grepl("^20[[:digit:]][[:digit:]]", df$date[1])){
    df_origin$dow <- df_origin$date %>% as.Date() %>% wday()
  } else{
    df_origin$dow <- 1 # dummy week variable
  }

  df_origin <- df_origin[, .(value = sum(value, na.rm=T)), 
                                    by = list(date, dow)]
  
  df_origin <- df_origin %>% as.data.frame()
  
  saveRDS(df_origin, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
                              paste0(unit, "_Movement Out of_",timeunit,"_", name_i, ".Rds")))
  
  #### Into Ward
  df_dest <- df[df$name_dest == name_i,]
  
  ## Add day of week
  if(grepl("^20[[:digit:]][[:digit:]]", df$date[1])){
    df_dest$dow <- df_dest$date %>% as.Date() %>% wday()
  } else{
    df_dest$dow <- 1 # dummy week variable
  }
  
  df_dest <- df_dest[, .(value = sum(value, na.rm=T)), 
                         by = list(date, dow)]
  
  df_dest <- df_dest %>% as.data.frame()

  saveRDS(df_dest, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
                              paste0(unit, "_Movement Into_",timeunit,"_", name_i, ".Rds")))
  
}

prep_od_adminname_i_date_i <- function(date_i, name_i, df, unit, timetype, orig_dest, admin_df, admin_sp){
  
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
      mutate(region = region %>% as.character()) %>%
      arrange(region) 
    
    df_clean$label_level[is.na(df_clean$value)] <- "15 or fewer <br> or information not available"
    df_clean$label_base[is.na(df_clean$value)] <- "15 or fewer <br> or information not available"
    
    # Origin/Destination Label
    df_clean$value[is.na(df_clean$value)] <- 0
    df_clean$value_zscore_base[is.na(df_clean$value)] <- 0
    df_clean$value_perchange_base[is.na(df_clean$value)] <- 0
    
    df_clean$value_zscore_base[is.na(df_clean$value_base)] <- 0
    df_clean$value_perchange_base[is.na(df_clean$value_base)] <- 0
    
    # Leave NA for O/D Orig/Dist
    df_clean$value[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_zscore_base[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_perchange_base[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    
    df_clean$value[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$label_level[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                    "<br>",
                                                                                                    "Origin ",
                                                                                                    
                                                                                                    # Remove S at end
                                                                                                    substr(unit, 
                                                                                                           1, 
                                                                                                           nchar(unit)-1))
    
    df_clean$label_base[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                   "<br>",
                                                                                                   "Origin ",
                                                                                                   
                                                                                                   # Remove S at end
                                                                                                   substr(unit, 
                                                                                                          1, 
                                                                                                          nchar(unit)-1))
    
    saveRDS(df_clean[,c("value", 
                        "value_zscore_base",
                        "value_perchange_base",
                        "label_level",
                        "label_base")], file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
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
      mutate(region = region %>% as.character()) %>%
      arrange(region) 
    
    df_clean$label_level[is.na(df_clean$value)] <- "15 or fewer <br> or information not available"
    df_clean$label_base[is.na(df_clean$value)] <- "15 or fewer <br> or information not available"
    
    df_clean$value[is.na(df_clean$value)] <- 0
    df_clean$value_zscore_base[is.na(df_clean$value)] <- 0
    df_clean$value_perchange_base[is.na(df_clean$value)] <- 0
    
    df_clean$value_zscore_base[is.na(df_clean$value_base)] <- 0
    df_clean$value_perchange_base[is.na(df_clean$value_base)] <- 0
    
    # Leave NA for O/D Orig/Dist
    df_clean$value[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_zscore_base[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    df_clean$value_perchange_base[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- NA
    
    df_clean$label_level[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                    "<br>",
                                                                                                    "Destination ",
                                                                                                    
                                                                                                    # Remove S at end
                                                                                                    substr(unit, 
                                                                                                           1, 
                                                                                                           nchar(unit)-1))
    
    df_clean$label_base[df_clean$region %in% admin_sp$region[admin_sp$name %in% name_i]] <- paste0(name_i,
                                                                                                   "<br>",
                                                                                                   "Destination ",
                                                                                                   
                                                                                                   # Remove S at end
                                                                                                   substr(unit, 
                                                                                                          1, 
                                                                                                          nchar(unit)-1))
    
    saveRDS(df_clean[,c("value", 
                        "value_zscore_base",
                        "value_perchange_base",
                        "label_level",
                        "label_base")], file.path(DASHBOARD_DATA_ONEDRIVE_PATH,
                                                  paste0(unit, "_Movement Into_",timetype,"_", name_i,"_",date_i,".Rds")))
    
  }
}

