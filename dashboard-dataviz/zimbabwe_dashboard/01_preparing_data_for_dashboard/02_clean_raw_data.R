#### Clean Data

#### DESCRIPTION: Prepares into format needed for dashboard. Deals with outliers, 
# etc.Datasets are saved as .Rds files in the clean_data folder. For consistenty, 
# the name of the original dataset is preserved.

#### BASIC STRUCTURE
# Organization is a product of things somewhat being cobbled together & the 
# use of this script expanding -- for example, adding district by just looping
# over things because I'm lazy and didn't want to rework much. 
# 1. Main cleaning code
#  1.1 Helper functions
#   
# 2. Load and clean spatial datasets (need standardzied variable names for
#    rest of code)
# 3. Loop through unit type (district, ward)
#    ---Load dataset
#    ---Run cleaning code
#    ---In some cases do a bit of final cleaning
#    ---Export

# Set if data should be exported
EXPORT <- T

CLEAN_NET_DAILY_MOVEMENT            <- T
CLEAN_OD_CONNECTION_MATRIX          <- T
CLEAN_DIR_REGIONAL_PAIR_CONNECTIONS <- F
CLEAN_MEDIAN_DISTANCE_TRAVELED      <- T
CLEAN_UNIQUE_RESIDENTS              <- T
CLEAN_UNIQUE_SUBSCRIBERS            <- T

# *** FUNCTIONS ================================================================

# HTML labels function ---------------------------------------------------------
# This creates slightly diffrent labels for each type of data. It does
# assume that all column names are standardized so it might be a bit
# unstable. Not great functional programming :/

make_change_label <- function(data, 
                              timeunit,
                              OD){
  
  # Different labels if OD or density indicators
  if(OD){
    label_head <- 
      paste0(
        data$name_origin, 
        " to ", 
        data$name_dest)
    
  } else{
    label_head <- data$name
  }
  
  # What goes into all labels
  label_middle <- 
    paste0("This ", timeunit, "'s value: ",data$value_count, ".")
  
  label_middle <- ifelse(abs(data$value_count) <= 15 | 
                           abs(data$value_count_baseline) <= 15 | 
                           is.na(data$value_count),
                         "15 or fewer<br>or information not available",
                         label_middle)
  
  # If first day/week of the series remove info
  # on previous time unit
  
  label_tail <- paste0("The baseline February value was: ",
                       data$value_count_baseline, ".<br>",
                       "Compared to baseline, this ", timeunit, "<br>",
                       "had a ", 
                       data$value_perchange_feb %>% round(2) %>% abs(),
                       "% ",
                       data$inc_dec_base,
                       " (a ",
                       data$value_zscore_feb %>% round(2), 
                       " z-score)."
  )
  
  label_tail <- ifelse(data$date == min(data$date),
                       "",
                       label_tail)
  
  label_tail <- ifelse(abs(data$value_count) <= 15 | 
                         abs(data$value_count_baseline) <= 15 | 
                         is.na(data$value_count),
                       "",
                       label_tail)
  
  # Construct final label
  html_label <- 
    paste(label_head,
          label_middle,
          label_tail,
          sep = "<br>") %>%
    str_replace_all("<br><br>", "<br>")%>%
    str_replace_all("<br>$", "")
  
}

make_count_label <- function(data,
                             timeunit,
                             OD = F){
  
  # Different labels if OD or density indicators
  if(OD){
    label_head <- 
      paste0(
        data$name_origin, 
        " to ", 
        data$name_dest)
    
  } else{
    label_head <- data$name
  }
  
  # What goes into all labels
  label_middle <- 
    paste0("This ", timeunit, "'s value: ",data$value_count, ".")
  
  # Take absolute value to account for negative values, like net
  label_middle <- ifelse(abs(data$value_count) <= 15 |
                         is.na(data$value_count),
                         "15 or fewer<br>or information not available",
                         label_middle)
  
  # If first day/week of the series remove info
  # on previous time unit
  label_tail <- paste0("Last ", timeunit, "'s value: ", 
                       data$value_lag, ".", 
                       "<br>", 
                       "This ", timeunit, " had a ",
                       data$value_count_chng %>% abs(), 
                       " ",
                       "(",
                       data$value_perchng %>% round(2) %>% abs(),
                       "%",
                       ") ",
                       data$inc_dec,
                       "<br>compared to the previous ", 
                       timeunit, ".")
  
  label_tail <- ifelse(data$date == min(data$date),
                       "",
                       label_tail)
  
  label_tail <- ifelse(abs(data$value_count) <= 15 | 
                         is.na(data$value_count) |
                         (data$value_perchng %in% Inf) |
                         is.na(data$value_perchng),
                       "",
                       label_tail)
  
  # Construct final label
  html_label <- 
    paste(label_head,
          label_middle,
          label_tail,
          sep = "<br>")
  
  return(html_label)
  
}

# Clean Date Variable ----------------------------------------------------------
clean_week_variable <- function(data, varname, type){
  # DESCRIPTION: Function to clean week variable into text format (e.g., 
  # Feb 01 - Feb 07).
  
  # INPUTS:
  # data: dataset 
  # varname: variable name (string) of week variable
  # type: either "integer" or "date."
  # If integer converts, for example, 6 -- > Feb 01 - Feb 07
  # If date (date of start of week), converts yyyy-mm-dd to text
  
  # Make a temp variable name so can access variable using $ instead of
  # [[""]] as easier to read. Add _TEMP to reduce change name doesn't conflict
  # with existing variable name. Not most stable but oh well.
  data$week_var_TEMP <- data[[varname]]
  
  # Inputs could be numeric, factor or date -- so for consistency convert to 
  # character as the output will be character.
  data$week_var_TEMP <- data$week_var_TEMP %>% as.character()
  
  if(type == "integer"){
    
    data$week_var_TEMP[data$week_var_TEMP %in% "6"] <- "Feb 01 - Feb 07"
    data$week_var_TEMP[data$week_var_TEMP %in% "7"] <- "Feb 08 - Feb 14"
    data$week_var_TEMP[data$week_var_TEMP %in% "8"] <- "Feb 15 - Feb 21"
    data$week_var_TEMP[data$week_var_TEMP %in% "9"] <- "Feb 22 - Feb 28"
    data$week_var_TEMP[data$week_var_TEMP %in% "10"] <- "Feb 29 - Mar 06"
    data$week_var_TEMP[data$week_var_TEMP %in% "11"] <- "Mar 07 - Mar 13"
    data$week_var_TEMP[data$week_var_TEMP %in% "12"] <- "Mar 14 - Mar 20"
    data$week_var_TEMP[data$week_var_TEMP %in% "13"] <- "Mar 21 - Mar 27"
    data$week_var_TEMP[data$week_var_TEMP %in% "14"] <- "Mar 28 - Apr 03"
    
  } else if (type == "date"){
    
    data$week_var_orig_TEMP <- data$week_var_TEMP
    
    data$week_var_TEMP <- "Feb 01 - Feb 07"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-02-08"] <- "Feb 08 - Feb 14"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-02-15"] <- "Feb 15 - Feb 21"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-02-22"] <- "Feb 22 - Feb 28"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-02-29"] <- "Feb 29 - Mar 06"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-03-07"] <- "Mar 07 - Mar 13"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-03-14"] <- "Mar 14 - Mar 20"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-03-21"] <- "Mar 21 - Mar 27"
    data$week_var_TEMP[data$week_var_orig_TEMP >= "2020-03-28"] <- "Mar 28 - Apr 03"
    
    data$week_var_orig <- NULL
  }
  
  # Change variable name back to original
  data[[varname]] <- data$week_var_TEMP
  data$week_var_TEMP <-  NULL
  
  return(data)
}

# Main Cleaning Function -------------------------------------------------------
if(F){
  # Run this in order to go create variables to run code within the function
  # for testing.
  data = read.csv(file.path(RAW_DATA_ADM3_PATH, "count_unique_subscribers_per_region_per_day.csv"), stringsAsFactors=F)
  values_var = "subscriber_count"
  dates_var = "visit_date"
  region_var = "region"
  type = "non-od"
  timeunit = "day"
  agg_od_day_to_wk = F
}

clean_data <- function(data, 
                       values_var, 
                       dates_var, 
                       region_var, 
                       type, 
                       timeunit, 
                       agg_od_day_to_wk,
                       admin_sp,
                       clean_outliers=T){
  
  # DESCRIPTION: Function to clean indicators data. Performs the following cleaning
  # steps:
  # (1) For each region-date pair, completes the dataset for any missing observations
  #     where any missing observation value is given a value of zero.
  # (2) Deals with outliers and missing observations
  # (2) Renames variables so that they are consistent across datasets. Output variable
  #     names are: 
  #               date, 
  #               region, 
  #               value
  # (3) Adds a label to be displayed on leaflet
  # (4) In some cases we only have the O-D data at the daily level. If needed,
  #     aggregates to the weekly level
  # (5) Merges relevant variables (e.g., ward_name) from the ward shapefile
  
  # INPUTS:
  # data:      Input dataframe
  # values_var:       Name of variable that captures the values
  # dates_var:        Name of variable that captures the date or time element
  # region_var:       Name of variable that captures the region/unit
  # type:             Either "non-od" or "od". The label is slightly different in 
  #                   these cases
  # timeunit:         "day" or "week"
  # agg_od_day_to_wk: If TRUE, aggregates an O-D matrix at the daily level
  #                   to the weekly level.
  # admin_sp:         Spatial Layer 
  # zeros_as_na:      Whether to treat 0s as NAs (eg, too little information)
  
  # STRUCTURE:
  # 1. Prep Dataset
  # 2. Outliers and Missings
  # 3. Daily --> Weekly
  # 4. Add Z-score and Percent Change
  # 5. Clean up output dataset
  # 6. Add labels and additional variables
  
  ##### 1. Prep Dataset ##### --------------------------------------------------
  
  #### Add generic variables
  data$values_var <- data[[values_var]]
  data$dates_var  <- data[[dates_var]]
  data$region_var <- data[[region_var]] %>% as.character()
  
  #### If "non-od" level, fill data with all regions
  # In some non-od level datasets, some regions have no observations and are
  # not included in the dataset. Here we fill the dataset with all regions.
  # For od level datasets, this would create too large of datasets that would
  # really slow down shiny; we deal with missing regions in shiny for od datasets.
  if(type %in% "non-od"){
    
    regions_not_in_data <- admin_sp$region[!(admin_sp$region %in% data$region_var)]
    regions_to_add <- lapply(regions_not_in_data, function(region_i){
      data_i <- data[1,]
      data_i$region_var <- region_i
      data_i$values_var <- 0
      return(data_i)
    }) %>% bind_rows()
    
    data <- bind_rows(data, regions_to_add)
  }
  
  #### Format Date Variable
  # Convert "day" variable to day format and "week" variable to ordered factor
  # variable with appropriate labels.
  if(timeunit %in% "day"){
    data <- data %>%
      mutate(dates_var = dates_var %>% 
               as.character() %>% 
               substring(1,10) %>% 
               as.Date())
  } 
  
  if(timeunit %in% "week"){
    data <- data %>%
      clean_week_variable("dates_var", "integer")
  }
  
  #### Complete
  # Makes a complete dataset, having all date-region pairs. In cases where a 
  # date-region pair was missing, the value is assigned as zero. 
  data <- data %>%
    as.data.frame() %>% # only works on data.frames, not tibbles
    tidyr::complete(region_var, dates_var,
                    fill = list(values_var = NA))
  
  ##### 2. Outliers and Missings ##### -----------------------------------------
  # TODO: parameterize outlier thresholds.
  
  #### Sort by Date
  # Need to sort by date as when replacing outliers and zero observations,
  # we take the previous and next value by date.
  data <- data %>%
    arrange(dates_var)
  
  if(clean_outliers){
    
    #### 2.1 Replace Outliers - - - - - - - - - - - - - - - - - - - - - - - - - 
    # Replace outliers if the outlier is less than 4 standard deviations below
    # the regional mean. For outliers, we take the average value of the lead and 
    # lag value.
    
    ## Treat 0s as NAs. Don't consider 0s when determining mean/sd for 
    ## determining outliers
    data$values_var[data$values_var %in% 0] <- NA
    
    ## Create variables to identify outliers and for interpolation
    data <- data %>%
      group_by(region_var) %>%
      mutate(value_region_mean = values_var %>% mean(na.rm=T)) %>%
      mutate(value_region_sd = values_var %>% sd(na.rm=T)) %>%
      mutate(value_lead = lead(values_var),
             value_lag = lag(values_var)) %>%
      rowwise() %>%
      mutate(value_leadlag_avg = mean(c(value_lead, value_lag), na.rm=T)) %>%
      as.data.frame()
    
    ## Make NAs Zeros. 
    # Need for replacing 0 values. If 0 is an outlier, then we'll replace.
    # In addition, if leadlag_avg is NA, make 0 to ensure all resulting
    # values are not NA.
    data$values_var[is.na(data$values_var)] <- 0
    data$value_leadlag_avg[is.na(data$value_leadlag_avg)] <- 0
    
    ## Scale
    data$value_scale <- (data$values_var - data$value_region_mean) / data$value_region_sd
    
    outliers_to_replace <- (data$value_scale < -4) %in% T
    data$values_var[outliers_to_replace] <-
      data$value_leadlag_avg[outliers_to_replace]
    
    #### 2.2 Replace Zeros - - - - - - - - - - - - - - - - - - - - - - - - - - - 
    # Replace observations of "0" in cases where the region has 3 or less zeros.
    # In cases where there are more than 3 zeros, these zeros are likely real. 
    # Here, we catch zeros where there aren't many in the datset but are not
    # identified as outliers.
    
    data <- data %>%
      group_by(region_var) %>%
      mutate(N_zeros = sum(values_var == 0)) %>%
      mutate(value_lead = lead(values_var),
             value_lag = lag(values_var)) %>%
      rowwise() %>%
      mutate(value_leadlag_avg = mean(c(value_lead, value_lag), na.rm=T)) %>%
      as.data.frame()
    
    zeros_to_replace <- (data$values_var %in% 0) & (data$N_zeros <= 3)
    data$values_var[zeros_to_replace] <- data$value_leadlag_avg[zeros_to_replace]
  }
  
  ##### 3. Daily --> Weekly ##### ----------------------------------------------
  # In some cases we just have a daily dataset. To compute weekly, we aggregate
  # here. Only relevant for O-D pairs with a day dataset but not a week dataset
  
  if(agg_od_day_to_wk){
    
    ## Convert date variable from day to week
    data <- data %>%
      clean_week_variable("dates_var", "date")
    
    ## Remove non-complete weeks. Only have data until March 31
    data <- data[!(data$dates_var  %in% "Mar 28 - Apr 03"),]
    
    ## Aggregate to weekly level
    data <- data %>%
      group_by(dates_var, region_var, region_origin, region_dest) %>%
      summarise(values_var = sum(values_var)) %>%
      arrange(dates_var)
    
    ## Change time unit to weekly. This matters when making label name
    timeunit <- "week"
  }
  
  ##### 4. Add Z-score and Percent Change ##### --------------------------------
  # Prepare: (1) Z-Score since Feburary baseline
  #          (2) % Change since Feburary baseline
  #          (3) % Change since day before
  #          (4) Lagged value from day before
  
  ## Create variables needed for identifying baseline. For day level, baseline
  # is day of week within Feburary. For weekly level, baseline is just Feb - 
  # but create a "dummy" day of week variable so code works for either case.
  if(timeunit %in% "day" & agg_od_day_to_wk %in% F){
    data$dow <- data$dates_var %>% wday() 
    data$month <- data$dates_var %>% month() 
  } else{
    data$dow <- 1
    
    data$month <- NA
    data$month[grepl("^Feb", data$dates_var)] <- 2
    data$month[grepl("^Mar", data$dates_var)] <- 3
  }
  
  ## Create variables of baseline values
  data <- data %>%
    group_by(region_var, dow) %>%
    mutate(values_var_dow_feb_mean = mean(values_var[month == 2], na.rm=T),
           values_var_dow_feb_sd = sd(values_var[month == 2], na.rm=T)) %>%
    ungroup() 
  
  ## Percent Change
  # If baseline value is NA or less than 15, make 15 
  # TODO: Parameterize this. Lower value could differ across datasets, or
  #       generally how we want to handle NA/missing could differ
  data$values_var_dow_feb_mean[is.na(data$values_var_dow_feb_mean)] <- 0
  data$values_var_dow_feb_mean[data$values_var_dow_feb_mean < 15] <- 15
  data <- data %>%
    mutate(value_perchange_feb = (values_var - values_var_dow_feb_mean)/(values_var_dow_feb_mean) * 100)
  
  ## Z-Score
  # For standard deviations that are zero, replace with minimum standard
  # deviation that isn't zero.
  min_sd <- min(data$values_var_dow_feb_sd[data$values_var_dow_feb_sd > 0], na.rm=T)
  data$values_var_dow_feb_sd[data$values_var_dow_feb_sd < min_sd] <- min_sd
  data$value_zscore_feb <- (data$values_var - data$values_var_dow_feb_mean) / data$values_var_dow_feb_sd
  
  #### Add Change, Percent Change and lagged value from previous time period
  data <- data %>%
    arrange(dates_var) %>%
    group_by(region_var) %>%
    
    mutate(value_count_lag = lag(values_var)) %>%
    mutate(value_count_chng = (values_var - value_count_lag)) %>%
    mutate(value_perchng = (values_var - value_count_lag)/value_count_lag*100) %>%
    
    ungroup()
  
  ##### 5. Clean up output dataset ##### ---------------------------------------
  # Clean up output variable names and remove temp variables created along 
  # the way
  
  #### Remove original variable names
  # This ensures variables are consistent across datasets. We keep the 
  # standardized variable names and remove the original variable names
  data[[values_var]] <- NULL
  data[[dates_var]] <- NULL
  data[[region_var]] <- NULL
  
  #### Replace generic varibles with cleaner names
  # In the above code, we use generic variable names like "values_var". We make
  # the variable name a bit weird (including "_var") to ensure that name
  # doesn't conflict with a name already in the dataset.
  data[["value_count"]] <- data$values_var
  data[["date"]] <- data$dates_var
  data[["region"]] <- data$region_var
  data[["value_count_baseline"]] <- data$values_var_dow_feb_mean
  
  #### Remove generic/long variables
  data$values_var <- NULL
  data$dates_var  <- NULL
  data$region_var <- NULL
  
  #### Remove temp/added variables
  data$value_scale <- NULL
  data$value_lead <- NULL
  data$value_leadlag_avg <- NULL
  data$N_zeros <- NULL
  
  ##### 6. Add labels and additional variables ##### ---------------------------
  
  ## Determine whether current value is an increase or decrease compared to
  ## (1) previous value and (2) basline value
  data <- data %>%
    mutate(inc_dec = ifelse( ((value_count < value_count_lag) %in% T), "decrease", "increase"),
           inc_dec_base = ifelse( ((value_count < value_count_baseline) %in% T), "decrease", "increase"))
  
  #### Add Variables
  # We add relevant variables from the ward shapefile and add a label variable.
  # This is done separately for non-od and od datasets. The merge is different
  # and the label is different in these cases.
  if(type %in% "non-od"){
    
    ## Merge in data from admin level shapefile
    data <- merge(data, admin_sp@data, by="region")
    
    ## Make labels
    # Count label: used for raw values
    # Change label: used when viewing percent change and z-score
    data$label_count <- make_count_label(data,
                                         timeunit,
                                         OD = F)
    
    data$label_change <- make_change_label(data,
                                           timeunit,
                                           OD = F)
    
    data$inc_dec <- NULL
    
  }
  
  if(type %in% "od"){
    data <- merge(data, admin_sp@data, by.x="region_origin", by.y="region") %>% 
      dplyr::rename(name_origin = name,
                    province_origin = province)
    data <- merge(data, admin_sp@data, by.x="region_dest", by.y="region") %>% 
      dplyr::rename(name_dest = name,
                    province_dest = province)
    
    data$label_count <- make_count_label(data,
                                         timeunit,
                                         OD = T)
    
    data$label_change <- make_change_label(data,
                                           timeunit,
                                           OD = T)
    
    # In O-D datset, we already have a "region_origin" and "region_dest" variable.
    # The "region" variable pastes these together and is not needed afterwords
    data$region <- NULL
    data$inc_dec <- NULL
    
  }
  
  return(data)
}

# *** CLEAN SPATIAL DATA =======================================================
# 1. Clean Outliers
# 2. Make variable names consistent across dataset to faciliate dashboard.
#    name = ward_name/district_name
#    region
#    province

# Prep Ward Data ---------------------------------------------------------------
# To allow different unit na

#### Load and Clean Wards Spatial Data
ward_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "ward_aggregated.Rds"))

#### Standardize Variable Names
# Make consistent between different unit types
ward_sp@data <- ward_sp@data %>%
  dplyr::rename(name = ward_name,
                province = ADM1_EN)

saveRDS(ward_sp, file.path(CLEAN_DATA_ADM3_PATH, "ward_aggregated_clean.Rds"))

#### Create spatial layer with just region name
ward_sp_regionname <- ward_sp
ward_sp_regionname@data <- ward_sp_regionname@data %>%
  dplyr::select(region, name, province) %>%
  dplyr::rename(name_id = name)

saveRDS(ward_sp_regionname, file.path(CLEAN_DATA_ADM3_PATH, "ward_aggregated_justregionname.Rds"))

# Prep District Data -----------------------------------------------------------
# To allow different unit na

#### Load and Clean Wards Spatial Data
district_sp <- readOGR(dsn = file.path(GEO_ADM2_PATH),
                       layer = "ZWE_adm2")

#### Standardize Variable Names
# Make consistent between different unit types
district_sp@data <- district_sp@data %>%
  dplyr::rename(name = NAME_2,
                region = ID_2,
                province = NAME_1) 
district_sp$area_km <- geosphere::areaPolygon(district_sp) / 1000^2

saveRDS(district_sp, file.path(CLEAN_DATA_ADM2_PATH, "district.Rds"))

#### Create spatial layer with just region name
district_sp_regionname <- district_sp
district_sp_regionname@data <- district_sp_regionname@data %>%
  dplyr::select(region, name, province) %>%
  dplyr::rename(name_id = name)

saveRDS(district_sp_regionname, file.path(CLEAN_DATA_ADM2_PATH, "district_justregionname.Rds"))

# *** CLEAN DATA ===============================================================
for(unit in c("district", "ward")){
  
  print(paste(unit, "--------------------------------------------------------"))
  
  #### Set parameters
  if(unit %in% "district"){
    CUSTOM_DATA_PATH <- CUSTOM_DATA_ADM2_PATH
    RAW_DATA_PATH    <- RAW_DATA_ADM2_PATH
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    admin_sp <- district_sp
  }
  
  if(unit %in% "ward"){
    CUSTOM_DATA_PATH <- CUSTOM_DATA_ADM3_PATH
    RAW_DATA_PATH    <- RAW_DATA_ADM3_PATH
    CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    admin_sp <- ward_sp
  }
  
  # *** NON-OD ===================================================================
  
  ##### UNIQUE SUBSCRIBERS #####
  
  # Unique Subscribers: Daily ----------------------------------------------------
  if(CLEAN_UNIQUE_SUBSCRIBERS){
    
    df <- read.csv(file.path(RAW_DATA_PATH, 
                             "count_unique_subscribers_per_region_per_day.csv"), 
                   stringsAsFactors=F)
    df_clean <- clean_data(df, "subscriber_count", "visit_date", "region", "non-od", "day", F, admin_sp)
    
    df_clean$label_count <- df_clean$label_count %>% str_replace_all("value", "subscribers")
    df_clean$label_count <- paste0(df_clean$label_count, 
                                   "<br>", 
                                   "Area (Km^2): ", 
                                   df_clean$area_km %>% round(2))
    
    df_clean$label_change <- df_clean$label_change %>% str_replace_all("value", "subscribers")
    df_clean$label_change <- paste0(df_clean$label_change, 
                                    "<br>", 
                                    "Area (Km^2): ", 
                                    df_clean$area_km %>% round(2))
    
    df_clean$density <- df_clean$value_count / df_clean$area_km
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH,
                                  "count_unique_subscribers_per_region_per_day.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "count_unique_subscribers_per_region_per_day.csv"), row.names=F)
    }
    
    # Unique Subscribers: Weekly ---------------------------------------------------
    
    
    df <- read.csv(file.path(RAW_DATA_PATH, 
                             "count_unique_subscribers_per_region_per_week.csv"), 
                   stringsAsFactors=F)
    df_clean <- clean_data(df, "subscriber_count", "visit_week", "region", "non-od", "week", F, admin_sp)
    
    df_clean$label_count <- df_clean$label_count %>% str_replace_all("value", "subscribers")
    df_clean$label_count <- paste0(df_clean$label_count, 
                                   "<br>", 
                                   "Area (Km^2): ", 
                                   df_clean$area_km %>% round(2))
    
    df_clean$label_change <- df_clean$label_change %>% str_replace_all("value", "subscribers")
    df_clean$label_change <- paste0(df_clean$label_change, 
                                    "<br>", 
                                    "Area (Km^2): ", 
                                    df_clean$area_km %>% round(2))
    
    df_clean$density <- df_clean$value_count / df_clean$area_km
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH,
                                  "count_unique_subscribers_per_region_per_week.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "count_unique_subscribers_per_region_per_week.csv"), row.names=F)
    }
    
  }
  
  ##### UNIQUE RESIDENTS #####
  if(CLEAN_UNIQUE_RESIDENTS){
    
    # Unique Residents: Daily ----------------------------------------------------
    df <- read.csv(file.path(RAW_DATA_PATH, 
                             "count_unique_active_residents_per_region_per_day.csv"), 
                   stringsAsFactors=F)
    df_clean <- clean_data(df, "subscriber_count", "visit_date", "region", "non-od", "day", F, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH,
                                  "count_unique_active_residents_per_region_per_day.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "count_unique_active_residents_per_region_per_day.csv"), row.names=F)
    }
    # Unique Residents: Weekly ---------------------------------------------------
    df <- read.csv(file.path(RAW_DATA_PATH, 
                             "count_unique_active_residents_per_region_per_week.csv"), 
                   stringsAsFactors=F)
    df_clean <- clean_data(df, "subscriber_count", "visit_week", "region", "non-od", "week", F, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH,
                                  "count_unique_active_residents_per_region_per_week.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "count_unique_active_residents_per_region_per_week.csv"), row.names=F)
    }
    
  }
  
  
  ##### MEDIAN DISTANCE TRAVELED #####
  if(CLEAN_MEDIAN_DISTANCE_TRAVELED){
    
    
    # Median Distance: Daily ----------------------------------------------------
    df <- read.csv(file.path(CUSTOM_DATA_PATH, 
                             "median_distance_per_day.csv"), 
                   stringsAsFactors=F)
    
    df_clean <- clean_data(df, "median_distance", "day", "home_region", "non-od", "day", F, admin_sp, F)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH,
                                  "median_distance_per_day.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "median_distance_per_day.csv"), row.names=F)
    }
    
    # Median Distance: Weekly ---------------------------------------------------
    if(unit %in% "ward"){
      # No weekly data for ward so collapse. Need to average so just to beforehand
      df <- read.csv(file.path(CUSTOM_DATA_PATH, 
                               "median_distance_per_day.csv"), 
                     stringsAsFactors=F) %>%
        
        # Standardize week variable as numeric for data cleaning function
        dplyr::mutate(day = day %>% week() + 1) %>%
        group_by(day, home_region) %>%
        dplyr::summarise(median_distance = mean(median_distance))
      
      df_clean <- clean_data(df, "median_distance", "day", "home_region", "non-od", "week", F, admin_sp, F)
      
      if(EXPORT){
        saveRDS(df_clean, file.path(CLEAN_DATA_PATH,
                                    "median_distance_per_week.Rds"))
        write.csv(df_clean, file.path(CLEAN_DATA_PATH, "median_distance_per_week.csv"), row.names=F)
      }
      
      
    } else{
      df <- read.csv(file.path(CUSTOM_DATA_PATH, 
                               "median_distance_per_week.csv"), 
                     stringsAsFactors=F) %>%
        
        # Standardize week variable as numeric for data cleaning function
        dplyr::mutate(week = week %>% week() + 1)
      df_clean <- clean_data(df, "median_distance", "week", "home_region", "non-od", "week", F, admin_sp)
      
      if(EXPORT){
        saveRDS(df_clean, file.path(CLEAN_DATA_PATH,
                                    "median_distance_per_week.Rds"))
        write.csv(df_clean, file.path(CLEAN_DATA_PATH, "median_distance_per_week.csv"), row.names=F)
      }
    }
    
  }
  
  # *** OD =======================================================================
  # For O-D datasets, an additional cleaning step before the cleaning function
  # is standardizing the region names. Specifically, the region names should be
  # (1) region_origin, (2) region_dest, and (3) region -- which concatonates
  # the previous two variables.
  
  ##### DIRECTED REGIONAL PAIR CONNECTIONS #####
  if(CLEAN_DIR_REGIONAL_PAIR_CONNECTIONS){
    
    # Directed Region Pair Connections: Daily --------------------------------------
    df <- read.csv(file.path(RAW_DATA_PATH, "directed_regional_pair_connections_per_day.csv"), stringsAsFactors=F)
    df <- df %>%
      dplyr::rename(region_origin = region_from,
                    region_dest = region_to) %>%
      mutate(region = paste(region_origin, region_dest)) 
    df_clean <- clean_data(df, "subscriber_count", "connection_date", "region", "od", "day", F, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH, "directed_regional_pair_connections_per_day.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "directed_regional_pair_connections_per_day.csv"), row.names=F)
    }
    
    # Directed Region Pair Connections: Weekly -------------------------------------
    df <- read.csv(file.path(RAW_DATA_PATH, "directed_regional_pair_connections_per_day.csv"), stringsAsFactors=F)
    df <- df %>%
      dplyr::rename(region_origin = region_from,
                    region_dest = region_to) %>%
      mutate(region = paste(region_origin, region_dest)) 
    df_clean <- clean_data(df, "subscriber_count", "connection_date", "region", "od", "day", T, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH, "directed_regional_pair_connections_per_week.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "directed_regional_pair_connections_per_week.csv"), row.names=F)
    }
    
  }
  
  ##### O-D CONNECTION MATRIX #####
  if(CLEAN_OD_CONNECTION_MATRIX){
    
    # O-D Connection: Daily --------------------------------------
    df <- read.csv(file.path(CUSTOM_DATA_PATH, "origin_destination_connection_matrix_per_day.csv"), stringsAsFactors=F)
    df <- df %>%
      dplyr::rename(region_origin = region_from,
                    region_dest = region_to) %>%
      mutate(region = paste(region_origin, region_dest)) 
    df_clean <- clean_data(df, "total_count", "connection_date", "region", "od", "day", F, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_per_day.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_per_day.csv"), row.names=F)
    }
    
    # O-D Connection: Weekly -------------------------------------
    df <- read.csv(file.path(CUSTOM_DATA_PATH, "origin_destination_connection_matrix_per_day.csv"), stringsAsFactors=F)
    df <- df %>%
      dplyr::rename(region_origin = region_from,
                    region_dest = region_to) %>%
      mutate(region = paste(region_origin, region_dest)) 
    df_clean <- clean_data(df, "total_count", "connection_date", "region", "od", "day", T, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_per_week.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_per_week.csv"), row.names=F)
    }
    
  }
  
  ##### NET DAILY MOVEMENT #####
  if(CLEAN_NET_DAILY_MOVEMENT){
    
    # Net Movement: Daily --------------------------------------------------------
    df <- read.csv(file.path(CUSTOM_DATA_PATH, "origin_destination_connection_matrix_per_day.csv"), stringsAsFactors=F)
    
    df_from <- df %>%
      group_by(connection_date, region_from) %>%
      dplyr::summarise(total_count_from = sum(total_count)) %>%
      dplyr::rename(region = region_from)
    
    df_to <- df %>%
      group_by(connection_date, region_to) %>%
      dplyr::summarise(total_count_to = sum(total_count))  %>%
      dplyr::rename(region = region_to)
    
    df <- merge(df_from, df_to, by = c("connection_date", "region")) %>%
      mutate(total_count_net = total_count_to - total_count_from)
    
    df_clean <- clean_data(df, "total_count_net", "connection_date", "region", "non-od", "day", F, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_net_per_day.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_net_per_day.csv"), row.names=F)
    }
    
    # Net Movement: Weekly --------------------------------------------------------
    df <- read.csv(file.path(CUSTOM_DATA_PATH, "origin_destination_connection_matrix_per_day.csv"), stringsAsFactors=F)
    
    df <- df %>%
      mutate(connection_date = connection_date %>% substring(1,10) %>% as.Date() %>% week())
    df$connection_date <- df$connection_date + 1 # TODO: Check. python starts 0, r 1 issue?
    
    df_from <- df %>%
      group_by(connection_date, region_from) %>%
      dplyr::summarise(total_count_from = sum(total_count)) %>%
      dplyr::rename(region = region_from)
    
    df_to <- df %>%
      group_by(connection_date, region_to) %>%
      dplyr::summarise(total_count_to = sum(total_count))  %>%
      dplyr::rename(region = region_to)
    
    df <- merge(df_from, df_to, by = c("connection_date", "region")) %>%
      mutate(total_count_net = total_count_to - total_count_from)
    
    df_clean <- clean_data(df, "total_count_net", "connection_date", "region", "non-od", "week", F, admin_sp)
    
    if(EXPORT){
      saveRDS(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_net_per_week.Rds"))
      write.csv(df_clean, file.path(CLEAN_DATA_PATH, "origin_destination_connection_matrix_net_per_week.csv"), row.names=F)
    }
    
  }
  
}

