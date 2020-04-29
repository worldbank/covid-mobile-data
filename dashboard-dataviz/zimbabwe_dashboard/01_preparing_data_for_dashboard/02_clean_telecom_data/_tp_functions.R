# Telecom Data Prep [tp] Functions

# Creates functions to standardize the process of cleaning and standardizing
# telecom data.

# For many functions, printing "Done: [function name]" to help identify wheich
# functions take a long time and where more effort should be spent making them 
# faster.

# TODO: (1) Functions designed so that any variable name inputs will work; however,
#           some (eg, complete) return dataframe always with standardized
#           var names. Fix so output the origin names entered in to make this
#           more general.
#       (2) Where possible, use data.table methods - particularly when use
#           group_by - to help make faster. 
#               --complete?
#               --interpolation and replace zero scripts

tp_standardize_vars <- function(data,
                                date_var,
                                region_var,
                                value_var){
  
  # DESCRIPTION
  # Standardizes original variable names to names used in shiny dashboard
  
  data <- data %>%
    dplyr::rename("date" = date_var,
                  "region" = region_var,
                  "value" = value_var) %>%
    dplyr::mutate(date = date %>% as.character(),
                  region = region %>% as.character(),
                  value = value %>% as.numeric())
  
  return(data)
}

tp_standardize_vars_od <- function(data,
                                   date_var,
                                   region_origin_var,
                                   region_dest_var,
                                   value_var){
  
  # DESCRIPTION
  # Standardizes original variable names to names used in shiny dashboard
  
  data <- data %>%
    dplyr::rename("date" = date_var,
                  "region_origin" = region_origin_var,
                  "region_dest" = region_dest_var,
                  "value" = value_var) %>%
    dplyr::mutate(date = date %>% as.character(),
                  region_origin = region_origin %>% as.character(),
                  region_dest = region_dest %>% as.character(),
                  value = value %>% as.numeric(),
                  region = paste(region_origin, region_dest))
  
  return(data)
}

tp_complete_date_region <- function(data,
                                    date_var = "date",
                                    region_var = "region",
                                    value_var = "value"){
  
  data <- data.frame(
    date = data[date_var],
    region = data[region_var],
    value = data[value_var]
  )
  
  data <- data %>%
    as.data.frame() %>%
    tidyr::complete(region, date) 
  
  return(data)
}

tp_complete_date_region_od <- function(data,
                                       date_var = "date",
                                       region_var = "region",
                                       region_origin_var = "region_origin",
                                       region_dest_var = "region_dest",
                                       value_var = "value"){
  
  
  data <- data.frame(
    date = data[date_var],
    region = data[region_var],
    region_origin = data[region_origin_var],
    region_dest = data[region_dest_var],
    value = data[value_var]
  )
  
  data <- data %>%
    as.data.frame() %>%
    tidyr::complete(nesting(region, region_origin, region_dest), date) 
  
  # https://stackoverflow.com/questions/43483497/data-table-equivalent-of-tidyrcomplete
  #res = setDT(data)[
  #  CJ(region = region, date = date, unique=TRUE), 
  #  on=.(region, date)
  #  ]
  
  print("Done: tp_complete_date_region_od")
  
  return(data)
}


tp_add_polygon_data <- function(data,
                                sp_data,
                                by = "region"){
  
  # In case enteres spatialpolygonsdataframe, grab data
  sp_data <- sp_data %>% as.data.frame()
  
  data <- data %>%
    left_join(sp_data, by=by) 
  
  data <- data[!is.na(data$name),]
  
  return(data)
}

tp_add_polygon_data_od <- function(data,
                                   sp_data,
                                   by = "region"){
  
  # TODO: Currently ignores "by" and assumes region, region_origin, region_dest
  
  # In case enteres spatialpolygonsdataframe, grab data
  sp_data <- sp_data %>% as.data.frame()
  
  # data.table for speed
  sp_data_origin <- sp_data %>% as.data.table()
  sp_data_dest <- sp_data %>% as.data.table()
  data <- data %>% as.data.table()
  
  # Add _origin and _destination to all variables
  names(sp_data_origin) <- paste0(names(sp_data_origin), "_origin")
  names(sp_data_dest)   <- paste0(names(sp_data_dest), "_dest")
  
  # Merge - data.table method
  data <- merge(data, sp_data_origin, by = "region_origin")
  data <- merge(data, sp_data_dest, by = "region_dest")
  
  #data <- data %>%
  #  left_join(sp_data_origin, by="region_origin")  %>%
  #  left_join(sp_data_dest, by="region_dest") 
  
  ## Clean up - exclude areas not in polygon
  data <- data[!is.na(data$name_origin),]
  data <- data[!is.na(data$name_dest),]
  
  data <- as.data.frame(data)
  
  print("Done: tp_add_polygon_data_od")
  
  return(data)
}

tp_fill_regions <- function(data, 
                            admin_sp,
                            unit_var = "region",
                            date_var = "date"){
  
  # DESCRIPTION
  # Some datasets to not contain all the regions found in the polygon - some
  # wards or districts, for example, may be missing. This function adds 
  # missing regions to the dataframe. It only adds for one time period; the
  # "complete" function should then be used after to expand the region across
  # time.
  
  # Args
  # data:     dataframe of tele data
  # admin_sp: polygon layer at same level as dataframe
  # unit_var: unit variable (e.g., region) common between datasets. Does not
  #           support different variable names across datasets.
  # date_var: When adding additional rows to data, all variables are NA
  #           except for date, where we fill in one date
  
  #### Prep data
  data$unit_var_TEMP     <- data[[unit_var]]
  admin_sp$unit_var_TEMP <- admin_sp[[unit_var]]
  
  date_example <- data[[date_var]][1]
  
  #### Dataframe with new regions
  new_units_df <- setdiff(admin_sp$unit_var_TEMP, 
                          data$unit_var_TEMP) %>%
    as.data.frame() %>%
    dplyr::mutate(date = date_example)
  
  names(new_units_df) <- c(unit_var, date_var)
  
  data <- bind_rows(data, new_units_df)
  data$unit_var_TEMP <- NULL
  
  print("Done: tp_fill_regions")
  return(data)
}

tp_clean_week <- function(data, 
                          newvar_name = "date",
                          date_var = "date"){
  
  # DESCRIPTION
  # Function to clean week variable into text format seen by user in Shiny
  # server (e.g., into Feb 01 - Feb 07).
  
  #### To Character
  # Inputs could be numeric, factor or date -- so for consistency convert to 
  # character as the output will be character.
  date_var <- data[[date_var]] %>% as.character()
  
  #### Determine Type 
  date_example <- date_var[!is.na(date_var)][1]
  
  if(grepl("^20[[:digit:]][[:digit:]]", date_example)){
    type <- "date"
  } else{
    type <- "integer"
  }
  
  #### Modify variable
  # TODO Extend to full year
  if(type == "integer"){
    
    date_var <- date_var %>%
      dplyr::recode("6" = "Feb 01 - Feb 07",
                    "7" = "Feb 08 - Feb 14",
                    "8" = "Feb 15 - Feb 21",
                    "9" = "Feb 22 - Feb 28",
                    "10" = "Feb 29 - Mar 06",
                    "11" = "Mar 07 - Mar 13",
                    "12" = "Mar 14 - Mar 20",
                    "13" = "Mar 21 - Mar 27",
                    "14" = "Mar 28 - Apr 03")
    
  } else if (type == "date"){
    # TODO Not idea as assumes starts on Feb 1
    
    date_var_ORIG <- date_var %>% substring(1,10) %>% as.character()
    
    date_var[date_var_ORIG >= "2020-02-01"] <- "Feb 01 - Feb 07"
    date_var[date_var_ORIG >= "2020-02-08"] <- "Feb 08 - Feb 14"
    date_var[date_var_ORIG >= "2020-02-15"] <- "Feb 15 - Feb 21"
    date_var[date_var_ORIG >= "2020-02-22"] <- "Feb 22 - Feb 28"
    date_var[date_var_ORIG >= "2020-02-29"] <- "Feb 29 - Mar 06"
    date_var[date_var_ORIG >= "2020-03-07"] <- "Mar 07 - Mar 13"
    date_var[date_var_ORIG >= "2020-03-14"] <- "Mar 14 - Mar 20"
    date_var[date_var_ORIG >= "2020-03-21"] <- "Mar 21 - Mar 27"
    date_var[date_var_ORIG >= "2020-03-28"] <- "Mar 28 - Apr 03"
    
  }
  
  #### Convert to factor
  # TODO workd fine for now because march comes after feb in alphabet so
  # in correctly ordered. But to make more general need to explicity order.
  # Integer and dates will order, so could keep a copy of those to order
  # new variable by that.
  
  data[[newvar_name]] <- date_var
  
  print("Done: tp_clean_week")
  return(data)
}

tp_agg_day_to_week <- function(data,
                               value_var = "value", 
                               region_var = "region", 
                               date_var = "date",
                               fun = "sum"){
  
  data <- data.frame(value = data[[value_var]],
                     region = data[[region_var]],
                     date = data[[date_var]])
  
  if(fun %in% "sum"){
    data <- data %>%
      dplyr::group_by(region, date) %>%
      dplyr::summarise(value = sum(value, na.rm=T))
  }
  
  if(fun %in% "mean"){
    data <- data %>%
      dplyr::group_by(region, date) %>%
      dplyr::summarise(value = mean(value, na.rm=T))
  }
  
  print("Done: tp_agg_day_to_week")
  
  return(data)
}

tp_agg_day_to_week_od <- function(data,
                                  value_var = "value", 
                                  region_var = "region", 
                                  region_origin_var = "region_origin", 
                                  region_dest_var = "region_dest", 
                                  date_var = "date",
                                  fun = "sum"){
  
  data <- data.frame(value = data[[value_var]],
                     region = data[[region_var]],
                     region_origin = data[[region_origin_var]],
                     region_dest = data[[region_dest_var]],
                     date = data[[date_var]])
  
  data <- data %>% as.data.table()
  
  if(fun %in% "sum"){
    
    data <- data[, .(value = sum(value, na.rm=T)), 
                                             by = list(region, 
                                                       region_origin,
                                                       region_dest,
                                                       date)]
    
  }
  
  if(fun %in% "mean"){
    data <- data[, .(value = mean(value, na.rm=T)), 
                 by = list(region, 
                           region_origin,
                           region_dest,
                           date)]
  }
  
  data <- data %>% as.data.frame()
  
  print("Done: tp_agg_day_to_week_od")
  
  return(data)
}

tp_clean_date <- function(data, 
                          newvar_name = "date",
                          date_var = "date"){
  
  # DESCRIPTION
  # The date variable tends to come in the same format to be cleaned in the 
  # same way.
  
  data[[newvar_name]] <- data[[date_var]] %>% 
    as.character() %>%
    substring(1,10) %>% 
    as.Date()
  
  print("Done: tp_clean_date")
  
  return(data)
}

tp_add_label_baseline <- function(data, 
                                  newvar_name = "label_base",
                                  value_var = "value", 
                                  region_var = "region", 
                                  date_var = "date", 
                                  value_base_var = "value_base",
                                  perchange_base_var = "value_perchange_base",
                                  zscore_base_var = "value_zscore_base",
                                  name_var = "name",
                                  name_origin_var = "name_origin",
                                  name_dest_var = "name_dest",
                                  timeunit,
                                  OD){
  
  #### Name
  if(OD){
    label_name <- paste0(data[[name_origin_var]], 
                         " to ", 
                         data[[name_dest_var]])
  } else{
    label_name <- data[[name_var]]
  }
  
  #### Current Value
  label_value <- paste0("This ", timeunit, "'s value: ",
                        data[[value_var]], ".")
  
  label_value <- ifelse(is.na(data[[value_var]]),
                        "15 or fewer<br>or information not available",
                        label_value)
  
  #### Baseline change value
  inc_dec <- ifelse(((data[[value_var]] > data[[value_base_var]]) %in% T),
                    "increase",
                    "decrease")
  
  label_base <- paste0("The baseline February value was: ",
                       data[[value_base_var]], ".<br>",
                       "Compared to baseline, this ", timeunit, "<br>",
                       "had a ", 
                       data[[perchange_base_var]] %>% round(2) %>% abs(),
                       "% ",
                       inc_dec,
                       " (a ",
                       data[[zscore_base_var]] %>% round(2), 
                       " z-score).")
  
  # In some instances, remove
  # next step should catch
  #label_base <- ifelse(data$date == min(data$date),
  #                     "",
  #                     label_base)
  
  label_base <- ifelse(is.na(data[[value_var]]) | is.na(data[[value_base_var]]),
                       "",
                       label_base)
  
  # Construct final label
  label <- 
    paste(label_name,
          label_value,
          label_base,
          sep = "<br>") %>%
    str_replace_all("<br><br>", "<br>")%>%
    str_replace_all("<br>$", "")
  
  data[[newvar_name]] <- label
  
  print("Done: tp_add_label_baseline")
  
  return(data)
}

tp_add_label_level <- function(data, 
                               newvar_name = "label_level",
                               value_var = "value", 
                               region_var = "region", 
                               date_var = "date", 
                               value_lag_var = "value_lag", 
                               value_perchange_var = "value_perchange", 
                               name_var = "name",
                               name_origin_var = "name_origin",
                               name_dest_var = "name_dest",
                               timeunit,
                               OD){
  
  #### Make function dataframe
  data_orig <- data
  
  data <- data.frame(value  = data[[value_var]],
                     region = data[[region_var]],
                     date   = data[[date_var]],
                     value_lag   = data[[value_lag_var]],
                     value_perchange = data[[value_perchange_var]])
  
  if(OD){
    data$name_origin <- data_orig[[name_origin_var]]
    data$name_dest <- data_orig[[name_dest_var]]
  } else{
    data$name <- data_orig[[name_var]]
  }
  
  #### Name
  if(OD){
    label_name <- paste0(data$name_origin, " to ", data$name_dest)
  } else{
    label_name <- data$name
  }
  
  #### Value
  label_value <- paste0("This ", timeunit, "'s value: ",data$value, ".")
  
  label_value <- ifelse(is.na(data$value),
                        "15 or fewer<br>or information not available",
                        label_value)
  
  #### Change
  inc_dec <- ifelse((data$value_perchange >= 0) %in% T,
                    "increase",
                    "decrease") 
  
  label_change <- paste0("Last ", timeunit, "'s value: ", 
                         data$value_lag, ".", 
                         "<br>", 
                         "This ", timeunit, " had a ",
                         abs(data$value - data$value_lag), 
                         " ",
                         "(",
                         data$value_perchange %>% round(2) %>% abs(),
                         "%",
                         ") ",
                         inc_dec,
                         "<br>compared to the previous ", 
                         timeunit, ".")
  
  # next step should catch
  #label_change <- ifelse(data$date == min(data$date),
  #                     "",
  #                     label_change)
  
  label_change <- ifelse(is.na(data$value) | 
                           (data$value_perchange %in% Inf) | 
                           is.na(data$value_perchange),
                         "",
                         label_change)
  
  # Construct final label
  label <- 
    paste(label_name,
          label_value,
          label_change,
          sep = "<br>") %>%
    str_replace_all("<br><br>", "<br>")%>%
    str_replace_all("<br>$", "")
  
  data_orig[[newvar_name]] <- label
  
  print("Done: tp_add_label_level")
  
  return(data_orig)
  
}



tp_add_percent_change <- function(data,
                                  value_var = "value", 
                                  region_var = "region", 
                                  date_var = "date"){
  
  ### Order by date
  data <- data[order(data[[date_var]]),]
  
  ### Make function dataframe
  data_orig <- data
  data <- data.frame(value  = data[[value_var]],
                     region = data[[region_var]],
                     date   = data[[date_var]])
  
  ### Calculate percent change
  # data.table method for lag
  data <- data %>% as.data.table()
  data <- data[, value_lag := c(NA, value[-.N]), by=region]
  data <- data %>% as.data.frame()
  
  data <- data %>%
    mutate(value_perchange = (value - value_lag) / value_lag * 100)
  
  data$value_perchange[is.na(data$value_perchange) |
                         data$value_perchange %in% c(Inf, -Inf)] <- NA
  
  # data.table messes up order; use data.table to efficiently order both
  data      <- data.table(data)
  data_orig <- data.table(data_orig)
  
  data      <- setorder(data, region, date) %>% as.data.frame()
  data_orig <- setorder(data_orig, region, date) %>% as.data.frame()
  
  data_orig[[paste0(value_var, "_perchange")]] <- data$value_perchange
  data_orig[[paste0(value_var, "_lag")]] <- data$value_lag
  
  print("Done: tp_add_percent_change")
  
  return(data_orig)
}  

tp_interpolate_outliers <- function(data,
                                    newvar_name = "value",
                                    value_var = "value", 
                                    region_var = "region", 
                                    date_var = "date", 
                                    outlier_sd = 4,
                                    outlier_replace = "negative",
                                    NAs_as_zero = T,
                                    return_scale = F){
  
  # DESCRIPTION
  # Interpolates outliers using values from the previous and following day. 
  # Assumes sorted by day.
  
  # Args:
  # value: Vector of values to replace outliers for
  # region: Vector of region ids
  # date: Vector of dates
  # outlier_sd: Standard deviations from mean to determine outlier
  # outlier_replace: "negative", "positive", "both" - whether to interpolace
  # negative/positive outliers only, or to interpoalte both
  # NAs_as_zero: Whether to treat NAs as zero when checking for outliers. If
  # T, NAs with all be turned into 0 then replaced if is outlier. If F,
  # NAs will stay as NAs. 
  # If T, mean and standard deviation to determine outliers ignores NAs.
  # return_scale: returns standardized value. 
  
  # RETURNS
  # (if return_scale = F) vector of interpolated values
  # (if return_scale = T) list, first item contains interpolated values, second
  # item contains original scaled values
  
  # Create dataframe with variables needed for identifying and replacing outliers
  data <- data[order(data[[date_var]]),]
  
  data_orig <- data
  
  data <- data.frame(value = data[[value_var]], 
                     region = data[[region_var]], 
                     date = data[[date_var]]) %>%
    group_by(region) %>%
    mutate(value_region_mean = value %>% mean(na.rm=T)) %>%
    mutate(value_region_sd = value %>% sd(na.rm=T)) %>%
    mutate(value_lead = lead(value),
           value_lag = lag(value)) %>%
    rowwise() %>%
    mutate(value_leadlag_avg = mean(c(value_lead, value_lag), na.rm=T)) %>%
    as.data.frame()
  
  # Make NAs Zeros. 
  if(NAs_as_zero){
    data$value[is.na(data$value)] <- 0
    data$value_leadlag_avg[is.na(data$value_leadlag_avg)] <- 0
  }
  
  # Scale
  data <- data %>%
    mutate(value_scale = (value - value_region_mean) / value_region_sd)
  
  if (outlier_replace == "both"){
    outliers_to_replace <- (abs(data$value_scale) < outlier_sd) %in% T
  } else if (outlier_replace == "negative"){
    outliers_to_replace <- (data$value_scale < -outlier_sd) %in% T
  } else if (outlier_replace == "positive"){
    outliers_to_replace <- (data$value_scale > outlier_sd) %in% T
  }
  
  data$value[outliers_to_replace] <- data$value_leadlag_avg[outliers_to_replace]
  
  # Add data to dataframe
  data_orig[[newvar_name]] <- data$value
  
  if(return_scale){
    data_orig[[paste0(value_var, "_scale")]] <- data$value_scale
  } 
  
  print("Done: tp_interpolate_outliers")
  
  return(data_orig)
}

tp_replace_zeros <- function(data,
                             newvar_name = "value",
                             value_var = "value", 
                             region_var = "region", 
                             date_var = "date", 
                             NAs_as_zero = T,
                             N_zero_thresh = 3){
  
  # DESCRIPTION
  # Interpolates values of zero in cases where the number of zeros within a 
  # region is less than an equal to 'N_zero_thresh'. Assumes sorted by day.
  
  if(NAs_as_zero){
    NA_replace_value <- 0
  } else{
    NA_replace_value <- NA
  }
  
  data_orig <- data
  
  data <- data.frame(value = data[[value_var]], 
                     region = data[[region_var]], 
                     date = data[[date_var]]) %>%
    group_by(region) %>%
    mutate(value = replace_na(value, NA_replace_value)) %>%
    mutate(N_zeros = sum(value %in% 0)) %>%
    mutate(value_lead = lead(value),
           value_lag = lag(value)) %>%
    rowwise() %>%
    mutate(value_leadlag_avg = mean(c(value_lead, value_lag), na.rm=T)) %>%
    as.data.frame()
  
  zeros_to_replace <- (data$value %in% 0) & (data$N_zeros <= N_zero_thresh)
  data$value[zeros_to_replace] <- data$value_leadlag_avg[zeros_to_replace]
  
  data_orig[[newvar_name]] <- data$value
  
  return(data_orig)
}

tp_add_baseline_comp_stats <- function(data,
                                       value_var = "value",
                                       region_var = "region",
                                       date_var = "date",
                                       baseline_months = 2){
  
  # DESCRIPTION
  # Adds baseline values
  
  # TODO: Assumes baseline comes from same year, could generalize
  
  data <- data[order(data[[date_var]]),]
  
  #### Create dataframe for tranformations
  data_sub <- data.frame(value = data[[value_var]],
                         region = data[[region_var]],
                         date = data[[date_var]])
  
  #### Add Month and Day of Week
  # TODO This process might not be most stable
  if(grepl("^20[[:digit:]][[:digit:]]", data_sub$date[1]) ){
    
    ### Daily / In Date format
    data_sub <- data_sub %>%
      mutate(date  = date %>% as.Date(),
             dow   = wday(date),
             month = month(date))
    
  } else{
    ### Weekly / Text
    
    data_sub$dow <- 1 # make dummy week variable
    
    data_sub$month <- NA
    data_sub$month[grepl("^Jan", data_sub$date)] <- 1
    data_sub$month[grepl("^Feb", data_sub$date)] <- 2
    data_sub$month[grepl("^Mar", data_sub$date)] <- 3
    data_sub$month[grepl("^Apr", data_sub$date)] <- 4
    data_sub$month[grepl("^May", data_sub$date)] <- 5
    data_sub$month[grepl("^Jun", data_sub$date)] <- 6
    data_sub$month[grepl("^Jul", data_sub$date)] <- 7
    data_sub$month[grepl("^Aug", data_sub$date)] <- 8
    data_sub$month[grepl("^Sep", data_sub$date)] <- 9
    data_sub$month[grepl("^Oct", data_sub$date)] <- 10
    data_sub$month[grepl("^Nov", data_sub$date)] <- 11
    data_sub$month[grepl("^Dec", data_sub$date)] <- 12
  }
  
  #### Create variables of baseline values
  # Use data.table method for below code
  #data_sub <- data_sub %>%
  #  dplyr::group_by(region, dow) %>%
  #  dplyr::mutate(value_dow_base_mean = mean(value[month %in% baseline_months], na.rm=T),
  #                value_dow_base_sd   = sd(value[month %in% baseline_months], na.rm=T)) %>%
  #  dplyr::ungroup() 
  
  data_sub_dt <- as.data.table(data_sub)
  
  data_sub_base_dt <- data_sub_dt[(data_sub_dt$month %in% baseline_months),]
  data_sub_base_agg_dt <- data_sub_base_dt[, .(value_dow_base_mean = mean(value, na.rm=T),
                                               value_dow_base_sd   = sd(value, na.rm=T)), 
                                           by = list(region, dow)]
  
  data_sub_dt <- merge(data_sub_dt, data_sub_base_agg_dt, by=c("region", "dow"))
  
  data_sub <- as.data.frame(data_sub_dt)
  
  #### Percent Change
  # If baseline value is NA or less than 15, make 15 
  #data_sub$value_dow_base_mean[is.na(data_sub$value_dow_base_mean)] <- 0
  #data_sub$value_dow_base_mean[data_sub$value_dow_base_mean < 15] <- 15
  
  data_sub <- data_sub %>%
    mutate(value_perchange_base = (value - value_dow_base_mean)/(value_dow_base_mean) * 100)
  
  #### Z-Score
  # For standard deviations that are zero, replace with minimum standard
  # deviation that isn't zero. 
  min_sd <- min(data_sub$value_dow_base_sd[data_sub$value_dow_base_sd > 0], na.rm=T)
  data_sub$value_dow_base_sd[data_sub$value_dow_base_sd < min_sd] <- min_sd
  
  data_sub <- data_sub %>%
    mutate(value_zscore_base = (value - value_dow_base_mean) / value_dow_base_sd)
  
  #### Cleanup
  data_sub$value_dow_base_mean[is.na(data_sub$value_dow_base_mean) |
                                 data_sub$value_dow_base_mean %in% c(Inf, -Inf)] <- NA
  
  data_sub$value_perchange_base[is.na(data_sub$value_perchange_base) |
                                  data_sub$value_perchange_base %in% c(Inf, -Inf)] <- NA
  
  data_sub$value_zscore_base[is.na(data_sub$value_zscore_base) |
                               data_sub$value_zscore_base %in% c(Inf, -Inf)] <- NA
  
  # Remove baseline values and comparisson if month is baseline
  bmonth_replace <- function(var, base_month = baseline_months){
    ifelse(month == base_month,
           NA,
           var)
  }
  
  data_sub %<>% 
    mutate(value_dow_base_mean =  ifelse(month == baseline_months,
                                         NA,
                                         value_dow_base_mean),
           value_perchange_base =  ifelse(month == baseline_months,
                                          NA,
                                          value_perchange_base),
           value_zscore_base =  ifelse(month == baseline_months,
                                       NA,
                                       value_zscore_base) )
  
  
  #### Add variables to data
  
  # data.table messes up order; use data.table to efficiently order both
  data      <- data.table(data)
  data_sub  <- data.table(data_sub)
  
  data      <- setorder(data, region, date) %>% as.data.frame()
  data_sub  <- setorder(data_sub, region, date) %>% as.data.frame()
  
  data[[paste0(value_var, "_base")]] <- data_sub$value_dow_base_mean
  data[[paste0(value_var, "_perchange_base")]] <- data_sub$value_perchange_base
  data[[paste0(value_var, "_zscore_base")]] <- data_sub$value_zscore_base
  
  print("Done: tp_add_baseline_comp_stats")
  
  return(data)
}


tp_less15_NA <- function(data,
                         value_var = "value"){
  
  data[[value_var]][data[[value_var]] <= 15] <- NA
  
  print("Done: tp_less15_NA")
  
  return(data)
}


