# Clean Subscribers Data

# Depends on: clean_movement_inout_data.R

EXPORT <- T

unit <- "ward"
timeunit <- "day"
for(unit in c("district", "ward")){
  for(timeunit in c("day", "week")){
    
    print(paste(unit, timeunit, "--------------------------------------------"))
    
    # Set parameters -------------------------------------------------------------
    if(unit %in% "district"){
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
      admin_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
    }
    
    if(unit %in% "ward"){
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
      admin_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))
    }
    
    # Clean ----------------------------------------------------------------------
    df <- readRDS(file.path(CLEAN_DATA_PATH,
                            paste0("origin_destination_connection_matrix_per_",
                                   timeunit,
                                   ".Rds"))) %>%
      as.data.table()
    
    ## Aggregate Origin
    df_orign <- df[, .(value   = sum(value, na.rm=T)), 
                   by = list(region_origin, date)]   
    
    names(df_orign)[names(df_orign) %in% "region_origin"] <- "region"
    names(df_orign)[names(df_orign) %in% "value"] <- "value_origin"
    
    ## Aggregate Destination
    df_dest <- df[, .(value   = sum(value, na.rm=T)), 
                  by = list(region_dest, date)]   
    
    names(df_dest)[names(df_dest) %in% "region_dest"] <- "region"
    names(df_dest)[names(df_dest) %in% "value"] <- "value_dest"
    
    ## Merge
    df_day_clean <- merge(df_orign, df_dest, by=c("region", "date")) %>%
      as.data.frame()
    
    ## Prep data
    df_day_clean <- df_day_clean %>%
      
      dplyr::mutate(value = value_dest - value_origin) %>%
      
      tp_standardize_vars("date", "region", "value") %>%
      
      # Clean Data
      tp_fill_regions(admin_sp) %>%
      tp_complete_date_region() %>%
      tp_add_polygon_data(admin_sp) %>%
      
      # Percent change
      tp_add_baseline_comp_stats() %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = timeunit, OD = F) %>%
      tp_add_label_baseline(timeunit = timeunit, OD = F) 
    
    if(EXPORT){
      saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH,
                                      paste0("origin_destination_connection_matrix_net_per_",
                                             timeunit,
                                             ".Rds")))
      
      write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, 
                                        paste0("origin_destination_connection_matrix_net_per_",
                                               timeunit,
                                               ".csv")), 
                row.names=F)
    }
    
    
  }
}


