# Clean Subscribers Data

# Depends on: clean_movement_inout_data.R

unit <- "adm2"
timeunit <- "daily"
for(unit in c("adm2", "adm3")){
  for(timeunit in c("daily", "weekly")){
    
    print(paste(unit, timeunit, "--------------------------------------------"))
    
    # Set parameters -------------------------------------------------------------
    admin_sp <- readRDS(file.path(GEO_PATH, paste0(unit, ".Rds")))
    
    if(unit %in% "adm2"){
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM2_PATH
    }
    
    if(unit %in% "adm3"){
      CLEAN_DATA_PATH  <- CLEAN_DATA_ADM3_PATH
    }
    
    # Clean ----------------------------------------------------------------------
    df <- readRDS(file.path(CLEAN_DATA_PATH,
                            paste0("i5_",
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
      tp_add_baseline_comp_stats(file_name = file.path(CLEAN_DATA_PATH, 
                                                       paste0("i5_net_",timeunit,"_base.csv")),
                                 type = timeunit) %>%
      tp_add_percent_change() %>%
      
      # Add labels
      tp_add_label_level(timeunit = timeunit, OD = F) %>%
      tp_add_label_baseline(timeunit = timeunit, OD = F) 
    
    
    ## Export
    saveRDS(df_day_clean, file.path(CLEAN_DATA_PATH,
                                    paste0("i5_net_",
                                           timeunit,
                                           ".Rds")))
    
    write.csv(df_day_clean, file.path(CLEAN_DATA_PATH, 
                                      paste0("i5_net_",
                                             timeunit,
                                             ".csv")), 
              row.names=F)
    
    
    
  }
}


