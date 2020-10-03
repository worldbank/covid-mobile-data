# Transfer dashboard data from OneDrive to Github


## Remove previous files in github
REMOVE_PREVIOUS_FILES <- F

if(REMOVE_PREVIOUS_FILES){
  temp <- list.files(DASHBOARD_DATA_GITHUB_PATH, 
                     full.names = T, 
                     pattern = "*.Rds") %>%
    lapply(file.remove)
  
}


# Move telecom data to github folder -------------------------------------------
i <- 1

telecom_files <- list.files(DASHBOARD_DATA_ONEDRIVE_PATH, pattern = "*.Rds")

telecom_files <- telecom_files[grepl("spark", telecom_files)]

temp <- telecom_files %>%
  lapply(function(file_i){
    if((i %% 100) %in% 0) print(i)
    i <<- i + 1
    
    file.copy(file.path(DASHBOARD_DATA_ONEDRIVE_PATH, file_i),
              paste0(DASHBOARD_DATA_GITHUB_PATH, "/"),
              overwrite=T)
  })


# Move geofiles to github folder -----------------------------------------------
for(file_i in list.files(GEO_PATH)){
  file.copy(file.path(GEO_PATH, file_i),
            paste0(DASHBOARD_DATA_GITHUB_PATH, "/"),
            overwrite=T)
}


