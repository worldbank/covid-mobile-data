# Transfer dashboard data from OneDrive to Github


temp <- list.files(DASHBOARD_DATA_ONEDRIVE_PATH, pattern = "*.Rds") %>%
  lapply(function(file_i){
    
    file.copy(file.path(DASHBOARD_DATA_ONEDRIVE_PATH, file_i),
              paste0(DASHBOARD_DATA_GITHUB_PATH, "/"),
              overwrite=T)
  })

