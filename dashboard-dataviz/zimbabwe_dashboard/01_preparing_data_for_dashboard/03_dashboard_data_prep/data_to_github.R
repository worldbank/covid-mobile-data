# Transfer dashboard data from OneDrive to Github

i <- 1
temp <- list.files(DASHBOARD_DATA_ONEDRIVE_PATH, pattern = "*.Rds") %>%
  lapply(function(file_i){
    if((i %% 100) %in% 0) print(i)
    i <<- i + 1
    
    file.copy(file.path(DASHBOARD_DATA_ONEDRIVE_PATH, file_i),
              paste0(DASHBOARD_DATA_GITHUB_PATH, "/"),
              overwrite=T)
  })

