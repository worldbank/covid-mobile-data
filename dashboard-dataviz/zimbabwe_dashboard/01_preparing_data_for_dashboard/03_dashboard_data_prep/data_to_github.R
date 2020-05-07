# Transfer dashboard data from OneDrive to Github

# Move telecom data to github folder -------------------------------------------
i <- 1

telecom_files <- list.files(DASHBOARD_DATA_ONEDRIVE_PATH, pattern = "*.Rds")

# Select subset if only need to move some
telecom_files <- telecom_files[grepl("Movement Out of|Movement Into", telecom_files)]

temp <- telecom_files %>%
  lapply(function(file_i){
    if((i %% 100) %in% 0) print(i)
    i <<- i + 1
    
    file.copy(file.path(DASHBOARD_DATA_ONEDRIVE_PATH, file_i),
              paste0(DASHBOARD_DATA_GITHUB_PATH, "/"),
              overwrite=T)
  })

# Move risk analysis files to github folder ---------------------------------
for(file_i in list.files(RISK_ANALYSIS_PATH)){
  file.copy(file.path(RISK_ANALYSIS_PATH, file_i),
            paste0(DASHBOARD_DATA_GITHUB_PATH, "/"),
            overwrite=T)
}