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


# Transfer risk analysis data

file.path("severe_disease_risk_district.csv")

if(!dir.exists(file.path(DASHBOARD_DATA_GITHUB_PATH, "risk-analysis"))){
  dir.create(file.path(DASHBOARD_DATA_GITHUB_PATH, "risk-analysis"))
}

file.copy(file.path(RISK_ANALYSIS_PATH, 
                    "severe_disease_risk_district.csv"),
          paste0(file.path(DASHBOARD_DATA_GITHUB_PATH, 
                           "risk-analysis"),
                 "/"),
          overwrite=T)
