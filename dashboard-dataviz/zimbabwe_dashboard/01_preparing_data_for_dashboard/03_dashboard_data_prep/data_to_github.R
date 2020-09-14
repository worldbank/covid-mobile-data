# Transfer dashboard data from OneDrive to Github and encrypt them

REMOVE_PREVIOUS_FILES <- T # delete previous data files in github repo. Files 
                           # will always be overwritten by this script, but this
                           # this is useful when changing namining conventions 
                           # of files

ENCRYPT <- F # whether to encrypt files

# Define Password --------------------------------------------------------------
PASSWORD <- readline(prompt="Enter password: ")

# hash 
data_key <- sha256(charToRaw(PASSWORD)) 

# Remove previous files in github ----------------------------------------------
if(REMOVE_PREVIOUS_FILES){
  temp <- list.files(DASHBOARD_DATA_GITHUB_PATH, 
                     full.names = T, 
                     pattern = "*.Rds|*.csv") %>%
    lapply(file.remove)
}

# Move telecom data to github folder -------------------------------------------
telecom_files <- list.files(DASHBOARD_DATA_ONEDRIVE_PATH, pattern = "*.Rds")

# Select subset if only need to move some
telecom_files <- telecom_files[grepl("covid_cases_districts", telecom_files)]

i <- 1
temp <- telecom_files %>%
  lapply(function(file_i){
    if((i %% 100) %in% 0) print(i)
    i <<- i + 1
    
    df <- readRDS(file.path(DASHBOARD_DATA_ONEDRIVE_PATH, file_i))
    
    # Never encrypt district/ward shapefiles
    if(file_i %in% c("wards_aggregated.Rds", "districts.Rds")){
      saveRDS(df, file.path(DASHBOARD_DATA_GITHUB_PATH, file_i), version=2)
    } else{
      #### Files to encrypt
      if(ENCRYPT){
        saveRDS(aes_cbc_encrypt(serialize(df, NULL), key = data_key), file.path(DASHBOARD_DATA_GITHUB_PATH, file_i), version=2)
      } else{
        saveRDS(df, file.path(DASHBOARD_DATA_GITHUB_PATH, file_i), version=2)
      }
      
      
    }

  })

# Move risk analysis files to github folder ---------------------------------
for(file_i in list.files(RISK_ANALYSIS_PATH)){
  file.copy(file.path(RISK_ANALYSIS_PATH, file_i),
            paste0(DASHBOARD_DATA_GITHUB_PATH, "/"),
            overwrite=T)
}


