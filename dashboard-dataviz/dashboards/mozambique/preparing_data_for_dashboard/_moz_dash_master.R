# Master R Script for Prepping Data for Dashboard
# Mozambique

#### Settings #### =============================================================
options(rsconnect.max.bundle.files = 400000)

CLEAN_SPATIAL_DATA <- F
CLEAN_TELECOM_DATA <- T
PREP_DATA_FOR_DASH <- T

BASELINE_DATE <- "2020-03-31"

#### Packages #### =============================================================
library(tidyverse)
library(sf)
library(sp)
library(plotly)
library(stargazer)
library(knitr)
library(gridExtra)
library(leaflet)
library(ggpubr)
library(purrr)
library(parallel)
library(pbmcapply)
library(rgeos)
library(rgdal)
library(sp)
library(rmapshaper)
library(raster)
library(geosphere)
library(lubridate)
library(data.table)
library(mapview)
library(bcrypt)

#### File paths #### ===========================================================

# Define Root Paths ------------------------------------------------------------
if(Sys.info()[["user"]] == "robmarty") PROJECT_PATH <- "~/Documents/World Bank/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "wb519128") PROJECT_PATH <- "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "WB521633") PROJECT_PATH <- "C:/Users/wb521633/WBG/Sveta Milusheva - COVID 19 Results"

if(Sys.info()[["user"]] == "robmarty") GITHUB_PATH <- "~/Documents/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "wb519128") GITHUB_PATH <- "C:/Users/wb519128/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "WB521633") GITHUB_PATH <- "C:/Users/wb521633/Documents/Github/covid-mobile-data"

if(Sys.info()[["user"]] == "wb519128") RAW_INDICATORS <- "C:/Users/wb519128/WBG/Dunstan Matekenya - MZ_indicators_MarApr2020"
if(Sys.info()[["user"]] == "WB521633") RAW_INDICATORS <- "C:/Users/wb521633/WBG/Dunstan Matekenya - MZ_indicators_MarApr2020"

# Define Paths from Root -------------------------------------------------------
GADM_PATH       <- file.path(PROJECT_PATH, "Mozambique", "gadm")
GEO_PATH        <- file.path(PROJECT_PATH, "Mozambique", "geo_files")

CLEAN_DATA_ADM2_PATH <- file.path(PROJECT_PATH, "Mozambique", "files_for_dashboard", "files_clean", "adm2")
CLEAN_DATA_ADM3_PATH <- file.path(PROJECT_PATH, "Mozambique", "files_for_dashboard", "files_clean", "adm3")

DASHBOARD_DATA_ONEDRIVE_PATH <- file.path(PROJECT_PATH, "Mozambique", "files_for_dashboard", "files_dashboard")
DASHBOARD_DATA_GITHUB_PATH     <- file.path(GITHUB_PATH, "dashboard-dataviz", "dashboards", "mozambique",
                                            "data_inputs_for_dashboard")

PREP_DATA_CODE_PATH <- file.path(GITHUB_PATH, "dashboard-dataviz", "dashboards", "mozambique", "preparing_data_for_dashboard")

#### Functions #### ============================================================
source(file.path(GITHUB_PATH, "dashboard-dataviz", "dashboards",
                 "_tp_functions.R"))

source(file.path(GITHUB_PATH, "dashboard-dataviz", "dashboards",
                 "_prep_data_for_dash_functions.R"))

#### FUNCTIONS #### ============================================================
clean_moz_names <- function(data, name, name_higher, type){
  # data: dataframe
  # name: name
  # name_higher: name of higher level adm
  
  # Temp names to make things cleaner below
  data$name_var = data[[name]]
  data$name_higher_var = data[[name_higher]]
  
  # Clean
  data$name_var        <- data$name_var %>% as.character() %>% str_replace_all("[^\x01-\x7F]+", "")
  data$name_higher_var <- data$name_higher_var %>% as.character() %>% str_replace_all("[^\x01-\x7F]+", "")
  
  if(type %in% "adm2"){
    data$name_var[data$name_var %in% "Boane" & data$name_higher_var %in% "Maputo"] <- "Maputo - Boane"
    data$name_var[data$name_var %in% "Boane" & data$name_higher_var %in% "Maputo City"] <- "Maputo City - Boane"
    
    data$name_var[data$name_var %in% "Chire"] <- "Chiure"
    data$name_var[data$name_var %in% "Chkw"] <- "Chokwe"
    data$name_var[data$name_var %in% "Guij"] <- "Guija"
    data$name_var[data$name_var %in% "Manhia"] <- "Manhica"
    data$name_var[data$name_var %in% "Matutune"] <- "Matutuine"
    data$name_var[data$name_var %in% "Maa"] <- "Maua"
    data$name_var[data$name_var %in% "Angnia"] <- "Angonia"
    
  }
  
  if(type %in% "adm3"){
    data$name_var[data$name_var %in% "Govuro" & data$name_higher_var %in% "Save"] <- "Govuro - Save"
    data$name_var[data$name_var %in% "Guro" & data$name_higher_var %in% "Dacata"] <- "Guro - Dacata"
    data$name_var[data$name_var %in% "Machaze" & data$name_higher_var %in% "Save"] <- "Machaze - Save"
    data$name_var[data$name_var %in% "Mossurize" & data$name_higher_var %in% "Dacata"] <- "Mossurize - Dacata"
    data$name_var[data$name_var %in% "Namapa" & data$name_higher_var %in% "Lurio"] <- "Namapa - Lurio"
    data$name_var[data$name_var %in% "Cuamba" & data$name_higher_var %in% "Lurio"] <- "Cuamba - Lurio"
    data$name_var[data$name_var %in% "Lago" & data$name_higher_var %in% "Lago Niassa"] <- "Lago - Lago Niassa"
  }
  
  data[[name]] <- data$name_var
  
  data$name_var <- NULL
  data$name_higher_var <- NULL
  
  return(data)
}

#### Scripts #### ==============================================================

# 1. Prepare Spatial Data ------------------------------------------------------
if(CLEAN_SPATIAL_DATA){
  source(file.path(PREP_DATA_CODE_PATH, "01_clean_spatial_data", "download_gadm.R"))
  source(file.path(PREP_DATA_CODE_PATH, "01_clean_spatial_data", "clean_adm2_file.R"))
  source(file.path(PREP_DATA_CODE_PATH, "01_clean_spatial_data", "clean_adm3_file.R"))
}

# 2. Prepare Spatial Data ------------------------------------------------------
if(CLEAN_TELECOM_DATA){
  source(file.path(PREP_DATA_CODE_PATH, "02_clean_telecom_data", "clean_i3_subscribers_data.R"))
  source(file.path(PREP_DATA_CODE_PATH, "02_clean_telecom_data", "clean_i5_movement_inout_data.R"))
  source(file.path(PREP_DATA_CODE_PATH, "02_clean_telecom_data", "clean_i5_net_movement_data.R"))
  source(file.path(PREP_DATA_CODE_PATH, "02_clean_telecom_data", "clean_i7_distance_traveled.R"))
}

# 3. Prep Data for Dashboard ---------------------------------------------------
if(PREP_DATA_FOR_DASH){
 source(file.path(PREP_DATA_CODE_PATH, "03_dashboard_data_prep", "prep_subs_obs_totals_data.R"))
 source(file.path(PREP_DATA_CODE_PATH, "03_dashboard_data_prep", "prep_telecom_agg_data.R"))
}















