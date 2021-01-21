# Master R Script for Prepping Data for Dashboard
# Mozambique

#### Settings #### =============================================================
options(rsconnect.max.bundle.files = 400000)

CLEAN_SPATIAL_DATA <- F
CLEAN_TELECOM_DATA <- F
PREP_DATA_FOR_DASH <- T

BASELINE_DATE <- "2020-03-31"

#### Packages #### =============================================================
library(tidyverse)
library(sparkline)
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

if(Sys.info()[["user"]] == "robmarty") GITHUB_PATH <- "~/Documents/Github/covid-mobile-dashboards"
if(Sys.info()[["user"]] == "wb519128") GITHUB_PATH <- "C:/Users/wb519128/Github/covid-mobile-dashboards"
if(Sys.info()[["user"]] == "WB521633") GITHUB_PATH <- "C:/Users/wb521633/Documents/Github/covid-mobile-dashboards"

# Define Paths from Root -------------------------------------------------------
GADM_PATH       <- "PATH-HERE"
GEO_PATH        <- "PATH-HERE"

CLEAN_DATA_ADM2_PATH <- "PATH-HERE"
CLEAN_DATA_ADM3_PATH <- "PATH-HERE"

DASHBOARD_DATA_ONEDRIVE_PATH <- "PATH-HERE"
DASHBOARD_DATA_GITHUB_PATH     <- "PATH-HERE"

PREP_DATA_CODE_PATH <- "PATH-HERE"

#### Functions #### ============================================================
source(file.path(GITHUB_PATH, "dashboard-dataviz", "dashboards",
                 "_tp_functions.R"))

source(file.path(GITHUB_PATH, "dashboard-dataviz", "dashboards",
                 "_prep_data_for_dash_functions.R"))


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
 source(file.path(PREP_DATA_CODE_PATH, "03_dashboard_data_prep", "data_to_github.R"))
}















