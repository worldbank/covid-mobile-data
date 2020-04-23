#### Settings #### =============================================================

rm(list = ls())

CLEAN_SPATIAL_DATA <- F
CLEAN_TELECOM_DATA <- F
PREP_DATA_FOR_DASH <- F

#### Packages #### =============================================================
library(tidyverse)
library(sf)
library(sp)
library(plotly)
library(stargazer)
library(knitr)
library(gridExtra)
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

#### File paths #### ===========================================================

# Define Root Paths ------------------------------------------------------------
if(Sys.info()[["user"]] == "robmarty") PROJECT_PATH <- "~/Documents/World Bank/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "wb519128") PROJECT_PATH <- "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "WB521633") PROJECT_PATH <- "C:/Users/wb521633/WBG/Sveta Milusheva - COVID 19 Results"

if(Sys.info()[["user"]] == "robmarty") GITHUB_PATH <- "~/Documents/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "wb519128") GITHUB_PATH <- "C:/Users/wb519128/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "WB521633") GITHUB_PATH <- "C:/Users/wb521633/Documents/Github/covid-mobile-data"

# Define Paths from Root -------------------------------------------------------
GEO_PATH        <- file.path(PROJECT_PATH, "proof-of-concept", "geo_files")
RISK_ANALYSIS_PATH   <- file.path(PROJECT_PATH, "proof-of-concept", "risk-analysis")
DATABRICKS_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw")

CLEAN_DATA_ADM2_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "files_for_dashboard", "files_clean", "adm2")
CLEAN_DATA_ADM3_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "files_for_dashboard", "files_clean", "adm3")

DASHBOARD_DATA_ONEDRIVE_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "files_for_dashboard", "files_dashboard")

DASHBOARD_PATH          <- file.path(GITHUB_PATH, "dashboard-dataviz", "zimbabwe_dashboard")
DASHBOARD_DATA_GITHUB_PATH     <- file.path(DASHBOARD_PATH, "data_inputs_for_dashboard")

DASHBOARD_CLEAN_SPATIAL_PATH <- file.path(DASHBOARD_PATH, "01_preparing_data_for_dashboard", "01_clean_spatial_data")
DASHBOARD_CLEAN_TELECOM_PATH <- file.path(DASHBOARD_PATH, "01_preparing_data_for_dashboard", "02_clean_telecom_data")
DASHBOARD_PREP_DASHBOARD_PATH <- file.path(DASHBOARD_PATH, "01_preparing_data_for_dashboard", "03_dashboard_data_prep")

#### Functions #### ============================================================
source(file.path(DASHBOARD_CLEAN_TELECOM_PATH,
                 "_tp_functions.R"))

source(file.path(DASHBOARD_PREP_DASHBOARD_PATH,
                 "_prep_data_for_dash_functions.R"))

#### Scripts #### ============================================================

# 1. Prepare Spatial Data ------------------------------------------------------
if(CLEAN_SPATIAL_DATA){
  source(file.path(DASHBOARD_CLEAN_SPATIAL_PATH, "clean_districts.R"))
  source(file.path(DASHBOARD_CLEAN_SPATIAL_PATH, "clean_wards.R"))
}

# 2. Prepare Spatial Data ------------------------------------------------------
if(CLEAN_TELECOM_DATA){
  # Clean Initial Datasets
  source(file.path(DASHBOARD_CLEAN_SPATIAL_PATH, "clean_subscribers_data.R"))
  source(file.path(DASHBOARD_CLEAN_SPATIAL_PATH, "clean_movement_inout_data.R"))
  
  # Clean datasets that depend on previous ones
  source(file.path(DASHBOARD_CLEAN_SPATIAL_PATH, "clean_net_movement_data.R"))
}

# 3. Prep Data for Dashboard ---------------------------------------------------
if(PREP_DATA_FOR_DASH){
  source(file.path(DASHBOARD_PREP_DASHBOARD_PATH, "prep_subs_obs_totals_data.R"))
  source(file.path(DASHBOARD_PREP_DASHBOARD_PATH, "prep_telecom_agg_data.R"))
}

