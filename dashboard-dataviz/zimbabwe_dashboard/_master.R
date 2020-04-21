#### Settings #### =============================================================

rm(list = ls())

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

if(Sys.info()[["user"]] == "robmarty") PROJECT_PATH <- "~/Documents/World Bank/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "wb519128") PROJECT_PATH <- "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "WB521633") PROJECT_PATH <- "C:/Users/wb521633/WBG/Sveta Milusheva - COVID 19 Results"

if(Sys.info()[["user"]] == "robmarty") GITHUB_PATH <- "~/Documents/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "wb519128") GITHUB_PATH <- "C:/Users/wb519128/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "WB521633") GITHUB_PATH <- "C:/Users/wb521633/Documents/Github/covid-mobile-data"

RAW_DATA_ADM2_PATH   <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin2", "flowminder")
RAW_DATA_ADM3_PATH   <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin3", "flowminder")

CUSTOM_DATA_ADM2_PATH   <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin2", "custom")
CUSTOM_DATA_ADM3_PATH   <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin3", "custom")

CLEAN_DATA_ADM2_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin2", "data_clean")
CLEAN_DATA_ADM3_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin3", "data_clean")

GEO_ADM2_PATH        <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin2", "geo_files")
GEO_ADM3_PATH        <- file.path(PROJECT_PATH, "proof-of-concept", "databricks-results", "zw", "admin3", "geo_files")

DASHBOARD_DATA_PATH  <- file.path(GITHUB_PATH, "dashboard-dataviz", "zimbabwe_dashboard", "data_inputs_for_dashboard")




