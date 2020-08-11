# Master R Script for Prepping Data for Dashboard

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
library(hrbrthemes)

#### File paths #### ===========================================================

# Define Root Paths ------------------------------------------------------------
if(Sys.info()[["user"]] == "robmarty") PROJECT_PATH <- "~/Documents/World Bank/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "wb519128") PROJECT_PATH <- "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "WB521633") PROJECT_PATH <- "C:/Users/wb521633/WBG/Sveta Milusheva - COVID 19 Results"

if(Sys.info()[["user"]] == "robmarty") GITHUB_PATH <- "~/Documents/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "wb519128") GITHUB_PATH <- "C:/Users/wb519128/Github/covid-mobile-data"
if(Sys.info()[["user"]] == "WB521633") GITHUB_PATH <- "C:/Users/wb521633/Documents/Github/covid-mobile-data"

# Define Paths from Root -------------------------------------------------------
CLEAN_DATA_ADM2_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "files_for_dashboard", "files_clean", "adm2")
CLEAN_DATA_ADM3_PATH <- file.path(PROJECT_PATH, "proof-of-concept", "files_for_dashboard", "files_clean", "adm3")
figures_path <- file.path(PROJECT_PATH, "proof-of-concept", "outputs", "figures")
