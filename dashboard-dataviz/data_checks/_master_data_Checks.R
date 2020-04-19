# Master Script for Checks on Data

# Filepaths --------------------------------------------------------------------
if(Sys.info()[["user"]] == "robmarty") dropbox_file_path <- "~/Documents/World Bank/Sveta Milusheva - COVID 19 Results"
if(Sys.info()[["user"]] == "wb519128") dropbox_file_path <- "C:/Users/wb519128/WBG/Sveta Milusheva - COVID 19 Results"

zw_data_file_path          <- file.path(project_file_path, "proof-of-concept", "databricks-results", "zw")
zw_admin3_data_file_path   <- file.path(zw_data_file_path, "admin3")
zw_voronoi_data_file_path  <- file.path(zw_data_file_path, "voronoi")

# Packages ---------------------------------------------------------------------
library(tidyverse)
library(sf)
library(sp)
library(plotly)
library(stargazer)
library(knitr)
library(gridExtra)
library(ggpubr)
