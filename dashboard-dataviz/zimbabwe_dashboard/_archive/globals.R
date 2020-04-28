# Globals ======================================================================


#### Pacakges
library(shinydashboard)
library(RColorBrewer)
library(shinythemes)
library(DT)
library(dplyr)
library(rmarkdown)
library(lubridate)
library(shiny)
library(ggplot2)
library(tidyr)
library(shinyWidgets)
library(zoo)
library(bcrypt)
library(shinyjs)
library(ngram)
library(rtweet)
library(stringdist)
library(stringr)
library(rgdal)
library(rgeos)
library(geosphere)
library(htmlwidgets)
library(tidyverse)
library(sf)
library(tidyverse)
library(raster)
library(leaflet)
library(leaflet.extras)
library(plotly)
library(data.table)
library(formattable)
library(tidyr)
library(viridis)
library(data.table)
library(raster)
library(htmltools)
library(scales)
library(lubridate)

#### Logged; make false to enable password
Logged = F


# LOAD/PREP DATA ===============================================================


#### Spatial base layers
ward_sp <- readRDS(file.path("data_inputs_for_dashboard", "wards_aggregated.Rds"))
district_sp <- readRDS(file.path("data_inputs_for_dashboard", "districts.Rds"))

#### Province List for Select Input
provinces <- ward_sp$province %>% unique() %>% sort()
provinces <- c("All", provinces)

#### Totals
obs_total  <- readRDS(file.path("data_inputs_for_dashboard","observations_total.Rds"))
subs_total <- readRDS(file.path("data_inputs_for_dashboard","subscribers_total.Rds"))


#### Risk analysis Data 
risk_an <- fread(file.path(RISK_ANALYSIS_PATH, 
                           "severe_disease_risk_district.csv"))
risk_an_labs <- fread(file.path(RISK_ANALYSIS_PATH, 
                           "severe_disease_risk_district_labels.csv"))

#### Data descriptions
data_methods_text <- read.table("text_inputs/data_methods.txt", sep="{")[[1]] %>% 
  as.character()
data_source_description_text <- read.table("text_inputs/data_source_description.txt", sep="{")[[1]] %>%
  as.character()

#### Risk analysis text
risk_analysis_text <- read.table("text_inputs/risk_analysis.txt", sep="{")[[1]] %>% 
  as.character()

# risk_analysis_text <- paste(risk_analysis_text[1],
#                             risk_analysis_text[2],
#                             sep = "<br>")

#### Default parameters on load
unit_i <- "Wards"
variable_i <- "Density"
timeunit_i <- "Daily"
date_i <- "2020-02-01"
previous_zoom_selection <- ""
metric_i <- "Count"
