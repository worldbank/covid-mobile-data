# Zimbabwe Movement Dashboard

# PACKAGES AND SETUP ===========================================================

options(rsconnect.max.bundle.files = 300000)
# options(rsconnect.max.bundle.size = 90000000)

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

#### Setting directory so will work locally
if (Sys.info()[["user"]] == "robmarty") {
  setwd("~/Documents/Github/covid-mobile-data/dashboard-dataviz/zimbabwe_dashboard")
}

if (Sys.info()[["user"]] == "WB521633") {
  setwd(
    "C:/Users/wb521633/Documents/Github/covid-mobile-data/dashboard-dataviz/zimbabwe_dashboard"
  )
}

if (Sys.info()[["user"]] == "wb519128") {
  setwd(
    "C:/Users/wb519128/GitHub/covid-mobile-data/dashboard-dataviz/zimbabwe_dashboard"
  )
}

# RUN THE APP ==================================================================
shinyApp(ui, server)
