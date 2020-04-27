# Zimbabwe Movement Dashboard

# PACKAGES AND SETUP ===========================================================

options(rsconnect.max.bundle.files = 300000)
# options(rsconnect.max.bundle.size = 90000000)



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
