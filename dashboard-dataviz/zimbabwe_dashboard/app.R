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
Logged = T


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


# UIs ==========================================================================

# * Password UI ---------------------------------------------------------------
ui_password <- function() {
  tagList(
    div(
      id = "login",
      HTML("<center> 
           <h3> WBG <br> COVID Mobility Analytics Task Force 
           </h3>
           </center> "),
      wellPanel(
        textInput("userName", "Username"),
        passwordInput("passwd", "Password"),
        br(),
        actionButton("Login", "Log in")
      )
    ),
    tags$style(
      type = "text/css",
      "#login {font-size:12px;
      text-align:
      left;position:absolute;top:
      40%;left:
      50%;margin-top:
      -100px;margin-left:
      -150px;}"
    )
  )
}


ui = (htmlOutput("page"))

# * Main UI -------------------------------------------------------------------
ui_main <- fluidPage(
  #tags$head(includeHTML(("google-analytics.html"))),
  h4("WBG - COVID Mobility Analytics Task Force", align="center"),
  #HTML("<h4> 
  #      WBG - COVID Mobility Analytics Task Force 
  #      </h4>"),
  navbarPage(
    theme = shinytheme("flatly"), # journal
    collapsible = TRUE,
    title = "Zimbabwe",
    
    id = "nav",
    
    # World Bank COVID Mobility Analytics Task Force
    
    
    # ** Telecom Data ----------------------------------------------------------
    tabPanel(
      "Dashboard",
      tags$head(includeCSS("styles.css")),
      
      dashboardBody(
        fluidRow(
          
          column(2,
                 strong(textOutput("metric_description"))),
          
          column(2,
                 align = "center",
                 selectInput(
                   "select_unit",
                   label = h4("Select Unit"),
                   choices = c("Wards", "Districts"),
                   multiple = F
                 )
          ),
          
          column(2,
                 align = "center",
                 uiOutput("ui_select_variable")
          ),
          
          column(
            width = 2,
            align = "center",
            selectInput(
              "select_metric",
              label = h4("Select Metric"),
              choices = c("Count",
                          "% Change",
                          "Z-Score"),
              multiple = F
            )
          ),
          
          column(
            width = 2,
            align = "center",
            selectInput(
              "select_timeunit",
              label = h4("Select Time Unit"),
              choices = c("Daily",
                          "Weekly"),
              multiple = F
            )
          )
          
        ),
        fluidRow(
          column(
            width = 9,
            
            h4(textOutput("map_title"),
               align = "center"),
            
            #strong(textOutput("metric_description"),
            #  align = "center"),
            
            strong(textOutput("map_instructions"),
                   align = "center"),
            
            leafletOutput("mapward",
                          height = 720),
            
            absolutePanel(
              id = "controls",
              class = "panel panel-default",
              top = 350,
              left = 40,
              width = 200,
              fixed = TRUE,
              draggable = TRUE,
              height = 300,
              align = "center",
              
              h5("Select Date"),
              uiOutput("ui_select_timeunit"),
              
              uiOutput("ui_select_region_zoom"),
              
              
              selectInput(
                "select_province",
                label = h5(textOutput("select_province_title")),
                choices = provinces,
                multiple = F
              )
            )
          ),
          column( 
            3,
            wellPanel(
              strong(textOutput("line_title"), align = "center"),
              h6(textOutput("line_instructions"), align = "center"),
              plotlyOutput("ward_line_time", height =
                             200),
              #hr(),
              strong(htmlOutput("table_title"), align = "center"),
              div(style = 'height:380px; overflow-y: scroll',
                  formattableOutput("ward_top_5_in")),
              h5(textOutput("rank_text"))
            )
          )
        ),
        fluidRow(
          column(12,
                 " ")
        )
        
      )
      
    ),
    
    # ** Risk analysis ------------------------------------------------------
    tabPanel(
      "Risk analysis",
      # Title and text
      
      dashboardBody(
        fluidRow(
          
          column(2,
                 #wellPanel(
                   #align = "center",
                   selectInput(
                     "select_risk_indicator",
                     label = h4("Select Indicator"),
                     
                     # Cambiarra braba arrumar isso dai
                     choices = c("HIV prevalence quintile", 
                                 "Anaemia prevalence quintile",
                                 "Respiratory illness prevalence quintile",
                                 "Overweight prevalence quintile", 
                                 "Smoking prevalence quintile",
                                 "Severe COVID-19 risk"),
                     selected = "HIV prevalence quintile",
                     multiple = F),
                   
                   
                   p(risk_analysis_text[1]),
                   p(risk_analysis_text[2])
                 #)
                 
          ),
          
          column(7, 
                 
                 fluidRow(
                   #column(3,
                   #        " "),
                   column(3, align="center", offset=3,
                          
                          selectInput(
                            "move_date_risk",
                            label = h4("Movement Date"),
                            choices = c("Feb 01 - Feb 07",
                                        "Feb 08 - Feb 14",
                                        "Feb 15 - Feb 21",
                                        "Feb 22 - Feb 28",
                                        "Feb 29 - Mar 06",
                                        "Mar 07 - Mar 13",
                                        "Mar 14 - Mar 20",
                                        "Mar 21 - Mar 27"),
                            multiple = F)
                   ),
                   column(3, align="center",
                          
                          selectInput("move_type_risk",
                                      label = h4("Movement Indicator"),
                                      choices = c("Movement Out of Districts",
                                                  "Movement Into Districts"),
                                      multiple = F
                          )
                   )
                 ),
                 
                 
                 leafletOutput("riskmap",
                               height = 720)
          ),
          
          column(3,
                 formattableOutput('risk_table2')  
          )
          
          
        )
      )
      
    ),
    
    # ** Data Description ------------------------------------------------------
    tabPanel("Data Description",
             fluidRow(column(12,
                             ""),
                      column(
                        4,
                        offset = 4,
                        fluidRow(
                          h1("Data Description", align = "center"),
                          
                          h4("Data Sources"),
                          data_source_description_text,
                          
                          h4("Methods"),
                          data_methods_text
                          
                          
                        )
                        
                      )
             ),
             fluidRow(
               column(12,
                      " ")
             ),
             fluidRow(
               column(2,
                      " "),
               column(4, align="right",
                      plotlyOutput("obs_total",
                                   height=350,
                                   width=430)
               ),
               column(4, align="left",
                      plotlyOutput("subs_total",
                                   height=350,
                                   width=430)
               )
               
             )
    )
    
  )
)


# SERVER =======================================================================
server = (function(input, output, session) {
  # ** Password ----------------------------------------------------------------
  USER <- reactiveValues(Logged = Logged)
  
  #### Check password details
  # If user is not logged in, check their username and password inputs. If the
  # username/password is correct, set Logged to true
  observe({
    if (USER$Logged == FALSE) {
      if (!is.null(input$Login)) {
        if (input$Login > 0) {
          Username <- isolate(input$userName)
          Password <- isolate(input$passwd)
          
          passwords_df <- read.csv("passwords.csv")
          
          passwords_df$username <-
            passwords_df$username %>% as.character()
          passwords_df$password <-
            passwords_df$password %>% as.character()
          
          if (Username %in% passwords_df$username) {
            passwords_df_i <- passwords_df[passwords_df$username %in% Username, ]
            
            #if(checkpw(Password, passwords_df_i$HashedPassword) %in% TRUE){
            if ((Password %in% passwords_df$password) %in% TRUE) {
              USER$Logged <- TRUE
            }
            
          }
        }
      }
    }
  })
  
  #### Toggle between UIs
  observe({
    # If not logged in, go to password UI
    if (USER$Logged == FALSE) {
      output$page <- renderUI({
        div(class = "outer", do.call(bootstrapPage, c("", ui_password())))
      })
    }
    
    # If not logged in, go to main ui
    if (USER$Logged == TRUE)
    {
      output$page <- renderUI({
        div(ui_main)
      })
      
      # ** Reactives and Observes ----------------------------------------------
      
      ##### **** Basemap Filtering ##### 
      ward_sp_filter <- reactive({
        
        #### Default
        if(is.null(input$select_unit)){
          out <- ward_sp
        } else{
          
          #### Select Admin Unit Level
          if(input$select_unit %in% "Wards"){
            admin_sp <- ward_sp
            out <- admin_sp
          } else if (input$select_unit %in% "Districts"){
            admin_sp <- district_sp
            out <- admin_sp
          } else{
            admin_sp <- ward_sp
            out <- admin_sp
          }
          
          #### Restrict to province
          if (!is.null(input$select_province)) {
            if (!(input$select_province %in% "All")) {
              out <- admin_sp[admin_sp$province %in% input$select_province, ]
            }
          }
          
        }
        
        
        out
      })
      
      
      
      
      ##### **** Telecom Data Filtering ##### 
      ward_data_sp_filtered <- reactive({
        #### Determine the selected ward.
        # This is relevant only in O-D matrices when select an origin ward
        ward_i <- "Harare 6"
        if (!is.null(input$mapward_shape_click$id)){
          ward_i <- input$mapward_shape_click$id
        }
        
        # When shiny starts, inputs are null
        
        unit_i <- input$select_unit
        variable_i <- input$select_variable 
        timeunit_i <- input$select_timeunit 
        date_i <- input$date_ward
        metric_i <- input$select_metric
        
        if(is.null(unit_i)) unit_i <- "Wards"
        if(is.null(variable_i)) variable_i <- "Density"
        if(is.null(timeunit_i)) timeunit_i <- "Daily"
        if(is.null(date_i)) date_i <- "2020-02-01"
        if(is.null(metric_i)) metric_i <- "Count"
        
        
        variable_i <- variable_i %>% str_replace_all(" Districts| Wards", "") 
        unit_i_singular <- substr(unit_i, 1, nchar(unit_i) - 1)
        
        #### Switch default admin_unit if switch to wards/districts
        if(unit_i %in% "Wards"){
          if(!(ward_i %in% ward_sp$name)){
            ward_i <- "Harare 6"
          }
        }
        
        if(unit_i %in% "Districts"){
          if(!(ward_i %in% district_sp$name)){
            ward_i <- "Harare"
          }
        }
        
        
        # Because date_i is a function of renderUI, updates later which causes
        # a problem when switching time unti types
        
        if( (timeunit_i %in% "Weekly") & 
            (substring(date_i,1,4) %in% "2020")){
          date_i <- "Feb 01 - Feb 07"
        }
        
        if( (timeunit_i %in% "Daily") & 
            (!(substring(date_i,1,4) %in% "2020"))){
          date_i <- "2020-02-01"
        }
        
        # Density - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        if(variable_i %in% c("Density")){
          
          # Need to check for 2020 in case weekly
          if(substring(date_i,1,4) %in% "2020"){
            #if(date_i > "2020-02-29") date_i <- "2020-02-01"
          }
          
          ward_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    date_i,".Rds")))
          
          time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    ward_i,".Rds")))
          
          
          if(metric_i %in% "Count"){
            
            map_data <- ward_level_df %>%
              dplyr::mutate(value = density,
                            html_label = label_level) %>%
              dplyr::select(value, html_label)
            
            table_data <- ward_level_df %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "% Change"){
            
            map_data <- ward_level_df %>%
              dplyr::mutate(value = value_perchange_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label)
            
            table_data <- ward_level_df %>%
              dplyr::mutate(value = value_perchange_base) %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "Z-Score"){
            
            map_data <- ward_level_df %>%
              dplyr::mutate(value = value_zscore_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label)
            
            table_data <- ward_level_df %>%
              dplyr::mutate(value = value_zscore_base) %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          }
          
          out <- list(
            map_data = map_data,
            table_data = table_data,
            line_data = line_data,
            pal_val_max = max(ward_level_df$value, na.rm = T),
            map_title = paste0(unit_i_singular,
                               " Density: ", 
                               date_i),
            table_title = paste0("Top ",
                                 unit_i,
                                 ": ", # by Total Number of Subscribers: 
                                 date_i),
            table_subtitle = "",
            line_title = paste0("Trends in Subscribers in ", ward_i)
          )
          
        }
        
        # Density - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        # TODO: Can more cleanly integrtate into Density / Non-OD all together
        if(variable_i %in% c("Net Movement", "Median Distance Traveled")){
          
          # Need to check for 2020 in case weekly
          if(substring(date_i,1,4) %in% "2020"){
            #if(date_i > "2020-02-29") date_i <- "2020-02-01"
          }
          
          
          ward_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    date_i,".Rds")))
          
          time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    ward_i,".Rds")))
          
          #print(ward_level_df)
          
          
          if(metric_i %in% "Count"){
            
            map_data <- ward_level_df %>%
              dplyr::select(value, label_level) %>%
              dplyr::rename(html_label = label_level)
            
            table_data <- ward_level_df %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df
            
          } else if (metric_i %in% "% Change"){
            
            map_data <- ward_level_df %>%
              dplyr::select(value_perchange_base, label_base) %>%
              dplyr::rename(value = value_perchange_base,
                            html_label = label_base)
            
            table_data <- ward_level_df %>%
              dplyr::select(name, value_perchange_base) %>%
              dplyr::rename(value = value_perchange_base)
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "Z-Score"){
            
            map_data <- ward_level_df %>%
              dplyr::select(value_zscore_base, label_base) %>%
              dplyr::rename(value = value_zscore_base,
                            html_label = label_base)
            
            table_data <- ward_level_df %>%
              dplyr::select(name, value_zscore_base) %>%
              dplyr::rename(value = value_zscore_base)
            
            line_data <- time_level_df 
            
          }
          
          
          out <- list(
            map_data = map_data,
            table_data = table_data,
            line_data = line_data,
            pal_val_max = max(ward_level_df$value, na.rm = T),
            map_title = paste0(unit_i_singular,
                               " ", variable_i, ": ",
                               " Density: ", 
                               date_i),
            table_title = paste0("Top ",
                                 unit_i,
                                 ": ", # by Total Number of Subscribers: 
                                 date_i),
            table_subtitle = "",
            line_title = paste0("Trends in Movement Distance in ", ward_i)
          )
          
        }
        
        # Movement - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        if(variable_i %in% c("Movement Into",
                             "Movement Out of")){
          
          ward_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    date_i,".Rds")))
          
          time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    ward_i,".Rds")))
          
          ward_time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                                  paste0(unit_i,"_",
                                                         variable_i, "_",
                                                         timeunit_i, "_",
                                                         ward_i,"_",
                                                         date_i, ".Rds")))
          
          
          if(metric_i %in% "Count"){
            
            map_data <- ward_time_level_df %>%
              dplyr::mutate(html_label = label_level) %>%
              dplyr::select(value, html_label) 
            
            table_data <- ward_level_df %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "% Change"){
            
            map_data <- ward_time_level_df %>%
              dplyr::mutate(value = value_perchange_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label) 
            
            table_data <- ward_level_df %>%
              dplyr::mutate(value = value_perchange_base) %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "Z-Score"){
            
            map_data <- ward_time_level_df %>%
              dplyr::mutate(value = value_zscore_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label) 
            
            table_data <- ward_level_df %>%
              dplyr::mutate(value = value_zscore_base) %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          }
          
          out <- list(
            map_data = map_data,
            table_data = table_data,
            line_data = line_data,
            pal_val_max = max(ward_level_df$value, na.rm = T),
            map_title =  ifelse(input$select_variable %in% c("Movement Out of Wards",
                                                             "Movement Out of Districts"),
                                paste0("Number of People Moving from ", 
                                       ward_i, 
                                       " to other ",
                                       unit_i,
                                       ": ", 
                                       date_i),
                                paste0("Number of People Moving into ", 
                                       ward_i, 
                                       " from other ",
                                       unit_i,
                                       ": ", 
                                       date_i)) ,
            
            table_title =  ifelse(input$select_variable %in% c("Movement Out of Wards",
                                                               "Movement Out of Districts"),
                                  paste0(unit_i, " with Most Movement Out: ", date_i),
                                  paste0(unit_i, " with Most Movement In: ", date_i)) ,
            
            table_subtitle = paste0("Total from all ", unit_i),
            
            line_title =  ifelse(input$select_variable %in% c("Movement Out of Wards",
                                                              "Movement Out of Districts"),
                                 paste0("Total Movement out of ", ward_i, " over Time"),
                                 paste0("Total Movement into ", ward_i, " over Time")) 
            
            
          )
          
        }
        
        
        
        out
        
      })
      
      # ** Map -----------------------------------------------------------------
      legend_colors <- rev(viridis(5))
      legend_labels <- c("High", "", "", "", "Low")
      
      #### Basemap
      output$mapward <- renderLeaflet({
        
        map_sp <- ward_sp_filter()
        map_extent <- map_sp %>% extent()
        
        leaflet() %>%
          addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
          fitBounds(
            lng1 = map_extent@xmin,
            lat1 = map_extent@ymin,
            lng2 = map_extent@xmax,
            lat2 = map_extent@ymax
          ) 
        
      })
      
      #### Add polygons to map reactively
      observe({
        
        ward_data_sp_react <- ward_data_sp_filtered()
        map_values <- ward_data_sp_react$map_data$value
        map_labels <- ward_data_sp_react$map_data$html_label
        
        #### If limit to province
        if(!is.null(input$select_province)){
          if(!(input$select_province %in% "All")){
            
            if(input$select_unit %in% "Wards"){
              map_values <- map_values[ward_sp$province %in% input$select_province]
              map_labels <- map_labels[ward_sp$province %in% input$select_province]
            } 
            
            if(input$select_unit %in% "Districts"){
              map_values <- map_values[district_sp$province %in% input$select_province]
              map_labels <- map_labels[district_sp$province %in% input$select_province]
            }
            
          }
        }
        
        #### NAs/O-D
        # Integrate this all into processing so don't have to do here in a kinda
        # hacky way.
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% c("Movement Into Wards",
                                          "Movement Out of Wards",
                                          "Movement Into Districts",
                                          "Movement Out of Districts")){
            #od_index <- which(is.na(map_values))
            
            od_index <- which(grepl("Origin|Destination", map_labels))
            
            #print(od_index)
          }
        }
        
        # For values with label of 15 or less to be NA. /
        if(!is.null(input$select_metric)){
          if(input$select_metric %in% c("% Change", "Z-Score")){
            
            map_values[grepl("information", map_labels)] <- NA
            
          } else{
            
            if(!is.null(input$select_variable)){
              if(input$select_variable %in% c("Density")){
                map_values[grepl("information", map_labels)] <- 0
                
              }
            }
            
            
          }
        }
        
        map_data <- ward_sp_filter()
        
        log_neg <- function(values){
          # Log that takes into account zero. Only for logging values for
          # displaying!
          
          values_pos_index <- (values > 0)  %in% T # %in% T to account for NAs 
          values_neg_index <- (values <= 0) %in% T
          
          values_pos_log <- log(values[values_pos_index]+1)
          values_neg_log <- -log(-(values[values_neg_index])+1)
          
          values[values_pos_index] <- values_pos_log
          values[values_neg_index] <- values_neg_log
          
          return(values)
        }
        
        #### Make outliers less extreme
        # Chop off at percentile
        q_vals <- quantile(map_values, c(.025,.975), na.rm=T)
        map_values[map_values < q_vals[1]] <- q_vals[1]
        map_values[map_values > q_vals[2]] <- q_vals[2]
        
        #### Log Values
        if(!is.null(input$select_metric)){
          if(input$select_metric %in% "Count"){
            
            if(!is.null(input$select_variable)){
              if(!(input$select_variable %in% "Net Movement")){
                map_values <- log_neg(map_values)
              } 
            }
            
          }
        }
        
        #print(summary(map_values))
        
        
        #print(map_values)
        
        ##### Define color palettes
        
        ## Diverging
        #N_colors_above0 <- max(map_values, na.rm=T) %>% abs() %>% round(0)
        #  N_colors_below0 <- min(map_values, na.rm=T) %>% abs() %>% round(0)
        
        #N_colors_below0 <- colorRampPalette(colors = c("red3", "lemonchiffon"), 
        #                                    space = "Lab")(N_colors_below0*10)
        #N_colors_above0 <- colorRampPalette(colors = c("lemonchiffon", "forestgreen"), 
        #                                    space = "Lab")(N_colors_above0*10)
        #rampcols <- c(N_colors_below0, N_colors_above0)
        #pal_ward <- colorNumeric(palette = rampcols, 
        #                         domain = map_values)
        
        
        pal_ward <- colorNumeric(
          palette = "viridis",
          domain = c(map_values), # c(0, map_values)
          na.color = "gray",
          reverse = F
        )
        
        if (nrow(map_data) > 700) {
          alpha = 1
        } else{
          alpha = 1 # 0.75 fix clear shapes before do this.
        }
        
        # 
        l <- leafletProxy("mapward", data = map_data) %>%
          addPolygons(
            label = ~ lapply(map_labels, htmltools::HTML),
            color = ~ pal_ward(map_values),
            
            layerId = ~ name,
            
            stroke = TRUE,
            weight = 1,
            smoothFactor = 0.2,
            fillOpacity = alpha,
            dashArray = "3",
            
            highlight =
              highlightOptions(
                weight = 5,
                color = "#666",
                dashArray = "",
                fillOpacity = 1,
                bringToFront = TRUE
              ),
            
            labelOptions = labelOptions(
              style = list("font-weight" = "normal",
                           padding = "3px 8px"),
              textsize = "15px",
              direction = "auto"
            )
          ) %>%
          clearControls() %>%
          addLegend(
            values = c(map_values), # c(0, map_values)
            colors = legend_colors,
            labels = legend_labels,
            opacity = 0.7,
            title = "Legend",
            position = "topright",
            na.label = "Origin"
          )
        
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% c("Movement Into Wards",
                                          "Movement Out of Wards",
                                          "Movement Into Districts",
                                          "Movement Out of Districts")){
            
            l <- l %>% 
              addPolygons(data=map_data[od_index,],
                          label = ~ lapply(map_labels[od_index], htmltools::HTML),
                          labelOptions = labelOptions(
                            style = list("font-weight" = "normal",
                                         padding = "3px 8px"),
                            textsize = "15px",
                            direction = "auto"
                          ),
                          color="red",
                          fillOpacity=1,
                          stroke = TRUE,
                          weight = 1,
                          smoothFactor = 0.2)
            
          }
        }
        
        
        
        #### Further Zoom to Region
        # Only change if choose something different
        if(!is.null(input$select_region_zoom)){
          if(previous_zoom_selection != input$select_region_zoom){
            if(input$select_region_zoom %in% map_data$name){
              
              #print(input$select_region_zoom %in% map_data$name_id)
              
              loc_i <- which(map_data$name %in% input$select_region_zoom)
              #map_labels_zoom <- map_labels[loc_i]
              map_data_zoom <- map_data[loc_i,] 
              
              map_data_zoom_extent <- map_data_zoom %>% extent()
              
              l <- l %>%
                fitBounds(
                  lng1 = map_data_zoom_extent@xmin,
                  lat1 = map_data_zoom_extent@ymin,
                  lng2 = map_data_zoom_extent@xmax,
                  lat2 = map_data_zoom_extent@ymax
                ) #%>%
              #addPolygons(data=map_data_zoom,
              #            #label = ~ lapply(map_labels, htmltools::HTML),
              #            #layerId = ~ name_id,
              #            color="yellow",
              #            opacity = 1.0, fillOpacity = 0)
              
              
              
              
              previous_zoom_selection <<- input$select_region_zoom
              
            }
          }
        }
        
        
        l
        
        
        
      })
      
      # ** Line Graph: ---------------------------------------------------------
      output$ward_line_time <- renderPlotly({
        
        ward_data_sp_react <- ward_data_sp_filtered()
        data_line <- ward_data_sp_react$line_data
        
        # Rename so variables are cleaner in plotly display
        data_line <- data_line %>%
          dplyr::rename(Date = date,
                        N = value)
        
        #### Figure
        # Slight difference in how constructed depending on daily/weekly value
        if (input$select_timeunit %in% "Daily") {
          p <- ggplot(data_line,
                      aes(x = Date, 
                          y = N)) +
            geom_line(size = 1, color = "orange") +
            geom_point(size = 1, color = "orange") +
            geom_point(data=data_line[as.character(data_line$Date) %in% as.character(input$date_ward),],
                       aes(x = Date,
                           y = N),
                       size = 2.5, pch = 1, color = "forestgreen") +
            labs(
              x = "",
              y = "",
              title = "",
              color = ""
            ) +
            scale_y_continuous(labels = scales::comma, 
                               limits = c(min(data_line$N, na.rm=T), max(data_line$N, na.rm =
                                                                           T))) +
            theme_minimal() +
            theme(plot.title = element_text(hjust = 0.5),
                  axis.text.x = element_text(angle = 45))
          
          
          if(!(input$select_metric %in% "Count")){
            
            dow_i <- input$date_ward %>% as.Date() %>% wday()
            data_dow_i <- data_line[data_line$dow %in% dow_i,] 
            
            data_dow_i <- data_dow_i[month(data_dow_i$Date) %in% 2,]
            
            #data_dow_i <- data_dow_i %>%
            #  arrange(Date) %>%
            #  head(-1)
            
            p <- p + 
              geom_point(data=data_dow_i, aes(x = Date,
                                              y = N), color="orange4") +
              geom_hline(yintercept = mean(data_dow_i$N), color="black", size=.2)
            
            
            
          }
          
          
          
          
        }
        
        if (input$select_timeunit %in% "Weekly") {
          
          data_line <- data_line[!grepl("Mar 28", data_line$Date),]
          
          p <- ggplot(data_line,
                      aes(
                        x = Date %>% substring(1,6),
                        y = N,
                        group = 1
                      )) +
            geom_line(size = 1, color = "orange") +
            geom_point(size = 1, color = "orange") +
            geom_point(data=data_line[as.character(data_line$Date) %in% as.character(input$date_ward),],
                       aes(x = Date %>% substring(1,6),
                           y = N),
                       size = 2.5, pch = 1, color = "forestgreen") +
            labs(
              x = "",
              y = "",
              title = "",
              color = ""
            ) +
            scale_y_continuous(labels = scales::comma, 
                               limits = c(min(data_line$N, na.rm=T), 
                                          max(data_line$N, na.rm =
                                                T))) +
            theme_minimal() +
            theme(plot.title = element_text(hjust = 0.5),
                  axis.text.x = element_text(angle = 45))
        }
        
        
        
        #### To plotly
        ggplotly(p, tooltip = c("Date", "N")) %>%
          layout(legend = list(
            orientation = "h",
            x = 0.4,
            y = -0.2
          )) %>%
          layout(plot_bgcolor='transparent', paper_bgcolor='transparent') %>%
          config(displayModeBar = F)
        
      })
      
      # ** Table ---------------------------------------------------------------
      output$ward_top_5_in <- renderFormattable({
        
        #### Define Colors
        customGreen = "#71CA97"
        customGreen0 = "#DeF7E9"
        customRed = "#ff7f7f"
        customRed0 = "#FA614B66"
        customGreen0 = "#DeF7E9"
        customYellow = "goldenrod2"
        
        #### Grab Data
        ward_data_sp_react <- ward_data_sp_filtered()
        data <- ward_data_sp_react$table_data 
        
        data <- data[!is.na(data$value),]
        
        table_max <- 50
        
        #### Restrict to Province
        if(!is.null(input$select_province)){
          if(!(input$select_province %in% "All")){
            data <- data[data$province %in% input$select_province,]
            table_max <- nrow(data)
          }
        }
        
        #### Prep Data for Table
        data_for_table <- data %>%
          dplyr::select(name, value) %>%
          mutate(value = value %>% round(2)) %>%
          arrange(desc(value)) 
        
        #### Variable names for table
        admin_name <- input$select_unit %>% str_replace_all("s$", "")
        
        if(input$select_variable %in% "Density"){
          var_name <- "Subscribers"
        } else if (input$select_variable %in% "Net Movement"){
          var_name <- "Net Trips"
        } else if (input$select_variable %in% "Median Distance Traveled"){
          var_name <- "Distance"
        } else{
          var_name <- "Trips"
        }
        
        ## Add metric if not count
        if(input$select_metric %in% c("% Change", "Z-Score")){
          var_name <- paste0(var_name, ": ", input$select_metric)
        }
        
        #### Make Table
        f_list <- list(
          `name` = formatter("span", style = ~ style(color = "black")),
          `value` = formatter(
            "span",
            style = x ~ style(
              display = "inline-block",
              direction = "lft",
              font.weight = "bold",
              #"border-radius" = "4px",
              "padding-left" = "2px",
              "background-color" = csscolor(customRed0),
              width = percent(proportion(x)),
              color = csscolor("black")
            )
          )
          
        )
        
        names(f_list)[1] <- admin_name
        names(f_list)[2] <- var_name
        
        names(data_for_table)[1] <- admin_name
        names(data_for_table)[2] <- var_name
        
        l <- formattable(
          data_for_table[1:table_max,],
          align = c("l", "l"),
          f_list
        )
        
        
      })
      
      # ** Totals Figures ------------------------------------------------------
      output$obs_total <- renderPlotly({
        p <- ggplot(data=obs_total, 
                    aes(x=Date, y=Observations)) +
          geom_line(size=1.5, color="black") +
          labs(x="",
               y="",
               title=" \nObservations") +
          theme_minimal() +
          theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
          theme(plot.title = element_text(hjust = 0.5, face="bold", size=16, family="Times"),
                axis.text = element_text(size=12, family="Times")) +
          scale_y_continuous(labels = scales::comma)
        #p
        ggplotly(p) %>% 
          config(displayModeBar = F)
      })
      
      output$subs_total <- renderPlotly({
        p <- ggplot(data=subs_total, 
                    aes(x=Date, y=Subscribers)) +
          geom_line(size=1.5, color="black") +
          labs(x="",
               y="",
               title=" \nSubscribers") +
          theme_minimal() +
          theme(panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
          theme(plot.title = element_text(hjust = 0.5, face="bold", size=16, family="Times"),
                axis.text = element_text(size=12, family="Times")) +
          scale_y_continuous(labels = scales::comma, limits=c(4500000, 5500000))
        ggplotly(p) %>%
          config(displayModeBar = F)
      })
      
      
      # ** Titles --------------------------------------------------------------
      output$map_title <- renderText({
        ward_data_sp_react <- ward_data_sp_filtered()
        title <- ward_data_sp_react$map_title
        
        if(input$select_metric %in% "% Change"){
          title <- paste0("% Change in ", title)
        }
        
        if(input$select_metric %in% "Z-Score"){
          title <- paste0("Z-Score in ", title)
        }
        
        title
        
      })
      
      output$metric_description <- renderText({
        
        out <- ""
        
        if(input$select_metric %in% "% Change"){
          out <- "% Change calculated relevant to baseline values"
        }
        
        if(input$select_metric %in% "Z-Score"){
          out <- "Z-Score is the change in value relevant to average baseline values scaled by the typical deviation in baseline values."
        }
        
        out
        
      })
      
      output$rank_text <- renderText({
        
        out <- ""
        
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% "Density"){
            #out <- "Wards are ranked by the standadized difference in value compared to similar days in February."
            out <- ""
          } else{
            out <- ""
          }
        }
        

        
        out
        
      })
      
      output$table_title <- renderText({
        ward_data_sp_react <- ward_data_sp_filtered()
        paste0(ward_data_sp_react$table_title,
               "<br>",
               ward_data_sp_react$table_subtitle)
      })
      
      output$line_title <- renderText({
        ward_data_sp_react <- ward_data_sp_filtered()
        ward_data_sp_react$line_title
      })
      
      output$map_instructions <- renderText({
        
        out <- ""
        
        if (!is.null(input$select_variable)){
        
        if (input$select_variable %in% "Movement Out of Wards") { 
          out <- "Click a ward on the map to select different origin ward"
        } else if (input$select_variable %in% "Movement Into Wards") {
          out <- "Click a ward on the map to select different destination ward"
        } else if (input$select_variable %in% "Movement Out of Districts") {
          out <- "Click a district on the map to select different destination district"
        } else if (input$select_variable %in% "Movement Into Districts") {
          out <- "Click a district on the map to select different destination district"
        } else{
          out <- ""
        }
          
        }
        
        out
        
      })
      
      #### Line Title Instructions
      output$line_instructions <- renderText({
        
        if(input$select_unit %in% "Wards"){
          out <- "Click a ward on the map to change ward"
        } else if(input$select_unit %in% "Districts"){
          out <- "Click a district on the map to change district"
        } else{
          out <- "Click a ward on the map to change ward"
        }
        
        out
        
      })
      
      #### Line Title Instructions
      output$select_province_title <- renderText({
        
        if(input$select_unit %in% "Wards"){
          out <- "View Wards in Select Province"
        } else if(input$select_unit %in% "Districts"){
          out <- "View Districts in Select Province"
        } else{
          out <- "View Wards in Select Province"
        }
        
        out
        
      })
      
      
      # ** Controls ------------------------------------------------------------
      # Right now movement we have feb-march, other for just feb. adjust controls
      # accordinly
      
      #### Zoom to Region
      
      output$ui_select_region_zoom <- renderUI({
        
        if(input$select_unit %in% "Wards"){
          out <- selectizeInput("select_region_zoom",
                                h5("Zoom to Ward"), 
                                choices = sort(ward_sp$name), 
                                selected = NULL, 
                                multiple = FALSE,
                                options = list(
                                  placeholder = 'Type Ward Name',
                                  onInitialize = I('function() { this.setValue(""); }')
                                )
          )
        }
        
        if(input$select_unit %in% "Districts"){
          out <- selectizeInput("select_region_zoom",
                                h5("Zoom to District"), 
                                choices = sort(district_sp$name), 
                                selected = NULL, 
                                multiple = FALSE,
                                options = list(
                                  placeholder = 'Type District Name',
                                  onInitialize = I('function() { this.setValue(""); }')
                                )
          )
        }
        
        out
        
        
      })
      
      
      
      #### Select Date/Week
      output$ui_select_timeunit <- renderUI({
        
        #### Initialize
        out <- dateInput(
          "date_ward",
          NULL,
          value = "2020-02-01",
          min = "2020-02-01",
          max = "2020-02-31"
        )
        
        
        if (input$select_timeunit %in% "Daily") {
          
          # If a change since baseline metric (not count), then only see March
          if(input$select_metric %in% c("Count")){
            out <- dateInput(
              "date_ward",
              NULL,
              value = "2020-03-01",
              min = "2020-02-01",
              max = "2020-03-31"
            )
          } else{
            out <- dateInput(
              "date_ward",
              NULL,
              value = "2020-03-01",
              min = "2020-03-01",
              max = "2020-03-31"
            )
          }
          
        }
        
        if (input$select_timeunit %in% "Weekly") {
          
          # If a change since baseline metric (not count), then only see March
          if(input$select_metric %in% c("Count")){
            out <-   selectInput(
              "date_ward",
              label = NULL,
              choices = c("Feb 01 - Feb 07",
                          "Feb 08 - Feb 14",
                          "Feb 15 - Feb 21",
                          "Feb 22 - Feb 28",
                          "Feb 29 - Mar 06",
                          "Mar 07 - Mar 13",
                          "Mar 14 - Mar 20",
                          "Mar 21 - Mar 27"),
              multiple = F
            )
          } else{
            out <-   selectInput(
              "date_ward",
              label = NULL,
              choices = c("Feb 29 - Mar 06",
                          "Mar 07 - Mar 13",
                          "Mar 14 - Mar 20",
                          "Mar 21 - Mar 27"),
              multiple = F
            )
          }
          
          
          
        }
        
        out
        
        
      })
      
      #### Select Unit
      output$ui_select_variable <- renderUI({
        
        #### Initialize
        out <- selectInput(
          "select_variable",
          label = h4("Select Variable"),
          choices = c("Density",
                      "Net Movement",
                      #"Median Distance Traveled",
                      "Movement Into Wards",
                      "Movement Out of Wards"),
          multiple = F
        )
        
        if(input$select_unit %in% "Wards"){
          out <- selectInput(
            "select_variable",
            label = h4("Select Variable"),
            choices = c("Density",
                        "Net Movement",
                        #"Median Distance Traveled",
                        "Movement Into Wards",
                        "Movement Out of Wards"),
            multiple = F
          )
        }
        
        if(input$select_unit %in% "Districts"){
          out <- selectInput(
            "select_variable",
            label = h4("Select Variable"),
            choices = c("Density",
                        "Net Movement",
                        #"Median Distance Traveled",
                        "Movement Into Districts",
                        "Movement Out of Districts"),
            multiple = F
          )
        }
        
        out
        
      })
      
      # ** Risk map -----------------------------------------------------------------
      #### Indicator Data
      risk_dist_sp <- reactive({
        
        data <- 
          merge(district_sp, 
                risk_an, 
                by.x = "name",
                by.y = "NAME_2")
        
        
        data[["risk_var"]] <- data[["severe_covid_risk"]]
        if(!is.null(input$select_risk_indicator)){
          # Select variable based on UI input
          data[["risk_var"]] <- data[[risk_an_labs$var[risk_an_labs$group == input$select_risk_indicator]]]
          
        } 
        
        # Return final data
        data
        
      })
      
      #### Movement Data
      risk_map_move_data <- reactive({
        
        #### Grab district
        district_i <- "Harare"
        if (!is.null(input$riskmap_shape_click$id)){
          district_i <- input$riskmap_shape_click$id
        }
        
        move_date_i <- "Feb 15 - Feb 21"
        if (!is.null(input$move_date_risk)){
          move_date_i <- input$move_date_risk
        }
        
        move_type_i <- "Movement Out of"
        if (!is.null(input$move_type_risk)){
          move_type_i <- input$move_type_risk %>% str_replace_all(" Districts", "")
        }
        
        #### Make lines
        dist_o <- district_sp[district_sp$name %in% district_i,]
        
        # https://www.stat.auckland.ac.nz/~paul/Reports/VWline/vwline-intro/power-curve.html
        l_all <- lapply(1:nrow(district_sp), function(i){
          
          N_lines <- 20 # must be even
          
          l <- gcIntermediate(dist_o %>% 
                                coordinates() %>%
                                as.vector(),
                              district_sp[i,] %>% 
                                coordinates %>% 
                                as.vector(),
                              n=N_lines,
                              addStartEnd=TRUE,
                              sp=T)
          
          
          return(l)
        }) %>% do.call(what="rbind")
        
        #### Grab data
        move_df <- readRDS(file.path("data_inputs_for_dashboard",
                                     paste0("Districts_",move_type_i,"_Weekly_",district_i,"_",move_date_i,".Rds")))
        l_all$id <- 1:length(l_all)
        l_all$value <- move_df$value
        
        #### Format Data
        
        l_all$value_alpha <- log(l_all$value + 1)
        l_all$value_alpha <- l_all$value_alpha / max(l_all$value_alpha,na.rm=T)
        l_all$value_weight <- l_all$value_alpha * 6
        
        if(move_type_i %in% "Movement Out of"){
          l_all$label <- paste0(district_i, " to ", district_sp$name, ": ", l_all$value) 
        } else{
          l_all$label <- paste0(district_sp$name , " to ", district_i, ": ", l_all$value) 
        }
        
        
        l_all <- l_all[!is.na(l_all$value),]
        
        #### Return
        l_all
      })
      
      
      
      
      #### Basemap
      output$riskmap <- renderLeaflet({
        
        map_extent <- district_sp %>% extent()
        
        leaflet() %>%
          addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
          fitBounds(
            lng1 = map_extent@xmin,
            lat1 = map_extent@ymin,
            lng2 = map_extent@xmax,
            lat2 = map_extent@ymax
          ) 
      })
      
      #### Main Map
      observe({
        
        #### Prep Movement Data
        move_data <- risk_map_move_data()
        
        pal_move_lines <- colorNumeric(
          palette = "Greys",
          domain = c(-1, move_data$value_alpha), # c(0, map_values)
          na.color = "gray",
          reverse = F
        )
        
        #### Prep Main Map Data
        pal <- 
          colorNumeric(
            palette = "viridis",
            domain = risk_dist_sp()@data$risk_var, # c(0, map_values)
            na.color = "gray",
            reverse = F)
        
        # legend parameters
        leg_labels = sort(unique(risk_dist_sp()@data$risk_var))
        lg_colors = pal(sort(unique(risk_dist_sp()@data$risk_var)))
        
        
        
        #if(is.null(input$select_risk_indicator)){
        #  leafletProxy("riskmap", data = risk_dist_sp()) %>%
        #    clearShapes()  %>% 
        #    addPolygons(
        #      data = risk_dist_sp(),
        #      layerId = ~ name,
        #      label = ~name,
        #      fillColor = ~pal(risk_var),
        #      weight = 2,
        #      opacity = 1,
        #      color = "white",
        #      fillOpacity = 0.7)  %>% 
        #    clearControls() %>% 
        #    addLegend(title = input$select_risk_indicator,
        #              position = 'bottomleft',
        #              colors = lg_colors,
        #              labels = leg_labels)
        #} 
        
        leafletProxy("riskmap", data = risk_dist_sp()) %>%
          
          clearShapes()  %>% 
          
          addPolygons(
            data = risk_dist_sp(),
            label = ~ name,
            layerId = ~ name,
            fillColor = ~pal(risk_var),
            weight = 2,
            opacity = 1,
            color = "white",
            fillOpacity = 0.7,
            labelOptions = labelOptions(
              style = list("font-weight" = "normal",
                           padding = "3px 8px"),
              textsize = "15px",
              direction = "auto"
            ))  %>% 
          
          addPolylines(data = move_data,
                       opacity = ~ sqrt(value_alpha),
                       weight = ~ value_weight,
                       color = ~ pal_move_lines(value_alpha),
                       label = ~ label,
                       group = "Movement",
                       labelOptions = labelOptions(
                         style = list("font-weight" = "normal",
                                      padding = "3px 8px"),
                         textsize = "15px",
                         direction = "auto"
                       )) %>%
          
          
          clearControls() %>% 
          addLegend(title = input$select_risk_indicator,
                    position = 'bottomleft',
                    colors = lg_colors,
                    labels = leg_labels) %>%
          addLayersControl(
            overlayGroups = c("Movement"),
            position = 'bottomleft',
            options = layersControlOptions(collapsed = FALSE)
          )
        
      })
      
      # ** Risk table ----------------------------------------------------------
      
      tab_data <- 
        reactive({
          
          tab_data <- 
            risk_dist_sp()@data %>% 
            dplyr::select("name",
                          "mean_hiv_pop_weighted_cat",
                          "mean_anaemia_pop_weighted_cat",
                          "mean_overweight_pop_weighted_cat",
                          "mean_smoker_pop_weighted_cat",
                          "mean_resp_risk_pop_weighted_cat",
                          "severe_covid_risk") %>% 
            dplyr::rename(District = name)
          
          # Rename other colunmns dimanicly
          names(tab_data)[-1] <- 
            risk_an_labs$group[match(names(risk_an[,21:26]), 
                                     risk_an_labs$var)]
          
          # Return value
          
          tab_data
          
        })
      
      
      
      output$risk_table <- renderDT ({ #renderTable({ 
        tab_data()
      })
      
      # ** Table 2 ---------------------------------------------------------------
      output$risk_table2 <- renderFormattable({
        
        #### Define Colors
        customGreen = "#71CA97"
        customGreen0 = "#DeF7E9"
        customRed = "#ff7f7f"
        customRed0 = "#FA614B66"
        customGreen0 = "#DeF7E9"
        customYellow = "goldenrod2"
     
        
        risk_var_i <- "HIV prevalence quintile"
        if(!is.null(input$select_risk_indicator)){
          if(input$select_risk_indicator %in% "HIV prevalence quintile") risk_var_i <- "mean_hiv_pop_weighted_cat"
          if(input$select_risk_indicator %in% "Anaemia prevalence quintile") risk_var_i <- "mean_anaemia_pop_weighted_cat"
          if(input$select_risk_indicator %in% "Respiratory illness prevalence quintile") risk_var_i <- "mean_resp_risk_pop_weighted_cat"
          if(input$select_risk_indicator %in% "Overweight prevalence quintile") risk_var_i <- "mean_overweight_pop_weighted_cat"
          if(input$select_risk_indicator %in% "Smoking prevalence quintile") risk_var_i <- "mean_smoker_pop_weighted_cat"
          if(input$select_risk_indicator %in% "Severe COVID-19 risk") risk_var_i <- "severe_covid_risk"
        }
        
        risk_an_df <- as.data.frame(risk_an)
        risk_an_i <- risk_an_df[,c("NAME_2",risk_var_i )]
        names(risk_an_i) <- c("name", "value")

        
        #### Prep Data for Table
        data_for_table <- risk_an_i %>%
          dplyr::select(name, value) %>%
          mutate(value = value %>% round(2)) %>%
          arrange(name) %>%
          arrange(desc(value)) 
        
        #### Make Table
        f_list <- list(
          `name` = formatter("span", style = ~ style(color = "black")),
          `value` = formatter(
            "span",
            style = x ~ style(
              display = "inline-block",
              direction = "lft",
              font.weight = "bold",
              #"border-radius" = "4px",
              "padding-left" = "2px",
              "background-color" = csscolor(customRed0),
              width = percent(proportion(x)),
              color = csscolor("black")
            )
          )
          
        )
        
        names(f_list)[1] <- "District"
        names(f_list)[2] <- input$select_risk_indicator
        
        names(data_for_table)[1] <- "District"
        names(data_for_table)[2] <- input$select_risk_indicator
        
        l <- formattable(
          data_for_table[1:60,],
          align = c("l", "l"),
          f_list
        )
        
        l
        
        
      })
      
      
      
      
    }
  })
  
})

# RUN THE APP ==================================================================
shinyApp(ui, server)
