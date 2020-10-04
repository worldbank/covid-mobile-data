# Zimbabwe Mobility Dashboard

##### ******************************************************************** #####
# 1. PACKAGES AND SETUP ========================================================

#### R Shiny Deployment Options
options(rsconnect.max.bundle.files = 400000)
#options(rsconnect.max.bundle.size = 99999999999)

#### Setting directory so will work locally
if (Sys.info()[["user"]] == "WB521633") {
  setwd("C:/Users/wb521633/Documents/Github/covid-mobile-data/dashboard-dataviz/dashboards/zimbabwe"
  )
}

if (Sys.info()[["user"]] == "wb519128") {
  setwd("C:/Users/wb519128/GitHub/covid-mobile-data/dashboard-dataviz/dashboard/zimbabwe"
  )
}

#### Pacakges
library(sparkline)
library(shinydashboard)
library(RColorBrewer)
library(shinythemes)
library(DT)
library(dplyr)
library(rmarkdown)
library(lubridate)
library(shiny)
library(wesanderson)
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
library(htmlwidgets)
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
library(geosphere)
library(openssl)

#### Logged; make false to enable password
Logged = F

# Read encrypted RDS file
readRDS_encrypted <- function(filepath, data_key){
  #unserialize(aes_cbc_decrypt(readRDS(filepath), key = data_key))
  readRDS(filepath)
}

source("functions.R")

##### ******************************************************************** #####
# 2. LOAD DATA AND SET PARAMETERS ==============================================
# Load files that only need to load once at the beginning and set select
# parameters

#### Spatial base layers
ward_sp <- readRDS(file.path("data_inputs_for_dashboard", "wards_aggregated.Rds"))
district_sp <- readRDS(file.path("data_inputs_for_dashboard", "districts.Rds"))

#### Province List for Select Input
provinces <- ward_sp$province %>% unique() %>% sort()
provinces <- c("All", provinces)

#### Risk analysis Data 
risk_an <- fread(file.path("data_inputs_for_dashboard", 
                           "severe_disease_risk_district.csv"))
risk_an_labs <- fread(file.path("data_inputs_for_dashboard", 
                                "severe_disease_risk_district_labels.csv"))

#### Data descriptions
data_methods_text <- read.table("text_inputs/data_methods.txt", sep="{")[[1]] %>% 
  as.character()
data_source_description_text <- read.table("text_inputs/data_source_description.txt", sep="{")[[1]] %>%
  as.character()
risk_analysis_text <- read.table("text_inputs/risk_analysis.txt", sep="{")[[1]] %>% 
  as.character()

#### Weekly Dates: Start of Week
WEEKLY_VALUES_ALL <- c("2020-01-29",
                       "2020-02-05",
                       "2020-02-12",
                       "2020-02-19",
                       "2020-02-26",
                       "2020-03-04",
                       "2020-03-11", 
                       "2020-03-18", 
                       "2020-03-25", 
                       "2020-04-01", 
                       "2020-04-08", 
                       "2020-04-15", 
                       "2020-04-22",
                       "2020-04-29", 
                       "2020-05-06", 
                       "2020-05-13", 
                       "2020-05-20", 
                       "2020-05-27",
                       "2020-06-03",
                       "2020-06-10",
                       "2020-06-17",
                       "2020-06-24")

WEEKLY_VALUES_POST_BASELINE <- WEEKLY_VALUES_ALL[WEEKLY_VALUES_ALL > "2020-03-04"]

#### Dummy Initial Parameters
last_selected_adm <- ""

##### ******************************************************************** #####
# 3. UIs =======================================================================

# ** 3.1 Password UI - - - - - - - - - - - - - - - - - - - - - - - - - - - -----
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
      ),
      htmlOutput("password_warning")
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

# ** 3.2 Main UI - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -----
ui_main <- fluidPage(
  
  h4("WBG - COVID Mobility Analytics Task Force", align="center"),
  
  navbarPage(
    theme = shinytheme("flatly"), # journal
    collapsible = TRUE,
    title = "Zimbabwe",
    
    id = "nav",
    
    # **** 3.2.1 Telecom Data --------------------------------------------------
    tabPanel(
      "Density and Movement",
      tags$head(includeCSS("styles.css")),
      
      dashboardBody(
        fluidRow(
          
          column(2,
                 strong(htmlOutput("metric_description"))),
          
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
            uiOutput("ui_select_metric")
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
            width = 8,
            
            h4(textOutput("map_title"),
               align = "center"),
            
            strong(textOutput("map_instructions"),
                   align = "center"),
            
            column(12, align = "center", htmlOutput("var_definitions")),
            uiOutput("map_spark"),
            leafletOutput("mapward",
                          height = 720),
            
            absolutePanel(
              id = "controls",
              class = "panel panel-default",
              top = 350,
              left = 40,
              width = 220,
              fixed = TRUE,
              draggable = T,
              height = 200,
              align = "center",
              
              h5("Select Date"),
              uiOutput("ui_select_timeunit"),
              
              #uiOutput("ui_select_region_zoom"),
              
              # selectInput(
              #   "select_province",
              #   label = h5(textOutput("select_province_title")),
              #   choices = provinces,
              #   multiple = F
              # ),
              
              column(12, align = "left",
                     textOutput("legend_note_title")
              )
              
            )
            
            
          ),
          column( 
            4,
            wellPanel(
              
              strong(textOutput("line_title"), align = "center"),
              br(),
              fluidRow(column(6, align = "center", offset = 3, uiOutput("ui_select_region_zoom"))),
              #h6(textOutput("line_instructions"), align = "center"),
              plotlyOutput("ward_line_time", height =
                             200),
              
              strong(htmlOutput("table_title"), align = "center"),
              
              div(style = 'height:425px; overflow-y: scroll',
                  htmlOutput("ward_top_5_in")), 
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
    
    # **** 3.2.2 Risk analysis -------------------------------------------------
    tabPanel(
      "Risk Analysis",
      
      dashboardBody(
        fluidRow(
          
          ## Selectors
          column(9,
                 
                 fluidRow(
                   
                   column(4, align = "center",
                          selectInput(
                            "select_risk_indicator",
                            label = h4("Select Indicator"),
                            
                            choices = c("Severe COVID-19 risk",
                                        "HIV prevalence", 
                                        "Anaemia prevalence",
                                        "Respiratory illness prevalence",
                                        "Overweight prevalence", 
                                        "Smoking prevalence"),
                            selected = "Severe COVID-19 risk",
                            multiple = F)
                   ),
                   column(4, align = "center",
                          selectInput(
                            "move_date_risk",
                            label = h4("Movement Date - Week Of:"),
                            choices = WEEKLY_VALUES_ALL,
                            multiple = F)
                   ),
                   column(4, align = "center",
                          selectInput("move_type_risk",
                                      label = h4("Movement Indicator"),
                                      choices = c("Movement Out of Districts",
                                                  "Movement Into Districts"),
                                      multiple = F
                          )
                   )
                   
                 ),
                 
                 fluidRow(
                   column(3,
                          HTML(risk_analysis_text[1]),
                          br(), br(),
                          HTML(risk_analysis_text[2]),
                          br(), br(),
                          HTML(risk_analysis_text[3])
                   ),
                   column(9,
                          column(12, align="center",
                                 strong("Click on a district to change the origin/destination")
                          ),
                          
                          leafletOutput("riskmap",
                                        height = 720)
                   )
                 )
                 
                 
          ),
          column(3, align = "center",
                 wellPanel(
                   h3("District Rankings"),
                   div(style = 'height:720px; overflow-y: scroll',
                       formattableOutput("risk_table"))
                 )
                 
          )
          
          
          
        )
        
      )
      
    ),
    
    # **** 3.2.3 Data Description ----------------------------------------------
    tabPanel("Data Description",
             fluidRow(column(12,
                             ""),
                      column(
                        4,
                        offset = 4,
                        fluidRow(
                          h1("Data Description", align = "center"),
                          
                          h4("Data Sources and Privacy"),
                          data_source_description_text,
                          
                          h4("Methods"),
                          HTML(data_methods_text)
                          
                          
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
    
  ),
  selected = "Dashboard"
)

##### ******************************************************************** #####
# 4. SERVER ====================================================================
server = (function(input, output, session) {
  
  # ** 4.1 Password - - - - - - - - - - - - - - - - - - - - - - - - - - - - -----
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
          
          # Key to unencrypt data
          data_key <<- sha256(charToRaw(Password))
          
          # Load passwords. If wrong password, will return error. This catches
          # error, where if error return empty dataframe
          passwords_df <- tryCatch(
            {
              #readRDS_encrypted("passwords.Rds", data_key)
              
              unserialize(aes_cbc_decrypt(readRDS("passwords.Rds"), key = data_key))
              
            },
            error = function(e){data.frame(NULL)}
          )
          
          
          
          if (Username %in% passwords_df$username) {
            passwords_df_i <- passwords_df[passwords_df$username %in% Username,]
            
            if(checkpw(Password, passwords_df_i$hashed_password) %in% TRUE){
              password_warning <<- "correct"
              
              USER$Logged <- TRUE
              
              #### Totals
              obs_total  <<- readRDS_encrypted(file.path("data_inputs_for_dashboard","observations_total.Rds"),
                                               data_key)
              subs_total <<- readRDS_encrypted(file.path("data_inputs_for_dashboard","subscribers_total.Rds"),
                                               data_key)
              covid_cases <<- readRDS_encrypted(file.path("data_inputs_for_dashboard","covid_cases_districts_centroids.Rds"),
                                                data_key)
              
            } else{
              password_warning <<- "incorrect"
            }
            
          } else{
            password_warning <<- "incorrect"
          }
          
          
          
          
          output$password_warning <- renderText({
            
            out <- ""
            
            if(!is.null(password_warning)){
              
              if(password_warning %in% "incorrect"){
                out <- '<center><h4 style="color:red"><b>Wrong username or password</b></h4></center>'
              } 
              
            }
            
            out
            
          })
          
          
          
          
          
        }
      }
    }
  })
  
  
  
  #### Toggle between UIs (password vs main)
  observe({
    # If not logged in, go to password UI
    if (USER$Logged == FALSE) {
      output$page <- renderUI({
        div(class = "outer", do.call(bootstrapPage, c("", ui_password())))
      })
    }
    
    # If logged in, go to main ui
    if (USER$Logged == TRUE)
    {
      output$page <- renderUI({
        div(ui_main)
      })
      
      # ** 4.2 Reactives - - - - - - - - - - - - - - - - - - - - - - - - - -----
      
      # **** 4.2.1 Basemap Filtering -------------------------------------------
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
      
      
      # **** 4.2.2 Telecom Data Filtering --------------------------------------
      ward_data_sp_filtered <- reactive({
        
        # Update region based on clicking
        if(!is.null(input$mapward_shape_click$id)){
          if(last_selected_adm != input$mapward_shape_click$id){
            
            updateSelectInput(session, "select_region_zoom",
                              selected = input$mapward_shape_click$id
            )
            last_selected_adm <<- input$mapward_shape_click$id
            
          }
        }
        
        # ****** 4.2.2.1 Grab inputs and define defaults -----------------------
        
        #### Grab inputs
        unit_i <- input$select_unit
        variable_i <- input$select_variable 
        timeunit_i <- input$select_timeunit 
        date_i <- input$date_ward
        metric_i <- input$select_metric
        
        #### Define Defaults
        # When shiny starts, defaults are NULL.
        
        ## Main inputs
        if(is.null(unit_i)) unit_i <- "Wards"
        if(is.null(variable_i)) variable_i <- "Density"
        if(is.null(timeunit_i)) timeunit_i <- "Daily"
        if(is.null(date_i)) date_i <- "2020-02-01"
        if(is.null(metric_i)) metric_i <- "Count"
        
        ## Only update ward_i if user has clicked; otherwise, use default
        ward_i <- "Harare 6"
        
        if(!is.null(input$select_region_zoom)){
          ward_i <- input$select_region_zoom
        }
        
        #### Clean Inputs
        # Some variables have names that include the unit (eg, Movement Into
        # District). For remaining code in this section, we rely on these named
        # varsions that don't include the
        variable_i <- variable_i %>% str_replace_all(" Districts| Wards", "") 
        
        # The input value is plural (Districts / Wards), but for some titles
        # we use the singular
        unit_i_singular <- substr(unit_i, 1, nchar(unit_i) - 1)
        
        #### Deal with input switching
        # When some inputs are switched, we need to update another input. 
        
        # Update ward based on unit input If a user switches from Wards to 
        # Districts, a specific ward will still be selected
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
        
        # Accounts for user going from, for example, zscore density to net
        # movement; server would fail as net movement only set up for counts.
        if(variable_i %in% "Net Movement"){
          metric_i <- "Count"
        }
        
        
        # Because date_i is a function of renderUI, updates later which causes
        # a problem when switching time units. Consequently, check the selected
        # time unit and make sure the selected date matches the time unit. For 
        # example, if a "Weekly" is selected, make sure the date inptu is in
        # a week format.
        
        # Make sure is valid week day
        if( (timeunit_i %in% "Weekly") & !(date_i %in% WEEKLY_VALUES_ALL)){
          date_i <- "2020-03-04"
        }
        
        # ****** 4.2.2.2 Density -----------------------------------------------
        if(variable_i %in% c("Density")){
          
          ward_level_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                       paste0(unit_i,"_",
                                                              variable_i, "_",
                                                              timeunit_i, "_",
                                                              date_i,".Rds")),
                                             data_key)
          
          
          time_level_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                       paste0(unit_i,"_",
                                                              variable_i, "_",
                                                              timeunit_i, "_",
                                                              ward_i,".Rds")),
                                             data_key)
          
          
          
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
          
          date_txt <- ""
          if(timeunit_i %in% "Weekly"){
            date_txt <- "Week of "
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
                                 ": ", 
                                 date_txt,
                                 date_i),
            table_subtitle = "",
            line_title = paste0("Trends in Subscribers in ", ward_i)
          )
          
        }
        
        # ****** 4.2.2.3 Net Movement/Dist Travled -----------------------------
        # TODO: Can more cleanly integrtate into Density / Non-OD all together
        if(variable_i %in% c("Net Movement", 
                             "Mean Distance Traveled", 
                             "Std Dev Distance Traveled")){
          
          ward_level_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                       paste0(unit_i,"_",
                                                              variable_i, "_",
                                                              timeunit_i, "_",
                                                              date_i,".Rds")),
                                             data_key)
          
          time_level_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                       paste0(unit_i,"_",
                                                              variable_i, "_",
                                                              timeunit_i, "_",
                                                              ward_i,".Rds")),
                                             data_key)
          
          
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
          
          date_txt <- ""
          if(timeunit_i %in% "Weekly"){
            date_txt <- "Week of "
          }
          
          
          out <- list(
            map_data = map_data,
            table_data = table_data,
            line_data = line_data,
            pal_val_max = max(ward_level_df$value, na.rm = T),
            map_title = paste0(unit_i_singular,
                               " ", variable_i, ": ",
                               date_i),
            table_title = paste0("Top ",
                                 unit_i,
                                 ": ", 
                                 date_txt,
                                 date_i),
            table_subtitle = "",
            line_title = paste0("Trends in ",variable_i," in ", ward_i)
          )
          
        }
        
        # ****** 4.2.2.4 Movement In/Out ---------------------------------------
        if(variable_i %in% c("Movement Into",
                             "Movement Out of")){
          
          ward_level_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                       paste0(unit_i,"_",
                                                              variable_i, "_",
                                                              timeunit_i, "_",
                                                              date_i,".Rds")),
                                             data_key)
          
          time_level_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                       paste0(unit_i,"_",
                                                              variable_i, "_",
                                                              timeunit_i, "_",
                                                              ward_i,".Rds")),
                                             data_key)
          
          ward_time_level_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                            paste0(unit_i,"_",
                                                                   variable_i, "_",
                                                                   timeunit_i, "_",
                                                                   ward_i,"_",
                                                                   date_i, ".Rds")),
                                                  data_key)
          
          
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
          
          date_txt <- ""
          if(timeunit_i %in% "Weekly"){
            date_txt <- "Week of "
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
                                       date_txt,
                                       date_i)) ,
            
            table_title =  ifelse(input$select_variable %in% c("Movement Out of Wards",
                                                               "Movement Out of Districts"),
                                  paste0(unit_i, " with Most Movement Out: ", date_txt, date_i),
                                  paste0(unit_i, " with Most Movement In: ", date_txt, date_i)) ,
            
            table_subtitle = paste0("Total from all ", unit_i),
            
            line_title =  ifelse(input$select_variable %in% c("Movement Out of Wards",
                                                              "Movement Out of Districts"),
                                 paste0("Total Movement out of ", ward_i, " over Time"),
                                 paste0("Total Movement into ", ward_i, " over Time")) 
            
            
          )
          
        }
        
        
        
        out
        
      })
      
      # ** 4.3 Figures - - - - - - - - - - - - - - - - - - - - - - - - - - -----
      
      # **** 4.3.1 Indicator Map -----------------------------------------------
      
      # ****** 4.3.1.1 Map Data ------------------------------------------------
      map_data_list <- reactive({
        
        od_index <- 1
        
        #### Grab polygon
        map_data <- ward_sp_filter()
        
        #### Grab data, and create vector for values and labels
        ward_data_sp_react <- ward_data_sp_filtered()
        map_values <- ward_data_sp_react$map_data$value
        map_labels <- ward_data_sp_react$map_data$html_label
        
        #### If limit to province
        # If user selected a province, limited the values and labels to that 
        # province
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
        
        #### Grab Origin/Destination Index
        # For Movement In/Out, grab the index of the origin/destional region.
        # Needed when adding the origin/destination polygon in red to the map.
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% c("Movement Into Wards",
                                          "Movement Out of Wards",
                                          "Movement Into Districts",
                                          "Movement Out of Districts")){
            
            od_index <- which(grepl("Origin|Destination", map_labels))
            
          }
        }
        
        #### Define observed value with value is 15 or less
        # When value is "masked" (typically 15 or less), decide whether the
        # value should be 0 or NA when shown on map. 0 values are shown as
        # purple and NA values are shown as gray. In cases where the value can 
        # only be positive, masked value should be 0.
        
        ## If metric is % change or z-score, always make masked value NA
        if(!is.null(input$select_metric)){
          if(input$select_metric %in% c("% Change", "Z-Score")){
            
            map_values[grepl("information", map_labels)] <- NA
            
            # If metric is count, only make   
          } else{
            
            if(!is.null(input$select_variable)){
              if(input$select_variable %in% c("Density")){
                map_values[grepl("information", map_labels)] <- 0
              }
              
              if(input$select_variable %in% c("Mean Distance Traveled",
                                              "Std Dev Distance Traveled")){
                map_values[grepl("information", map_labels)] <- NA
                map_labels[grepl("information", map_labels)] <- "information not available"
              }
              
            }
            
            
          }
        }
        
        
        
        
        #### Make outliers less extreme
        # Chop off at percentile
        # TODO: Doesn't work with lots of zeros, so commenting out for now.
        
        N_non_zero <- sum(abs(map_values) > 0, na.rm=T)
        if(N_non_zero > 400){
          q_vals <- quantile(map_values, c(.025,.975), na.rm=T)
          map_values[map_values < q_vals[1]] <- q_vals[1]
          map_values[map_values > q_vals[2]] <- q_vals[2]
        }
        
        
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
        
        #### Cases where all NAs
        # In some cases, all the values will be NA (eg, Movement In/Out % change
        # where there was no movement out of district, and we define masked value
        # as NA). Here, make all values 0.
        if(sum(!is.na(map_values)) %in% 0) map_values <- rep(0, length(map_values))
        
        #### Map Aesthetics
        # For counts, viridis; for % change and z-score, diverging palette for
        # positive/negative. Define counts outside of if statement so has values
        # for initializing.
        
        # Legend color and labels. Not used in map, just to define the legend, 
        # so should mimic what we do with palette applied to map. 
        legend_colors <- rev(viridis(5))
        legend_labels <- c("High", "", "", "", "Low")
        
        # Define pallete
        pal_ward <- colorNumeric(
          palette = "viridis",
          domain = c(map_values), # c(0, map_values)
          na.color = "gray",
          reverse = F
        )
        
        
        if(!is.null(input$select_metric)){
          if(!(input$select_metric %in% "Count")){
            
            wes <- wesanderson::wes_palette("Zissou1", type = "continuous") %>%
              as.vector()
            
            
            legend_colors <- brewer.pal(4, "PuOr") %>% rev()
            legend_labels <- c("Positive", "", "", "Negative")
            
            # Define pallete
            max_value <- map_values[!is.na(map_values)] %>% abs() %>% max()
            
            pal_ward <- colorNumeric(
              palette = "PuOr",
              domain = c(-max_value, max_value), # c(0, map_values)
              na.color = "gray",
              reverse = F
            )
            
          }
        }
        
        
        
        # If all non-NA values are NA, make purple
        if(sum(!is.na(map_values)) == sum(map_values %in% 0)){
          
          map_values <- rep(NA, length(map_values))
          
          # Define pallete
          pal_ward <- colorNumeric(
            palette = "viridis",
            domain = c(0, 0, 0),
            na.color = "#440154FF",
            reverse = F
          )
        }
        
        # Alpha value. Originally had if map is zoomed in (few units), we made 
        # more transparent. As of now not doing that, but including here in 
        # case want to change.
        if (nrow(map_data) > 700) {
          alpha = 1
        } else{
          alpha = 1 # 0.75 fix clear shapes before do this.
        }
        
        covid_cases <- covid_cases[covid_cases$N > 0,]
        
        #covid_cases$N_weight <- log(covid_cases$N + 1)
        #covid_cases$N_weight <- covid_cases$N^(1/1.2)
        covid_cases$N_weight <- covid_cases$N^(1/1.7)*3
        #covid_cases$N_weight <- log(covid_cases$N + 1, base=2) * 7
        
        #### Load sparkline
        #if(!is.null(input$select_unit) & !is.null(input$select_variable) &
        #   !is.null(input$select_timeunit)){
        #  
        #  data_spark <- readRDS(file.path("data_inputs_for_dashboard",
        #                                  paste0("spark_", input$select_unit, "_",input$select_variable,"_",input$select_timeunit, ".Rds")))
        #  spark <- data_spark$l_spark
        #  
        #  map_labels <- paste0(map_labels, "<br>", spark)
        
        #map_labelsa <<- paste0(map_labels, "<br>", spark)
        #map_dataa <<- map_data
        
        # map_dataa$l <- map_labelsa
        # leaflet() %>%
        #   addPolygons(data=map_dataa,
        #               label = lapply(map_labelsa, htmltools::HTML),
        #               popupOptions = popupOptions(minWidth = 200,
        #                                           maxHeight = 150)) %>%
        #   onRender("function(el,x) {
        #     this.on('tooltipopen', function() {HTMLWidgets.staticRender();})
        #   }") %>%
        #   add_deps("sparkline") %>%
        # browsable()
        
        #}
        
        return(list(map_data = map_data,
                    map_labels = map_labels,
                    map_values = map_values,
                    pal_ward = pal_ward,
                    legend_colors = legend_colors,
                    legend_labels = legend_labels,
                    od_index = od_index,
                    alpha = alpha,
                    covid_cases = covid_cases))
        
      })
      
      # ****** 4.3.1.2 Map Without Sparkline -----------------------------------
      
      #### Basemap
      output$mapward <- renderLeaflet({
        
        l <- NULL
        
        if(!is.null(input$select_variable)){
          if(grepl("^Movement", input$select_variable)){
            
            map_sp <- ward_sp_filter()
            map_extent <- map_sp %>% extent()
            
            l <- leaflet() %>%
              addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
              fitBounds(
                lng1 = map_extent@xmin,
                lat1 = map_extent@ymin,
                lng2 = map_extent@xmax,
                lat2 = map_extent@ymax
              ) 
            
          }
        }
        
        l
        
      })
      
      #### Add polygons to map reactively
      observe({
        
        l <- NULL
        
        if(!is.null(input$select_variable)){
          if(grepl("^Movement", input$select_variable)){
            
            map_data_l <- map_data_list()
            
            map_data = map_data_l$map_data
            map_labels = map_data_l$map_labels
            map_values = map_data_l$map_values
            pal_ward = map_data_l$pal_ward
            legend_colors = map_data_l$legend_colors
            legend_labels = map_data_l$legend_labels
            covid_cases = map_data_l$covid_cases
            od_index = map_data_l$od_index
            alpha = map_data_l$alpha
            
            #### Main Leaflet Map 
            l <- leafletProxy("mapward", data = map_data) %>%
              addPolygons(
                label = ~ lapply(map_labels, htmltools::HTML),
                #popupOptions = popupOptions(minWidth = 200,
                #                            maxHeight = 150),
                
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
                    bringToFront = FALSE
                  ),
                
                labelOptions = labelOptions(
                  style = list("font-weight" = "normal",
                               padding = "3px 8px"),
                  textsize = "15px",
                  direction = "auto"
                )
              ) %>%
              addCircles(data = covid_cases,
                         lng = ~longitude,
                         lat = ~latitude,
                         label = ~lapply(label, htmltools::HTML),
                         color = "red",
                         opacity = 1,
                         weight = ~N_weight,
                         labelOptions = labelOptions(
                           style = list("font-weight" = "normal",
                                        padding = "3px 8px"),
                           textsize = "15px",
                           direction = "auto"
                         ),
                         group = "District Level<br>COVID-19 Cases<br><em>As of June 25th</em>") %>%
              clearControls() %>%
              #         onRender("function(el,x) {
              #      this.on('tooltipopen', function() {HTMLWidgets.staticRender();})
              #   }") %>%
              addLegend(
                values = c(map_values), # c(0, map_values)
                colors = legend_colors,
                labels = legend_labels,
                opacity = 0.7,
                title = "Legend",
                position = "topright",
                na.label = "Origin"
              ) %>%
              addLayersControl(
                overlayGroups = c("District Level<br>COVID-19 Cases<br><em>As of June 25th</em>"),
                position = 'topright',
                options = layersControlOptions(collapsed = FALSE)
              ) %>%
              hideGroup("District Level<br>COVID-19 Cases<br><em>As of June 25th</em>")
            
            #### Add Origin/Desintation Polygon in Red
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
            
          }
        }
        
        
        
        
        l #%>%
        #add_deps("sparkline") %>%
        #browsable()
        
      })
      
      # ****** 4.3.1.3 Map With Sparkline --------------------------------------
      output$map_spark <- renderUI({
        
        l <- NULL
        
        if(!is.null(input$select_variable)){
          if(!grepl("^Movement", input$select_variable)){
            
            map_data_l <- map_data_list()
            
            map_data = map_data_l$map_data
            map_labels = map_data_l$map_labels
            map_values = map_data_l$map_values
            pal_ward = map_data_l$pal_ward
            legend_colors = map_data_l$legend_colors
            legend_labels = map_data_l$legend_labels
            covid_cases = map_data_l$covid_cases
            od_index = map_data_l$od_index
            alpha = map_data_l$alpha
            
            #### Load Sparkline
            if(!is.null(input$select_unit) & !is.null(input$select_variable) &
               !is.null(input$select_timeunit)){
              
              data_spark <- readRDS(file.path("data_inputs_for_dashboard",
                                              paste0("spark_", input$select_unit, "_",input$select_variable,"_",input$select_timeunit, "_date",input$date_ward, ".Rds")))
            
              
              map_labels <- paste0(map_labels, "<br><br>", data_spark$l_spark)
              
            }
            
            #### Main Leaflet Map 
            l <- leaflet(height = "700px") %>%
              addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
              addPolygons(data = map_data,
                          label = ~ lapply(map_labels, htmltools::HTML),
                          #popupOptions = popupOptions(minWidth = 200,
                          #                            maxHeight = 150),
                          
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
                              bringToFront = FALSE
                            ),
                          
                          labelOptions = labelOptions(
                            style = list("font-weight" = "normal",
                                         padding = "3px 8px"),
                            textsize = "15px",
                            direction = "auto"
                          )
              ) %>%
              addCircles(data = covid_cases,
                         lng = ~longitude,
                         lat = ~latitude,
                         label = ~lapply(label, htmltools::HTML),
                         color = "red",
                         opacity = 1,
                         weight = ~N_weight,
                         labelOptions = labelOptions(
                           style = list("font-weight" = "normal",
                                        padding = "3px 8px"),
                           textsize = "15px",
                           direction = "auto"
                         ),
                         group = "District Level<br>COVID-19 Cases<br><em>As of June 25th</em>") %>%
              clearControls() %>%
              onRender("function(el,x) {
                this.on('tooltipopen', function() {HTMLWidgets.staticRender();})
             }") %>%
              addLegend(
                values = c(map_values), # c(0, map_values)
                colors = legend_colors,
                labels = legend_labels,
                opacity = 0.7,
                title = "Legend",
                position = "topright",
                na.label = "Origin"
              ) %>%
              addLayersControl(
                overlayGroups = c("District Level<br>COVID-19 Cases<br><em>As of June 25th</em>"),
                position = 'topright',
                options = layersControlOptions(collapsed = FALSE)
              ) %>%
              hideGroup("District Level<br>COVID-19 Cases<br><em>As of June 25th</em>") %>%
              add_deps("sparkline") %>%
              browsable()
            
            #### Add Origin/Desintation Polygon in Red
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
            
          }
        }
        
        
        l 
        
        
      })
      
      # **** 4.3.2 Line Graph --------------------------------------------------
      output$ward_line_time <- renderPlotly({
        
        #### Grab data
        ward_data_sp_react <- ward_data_sp_filtered()
        data_line <- ward_data_sp_react$line_data
        
        # Rename so variables are cleaner in plotly display
        data_line <- data_line %>%
          dplyr::rename(Date = date,
                        N = value)
        
        data_line$Date <- data_line$Date %>% as.character() %>% as.Date()
        
        # Line graph is made slightly differently depending on daily or weekly
        
        # Daily - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        if (input$select_timeunit %in% c("Daily", "Weekly")) {
          
          #### Main Line Graph Element
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
          
          
          #### If % change or baseline, add dots showing baseline values and
          # a line for mean
          if(!is.null(input$select_metric)){
            if(!(input$select_metric %in% "Count")){
              
              dow_i <- input$date_ward %>% as.Date() %>% wday()
              data_dow_i <- data_line[data_line$dow %in% dow_i,] 
              
              data_dow_i <- data_dow_i[month(data_dow_i$Date) %in% 2,]
              
              p <- p + 
                geom_point(data=data_dow_i, aes(x = Date,
                                                y = N), color="orange4") +
                geom_hline(yintercept = mean(data_dow_i$N), color="black", size=.2)
              
            }
          }
          
        }
        
        # Define Plotly - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
        ggplotly(p, tooltip = c("Date", "N")) %>%
          layout(legend = list(
            orientation = "h",
            x = 0.4,
            y = -0.2
          )) %>%
          layout(plot_bgcolor='transparent', paper_bgcolor='transparent') %>%
          config(displayModeBar = F)
        
      })
      
      # **** 4.3.3 Table of Top Areas ------------------------------------------
      
      output$ward_top_5_in <- renderUI({
        
        #### Grab Data
        ward_data_sp_react <- ward_data_sp_filtered()
        data <- ward_data_sp_react$table_data 
        data <- data[!is.na(data$value),]
        
        #### Define Parameters
        customGreen = "#71CA97"
        customGreen0 = "#DeF7E9"
        customRed = "#ff7f7f"
        customRed0 = "#FA614B66"
        customGreen0 = "#DeF7E9"
        customYellow = "goldenrod2"
        
        table_max <- 20
        
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
        
        #### Grab Line Graph Info
        data_for_table <- data_for_table[1:table_max,]
        
        #### Check NA vaues
        data_for_table <- data_for_table[!is.na(data_for_table$name),]
        table_max <- nrow(data_for_table)
        
        if((nrow(data_for_table) > 0) & 
           !is.null(input$select_unit) &
           !is.null(input$select_variable) &
           !is.null(input$select_timeunit)){
          
          #### Add Sparkline
          # https://bl.ocks.org/timelyportfolio/65ba35cec3d61106ef12865326e723e8
          trend_spark <- lapply(1:nrow(data_for_table), function(i){
            df_out <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                                  paste0(input$select_unit,"_",
                                                         input$select_variable %>% str_replace_all(" Districts| Wards", "") , "_",
                                                         input$select_timeunit, "_",
                                                         data_for_table$name[i],".Rds")),
                                        data_key) %>%
              dplyr::mutate(group = i) 
            
            if(input$select_timeunit %in% "Daily"){
              # UNCOMMENT TO ONLY SHOW TREND UNTIL DATE 
              #df_out <- df_out %>%
              #  filter(date <= input$date_ward)
            } else{
              #df_out$Date_short <- df_out$date %>%
              #  as.character() %>%
              #  substring(1,6) %>%
              #  factor(levels = WEEKLY_VALUES_ALL,
              #         ordered = T)
              
              df_out <- df_out %>%
                arrange(Date_short)
            }
            
            return(df_out)
            
          }) %>%
            bind_rows() %>%
            group_by(group) %>%
            summarize(
              TrendSparkline = spk_chr(
                value, 
                type ="line",
                lineColor = 'black', 
                fillColor = "orange", # NA for no fill
                height=40,
                width=100
              )
            )
          
          data_for_table$Trend <- trend_spark$TrendSparkline
        } else{
          # Need to use rep to account for cases where data is nrow=0
          data_for_table$Trend <- rep("", nrow(data_for_table))
        }
        
        #### Variable names for table
        admin_name <- input$select_unit %>% str_replace_all("s$", "")
        
        var_name <- "Density"
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% "Density"){
            var_name <- "Subscribers"
          } else if (input$select_variable %in% "Net Movement"){
            var_name <- "Net Trips"
          } else if (input$select_variable %in% "Mean Distance Traveled"){
            var_name <- "Mean Distance"
          } else if (input$select_variable %in% "Std Dev Distance Traveled"){
            var_name <- "Std Dev Distance"
          } else{
            var_name <- "Trips"
          }
        }
        
        ## Add metric if not count
        if(!is.null(input$select_metric)){
          if(input$select_metric %in% c("% Change", "Z-Score")){
            var_name <- paste0(var_name, ": ", input$select_metric)
          }
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
        
        ### Apply varable names
        names(f_list)[1] <- admin_name
        names(f_list)[2] <- var_name
        
        names(data_for_table)[1] <- admin_name
        names(data_for_table)[2] <- var_name
        
        # Make table
        # https://github.com/renkun-ken/formattable/issues/89
        l <- formattable(
          data_for_table[1:table_max,] %>% as.data.table(),
          align = c("l", "l", "l"),
          f_list
        ) %>% format_table(align = c("l", "l", "l")) %>%
          htmltools::HTML() %>%
          div() %>%
          # use new sparkline helper for adding dependency
          spk_add_deps() %>%
          # use column for bootstrap sizing control
          # but could also just wrap in any tag or tagList
          {column(width=12, .)}
        
        l
        
        
      })
      
      # **** 4.3.4 Total Observations/Subscribers ------------------------------
      #### Total Observations
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
      
      #### Total Subscribers
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
          scale_y_continuous(labels = scales::comma) # limits=c(4500000, 5500000)
        ggplotly(p) %>%
          config(displayModeBar = F)
      })
      
      # **** 4.3.5 Risk Map ----------------------------------------------------
      #### Indicator Data
      risk_dist_sp <- reactive({
        
        data <- 
          merge(district_sp, 
                risk_an, 
                by.x = "name",
                by.y = "NAME_2")
        
        data[["risk_var"]] <- data[["severe_covid_risk_with_age"]]
        
        
        if(!is.null(input$select_risk_indicator)){
          # Select variable based on UI input
          data[["risk_var"]] <- data[[risk_an_labs$var[risk_an_labs$group == input$select_risk_indicator]]]
        } 
        
        # Return final data
        data
        
      })
      
      # O/D Movement Lines - - - - - - - - - - - - - - - - - - - - - - - - - - -
      risk_map_move_data <- reactive({
        
        #### Grab district
        district_i <- "Harare"
        if (!is.null(input$riskmap_shape_click$id)){
          district_i <- input$riskmap_shape_click$id
        }
        
        move_date_i <- "2020-03-04"
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
          
          N_lines <- 15 # must be even
          
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
        move_df <- readRDS_encrypted(file.path("data_inputs_for_dashboard",
                                               paste0("Districts_",move_type_i,"_Weekly_",district_i,"_",move_date_i,".Rds")),
                                     data_key)
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
        
        l_all_arrows <- lapply(1:nrow(l_all), extract_arrows, l_all, 5, move_type_i) %>% 
          do.call(what="rbind")
        
        ## Append
        # Make sure same variables and crs before appending
        for(var in names(l_all)) l_all_arrows[[var]] <- l_all[[var]]
        crs(l_all_arrows) <- crs(l_all)
        
        l_all <- rbind(l_all,
                       l_all_arrows)
        
        #### Return
        l_all
      })
      
      # Basemap - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
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
      
      # Main Map - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
      observe({
        
        req(input$nav == "Risk Analysis") # This makes leaflet show up; before no defaults.
        
        #### Prep Movement Data
        move_data <- risk_map_move_data()
        
        pal_move_lines <- colorNumeric(
          palette = "Greys",
          domain = c(-1, move_data$value_alpha), # c(0, map_values)
          na.color = "gray",
          reverse = F
        )
        
        #### Prep Main Map Data
        risk_map <- risk_dist_sp()
        
        wes <- wesanderson::wes_palette("Zissou1", type = "continuous") %>%
          as.vector()
        
        pal <- 
          colorNumeric(
            palette = wes,
            domain = c(risk_dist_sp()@data$risk_var), # c(0, map_values)
            na.color = "gray",
            reverse = F)
        
        # Needed reversed for legend
        pal_rev <- 
          colorNumeric(
            palette = wes,
            domain = c(risk_dist_sp()@data$risk_var), # c(0, map_values)
            na.color = "gray",
            reverse = T)
        
        # legend parameters
        leg_labels = sort(unique(risk_map@data$risk_var)) %>% rev()
        lg_colors = pal(sort(unique(risk_map@data$risk_var))) %>% rev()
        
        
        map_label <- risk_map@data$name
        
        leafletProxy("riskmap", data = risk_map) %>%
          
          clearShapes()  %>% 
          
          addPolygons(
            data = risk_map,
            label = ~ map_label,
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
                       group = "<b>Movement</b><br>Thicker lines indicate<br>greater movement",
                       labelOptions = labelOptions(
                         style = list("font-weight" = "normal",
                                      padding = "3px 8px"),
                         textsize = "15px",
                         direction = "auto"
                       )) %>%
          
          
          clearControls() %>% 
          addLegend(title = input$select_risk_indicator,
                    pal = pal_rev,
                    values = risk_dist_sp()@data$risk_var,
                    #colors = lg_colors,
                    #labels = leg_labels,
                    labFormat = labelFormat(transform = function(x) sort(x, decreasing = T)),
                    position = 'bottomleft') %>%
          addLayersControl(
            overlayGroups = c("<b>Movement</b><br>Thicker lines indicate<br>greater movement"),
            position = 'bottomleft',
            options = layersControlOptions(collapsed = FALSE)
          )
        
      })
      
      # **** 4.3.6 Risk Table --------------------------------------------------
      output$risk_table <- renderFormattable({
        
        risk_var_i <- "Severe COVID-19 risk"
        if(!is.null(input$select_risk_indicator)){
          if(input$select_risk_indicator %in% "HIV prevalence") risk_var_i <- "mean_hiv_pop_weighted"
          if(input$select_risk_indicator %in% "Anaemia prevalence") risk_var_i <- "mean_anaemia_pop_weighted"
          if(input$select_risk_indicator %in% "Respiratory illness prevalence") risk_var_i <- "mean_resp_risk_pop_weighted"
          if(input$select_risk_indicator %in% "Overweight prevalence") risk_var_i <- "mean_overweight_pop_weighted"
          if(input$select_risk_indicator %in% "Smoking prevalence") risk_var_i <- "mean_smoker_pop_weighted"
          if(input$select_risk_indicator %in% "Severe COVID-19 risk") risk_var_i <- "severe_covid_risk_with_age"
        }
        
        risk_an_df <- as.data.frame(risk_an)
        risk_an_i <- risk_an_df[,c("NAME_2",risk_var_i )]
        names(risk_an_i) <- c("name", "value")
        
        
        #### Prep Data for Table
        data_for_table <- risk_an_i %>%
          dplyr::select(name, value) %>%
          arrange(name) %>%
          arrange(desc(value)) 
        
        if(risk_var_i %in% "severe_covid_risk_with_age"){
          data_for_table$value <- data_for_table$value %>% round(4)
        } else{
          data_for_table$value <- data_for_table$value %>% round(2)
        }
        
        #### Make Table
        f_list <- list(
          `name` = formatter("span", style = ~ style(color = "black")),
          `value` = color_tile2(c("#95C6D3", "#ECDC87", "#EA7E71"))
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
      
      # ** 4.4 Titles - - - - - - - - - - - - - - - - - - - - - - - - - - - -----
      
      # Define titles that dynamically changed depending on inputs
      
      # **** 4.4.1 Map Title ---------------------------------------------------
      output$map_title <- renderText({
        ward_data_sp_react <- ward_data_sp_filtered()
        title <- ward_data_sp_react$map_title
        
        if(!is.null(input$select_metric)){
          if(input$select_metric %in% "% Change"){
            title <- paste0("% Change in ", title)
          }
          
          if(input$select_metric %in% "Z-Score"){
            title <- paste0("Z-Score in ", title)
          }
        }
        
        title
        
      })
      
      # **** 4.4.2 Metric Description ------------------------------------------
      output$metric_description <- renderText({
        
        out <- ""
        
        if(!is.null(input$select_metric)){
          if(input$select_metric %in% "% Change"){
            out <- '<span style="color:red">% Change</span> calculated relevant to baseline values.'
            
          }
          
          if(input$select_metric %in% "Z-Score"){
            out <- '<span style="color:red">Z-Score</span> is the change in value relative to average baseline values scaled by the typical deviation in baseline values.'
          }
        }
        
        
        if(!is.null(input$select_timeunit)){
          
          if(out != ""){
            
            
            if(input$select_timeunit %in% "Daily"){
              out <- paste(out, 'Baseline values are days of the same day of week between February 1 and March 15.')
            }
            
            if(input$select_timeunit %in% "Weekly"){
              out <- paste(out, 'Baseline values are weeks between February 1 and March 15.')
            }
          }
          
        }
        
        out
        
      })
      
      # **** 4.4.3 Text for Top Units Table ------------------------------------
      # Nothing here, but still here in case need to include some text
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
      
      # **** 4.4.4 Table Title -------------------------------------------------
      output$table_title <- renderText({
        ward_data_sp_react <- ward_data_sp_filtered()
        paste0(ward_data_sp_react$table_title,
               "<br>",
               ward_data_sp_react$table_subtitle)
      })
      
      # **** 4.4.5 Line Title --------------------------------------------------
      output$line_title <- renderText({
        ward_data_sp_react <- ward_data_sp_filtered()
        ward_data_sp_react$line_title
      })
      
      # **** 4.4.6 Map Instructions --------------------------------------------
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
      
      # **** 4.4.7 Line Title Instructions -------------------------------------
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
      
      # **** 4.4.8 Select Province Instructions --------------------------------
      
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
      
      output$legend_note_title <- renderText({
        
        out <- "'High' and 'Low' colors are relative to the selected date chosen."
        
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% c("Movement Into Wards",
                                          "Movement Out of Wards")){
            out <- "'High' and 'Low' colors are relative to the selected date and ward chosen."
          }
          
          if(input$select_variable %in% c("Movement Into District",
                                          "Movement Out of District")){
            out <- "'High' and 'Low' colors are relative to the selected date and district chosen."
          }
        }
        
        out
        
      })
      
      # **** 4.4.9 Variable Definitions ----------------------------------------
      output$var_definitions <- renderText({
        
        # Cleanup unit name
        unit <- ""
        unit_upper <- ""
        if(!is.null(input$select_unit)){
          unit_upper <- input$select_unit
          unit <- unit_upper %>% tolower() %>% str_replace_all("s$", "")
        }
        
        # Definition
        out <- ""
        if(!is.null(input$select_variable)){
          
          if(input$select_variable %in% "Density"){
            out <- paste0("<b>Density</b> is the number of subscribers in the ",
                          unit," divided by its area.")
          }
          
          if(input$select_variable %in% "Net Movement"){
            out <- paste0("<b>Net Movement</b> is the number of trips into
                          the ",unit," made by subscribes subtracted by the
                          number of trips out of the ", unit, ".")
          }
          
          if(grepl("Movement Into", input$select_variable)){
            out <- paste0("<b>Movement Into ",unit_upper,"</b> is the number of trips
                           made by subscribers into the ", unit, ".")
            
          }
          
          
          if(grepl("Movement Out of", input$select_variable)){
            out <- paste0("<b>Movement Out of ",unit_upper,"</b> is the number of trips
                           made by subscribers out of the ", unit, ".")
          }
          
          if(input$select_variable %in% "Mean Distance Traveled"){
            out <- paste0("<b>Mean Distance Traveled</b> is the mean distance traveled
                           by subscribers in a", unit, ".")
          }
          
          ## Emphasize
          out <- paste0("<em>",out,"</em>")
          
        } 
        
        out
        
      })
      
      
      # ** 4.5 Controls - - - - - - - - - - - - - - - - - - - - - - - - - -----
      # Define controls that dynamically change based on inputs
      
      # **** 4.5.1 Zoom to Region ----------------------------------------------
      output$ui_select_region_zoom <- renderUI({
        
        if(input$select_unit %in% "Wards"){
          out <- selectizeInput("select_region_zoom",
                                #h5("Select Ward"), 
                                NULL,
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
                                #h5("Select District"), 
                                NULL,
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
      
      # **** 4.5.2 Select Metric -----------------------------------------------
      output$ui_select_metric <- renderUI({
        
        out <- selectInput(
          "select_metric",
          label = h4("Select Metric"),
          choices = c("Count",
                      "% Change",
                      "Z-Score"),
          multiple = F
        )
        
        
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% "Net Movement"){
            out <- selectInput(
              "select_metric",
              label = h4("Select Metric"),
              choices = c("Count"),
              multiple = F
            )
          }
        }
        
        out
        
      })
      
      # **** 4.5.3 Select Date/Week --------------------------------------------
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
          if(!is.null(input$select_metric)){
            
            if(input$select_metric %in% c("Count")){
              
              out <- dateInput(
                "date_ward",
                NULL,
                value = "2020-03-01",
                min = "2020-02-01",
                max = "2020-06-30" # max = "2020-03-29"
              )
              
            } else{
              
              
              out <- dateInput(
                "date_ward",
                NULL,
                value = "2020-03-01",
                min = "2020-03-01",
                max = "2020-06-30" # max = "2020-03-29"
              )
              
              
            }
          }
          
        }
        
        if (input$select_timeunit %in% "Weekly") {
          
          # If a change since baseline metric (not count), then only see March
          if(input$select_metric %in% c("Count")){
            
            out <-   selectInput(
              "date_ward",
              label = NULL,
              choices = WEEKLY_VALUES_ALL,
              
              multiple = F
            )
            
            
          } else{
            
            
            out <-   selectInput(
              "date_ward",
              label = NULL,
              choices = WEEKLY_VALUES_POST_BASELINE,
              
              multiple = F
            )
            
            
            
            
            
            
          }
          
          
          
        }
        
        out
        
        
      })
      
      # **** 4.5.4 Select Unit -------------------------------------------------
      output$ui_select_variable <- renderUI({
        
        #### Initialize
        out <- selectInput(
          "select_variable",
          label = h4("Select Variable"),
          choices = c("Density",
                      "Net Movement",
                      "Movement Into Wards",
                      "Movement Out of Wards",
                      "Mean Distance Traveled"),
          multiple = F
        )
        
        if(input$select_unit %in% "Wards"){
          out <- selectInput(
            "select_variable",
            label = h4("Select Variable"),
            choices = c("Density",
                        "Net Movement",
                        "Movement Into Wards",
                        "Movement Out of Wards",
                        "Mean Distance Traveled"),
            multiple = F
          )
        }
        
        if(input$select_unit %in% "Districts"){
          out <- selectInput(
            "select_variable",
            label = h4("Select Variable"),
            choices = c("Density",
                        "Net Movement",
                        "Movement Into Districts",
                        "Movement Out of Districts",
                        "Mean Distance Traveled"),
            multiple = F
          )
        }
        
        out
        
      })
      
      
    }
  })
  
})

##### ******************************************************************** #####
# 5. RUN THE APP ===============================================================
shinyApp(ui, server)
