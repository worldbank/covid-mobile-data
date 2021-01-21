# Country Mobility Dashboard

##### ******************************************************************** #####
# 1. PACKAGES AND SETUP ========================================================

#### R Shiny Deployment Options
options(rsconnect.max.bundle.files = 400000)

#### Setting directory so will work locally
if (Sys.info()[["user"]] == "WB521633") {
  setwd("C:/Users/wb521633/Documents/Github/covid-mobile-dashboards/dashboard-dataviz/dashboards/example"
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
#library(wesanderson)
library(ggplot2)
library(tidyr)
library(shinyWidgets)
library(zoo)
library(bcrypt)
library(shinyjs)
library(ngram)
library(stringdist)
library(stringr)
library(rgdal)
library(rgeos)
library(geosphere)
library(htmlwidgets)
library(tidyverse)
library(sf)
library(raster)
library(leaflet)
library(leaflet.extras)
library(plotly)
library(formattable)
library(tidyr)
library(viridis)
library(data.table)
library(htmltools)
library(scales)
library(geosphere)

source("functions.R")

#### Logged; make false to enable password
Logged = F

##### ******************************************************************** #####
# 2. LOAD/PREP DATA ============================================================
# Load files that only need to load once at the beginning.

#### Spatial base layers
adm2_sp <- readRDS(file.path("data_inputs_for_dashboard", "adm3.Rds"))
adm1_name_sp <- readRDS(file.path("data_inputs_for_dashboard", "adm2.Rds"))

#### Province List for Select Input
provinces <- adm2_sp$province %>% unique() %>% sort()
provinces <- c("All", provinces)

#### Totals
obs_total  <- readRDS(file.path("data_inputs_for_dashboard","observations_total.Rds"))
subs_total <- readRDS(file.path("data_inputs_for_dashboard","subscribers_total.Rds"))

#### Data descriptions
data_methods_text <- read.table("text_inputs/data_methods.txt", sep="{")[[1]] %>% 
  as.character()
data_source_description_text <- read.table("text_inputs/data_source_description.txt", sep="{",
                                           encoding = "UTF-8")[[1]] %>%
  as.character()

#### Dummy default parameters on load
last_selected_adm <- ""

#### Weekly Dates: Start of Week
WEEKLY_VALUES_ALL <- c("2020-03-04",
                       "2020-03-11", 
                       "2020-03-18", 
                       "2020-03-25", 
                       "2020-04-01", 
                       "2020-04-08", 
                       "2020-04-15", 
                       "2020-04-22",
                       "2020-04-29")

WEEKLY_VALUES_POST_BASELINE <- WEEKLY_VALUES_ALL[WEEKLY_VALUES_ALL > "2020-03-31"]


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
    title = "Country Name",
    
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
                   choices = c("adm2_name", "adm1_name"),
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
            leafletOutput("mapadm2",
                          height = "700px"),
            
            
            
            absolutePanel(
              id = "controls",
              class = "panel panel-default",
              top = 350,
              left = 40,
              width = 220,
              fixed = TRUE,
              draggable = F,
              height = 200,
              align = "center",
              
              h5("Select Date"),
              uiOutput("ui_select_timeunit"),
              
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
              plotlyOutput("adm2_line_time", height =
                             200),
              
              strong(htmlOutput("table_title"), align = "center"),
              
              div(style = 'height:425px; overflow-y: scroll',
                  htmlOutput("adm2_top_5_in")), 
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
    
    # **** 3.2.3 Data Description ----------------------------------------------
    tabPanel("Data Description",
             fluidRow(column(12,
                             ""),
                      column(
                        4,
                        offset = 4,
                        fluidRow(
                          h1("Data Description", align = "center"),
                          
                          h4("Data Sources"),
                          HTML(data_source_description_text),
                          
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
               column(4, align="left",
                      plotlyOutput("obs_total",
                                   height=350,
                                   width=450)
               ),
               column(4, align="left",
                      plotlyOutput("subs_total",
                                   height=350,
                                   width=450)
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
          
          passwords_df <- readRDS("passwords.Rds")
          
          if (Username %in% passwords_df$username) {
            passwords_df_i <- passwords_df[passwords_df$username %in% Username,]
            
            if(checkpw(Password, passwords_df_i$hashed_password) %in% TRUE){
              password_warning <<- "correct"
              USER$Logged <- TRUE
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
    
    # If not logged in, go to main ui
    if (USER$Logged == TRUE)
    {
      output$page <- renderUI({
        div(ui_main)
      })
      
      # ** 4.2 Reactives - - - - - - - - - - - - - - - - - - - - - - - - - -----
      
      # **** 4.2.1 Basemap Filtering -------------------------------------------
      adm2_sp_filter <- reactive({
        
        #### Default
        if(is.null(input$select_unit)){
          out <- adm2_sp
        } else{
          
          #### Select Admin Unit Level
          if(input$select_unit %in% "adm2_name"){
            admin_sp <- adm2_sp
            out <- admin_sp
          } else if (input$select_unit %in% "adm1_name"){
            admin_sp <- adm1_name_sp
            out <- admin_sp
          } else{
            admin_sp <- adm2_sp
            out <- admin_sp
          }
          
          #### Restrict to province
          #if (!is.null(input$select_province)) {
          #  if (!(input$select_province %in% "All")) {
          #    out <- admin_sp[admin_sp$province %in% input$select_province, ]
          #  }
          #}
          
        }
        
        out
      })
      
      
      # **** 4.2.2 Telecom Data Filtering --------------------------------------
      
      adm2_data_sp_filtered <- reactive({
        
        # Update region based on clicking
        # Clicking only applies to movement
        #if(!is.null(input$select_variable)){
        #  if(grepl("^Movement", input$select_variable)){
            
            if(!is.null(input$mapadm2_shape_click$id)){
              if(last_selected_adm != input$mapadm2_shape_click$id){
                
                updateSelectInput(session, "select_region_zoom",
                                  selected = input$mapadm2_shape_click$id
                )
                last_selected_adm <<- input$mapadm2_shape_click$id
                
              }
            }
        #  }
        #}
        
        # ****** 4.2.2.1 Grab inputs and define defaults -----------------------
        
        #### Grab inputs
        unit_i <- input$select_unit
        variable_i <- input$select_variable 
        timeunit_i <- input$select_timeunit 
        date_i <- input$date_adm2
        metric_i <- input$select_metric
        
        #### Define Defaults
        # When shiny starts, defaults are NULL.
        
        ## Main inputs
        if(is.null(unit_i)) unit_i <- "adm1_name" # "adm2_name"
        if(is.null(variable_i)) variable_i <- "Density"
        if(is.null(timeunit_i)) timeunit_i <- "Weekly" # "Daily"
        if(is.null(date_i)) date_i <- "2020-03-04"
        if(is.null(metric_i)) metric_i <- "Count"
        
        ## Only update adm2_i if user has clicked; otherwise, use default
        adm2_i <- "Cidade De Matola"
        
        if(!is.null(input$select_region_zoom)){
          adm2_i <- input$select_region_zoom
        }
        
        #if (!is.null(input$mapadm2_shape_click$id)){
        #  adm2_i <- input$mapadm2_shape_click$id
        #}
        
        
        # observeEvent(input$select_region_zoom,{
        #   if(!is.null(input$select_region_zoom)){
        #     adm2_i <- input$select_region_zoom
        #   }
        # })
        
        
        
        #### Clean Inputs
        # Some variables have names that include the unit (eg, Movement Into
        # adm1_name). For remaining code in this section, we rely on these named
        # varsions that don't include the
        variable_i <- variable_i %>% str_replace_all(" adm1_name| adm2_name", "") 
        
        # The input value is plural (adm1_name / adm2s), but for some titles
        # we use the singular
        unit_i_singular <- substr(unit_i, 1, nchar(unit_i) - 1)
        
        #### Deal with input switching
        # When some inputs are switched, we need to update another input. 
        
        # Update adm2 based on unit input If a user switches from adm2s to 
        # adm1_name, a specific adm2 will still be selected
        if(unit_i %in% "adm2_name"){
          if(!(adm2_i %in% adm2_sp$name)){
            adm2_i <- "Cidade De Matola"
          }
        }
        
        if(unit_i %in% "adm1_name"){
          if(!(adm2_i %in% adm1_name_sp$name)){
            adm2_i <- "NAME HERE"
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
          
          adm2_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    date_i,".Rds")))
          
          time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    adm2_i,".Rds")))
          
          
          
          if(metric_i %in% "Count"){
            
            map_data <- adm2_level_df %>%
              dplyr::mutate(value = density,
                            html_label = label_level) %>%
              dplyr::select(value, html_label)
            
            table_data <- adm2_level_df %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "% Change"){
            
            map_data <- adm2_level_df %>%
              dplyr::mutate(value = value_perchange_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label)
            
            table_data <- adm2_level_df %>%
              dplyr::mutate(value = value_perchange_base) %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "Z-Score"){
            
            map_data <- adm2_level_df %>%
              dplyr::mutate(value = value_zscore_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label)
            
            table_data <- adm2_level_df %>%
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
            pal_val_max = max(adm2_level_df$value, na.rm = T),
            map_title = paste0(unit_i_singular,
                               " Density: ", 
                               date_i),
            table_title = paste0("Top ",
                                 unit_i,
                                 ": ", 
                                 date_txt,
                                 date_i),
            table_subtitle = "",
            line_title = paste0("Trends in Subscribers in ", adm2_i)
          )
          
        }
        
        # ****** 4.2.2.3 Net Movement/Dist Travled -----------------------------
        # TODO: Can more cleanly integrtate into Density / Non-OD all together
        if(variable_i %in% c("Net Movement", 
                             "Mean Distance Traveled", 
                             "Std Dev Distance Traveled")){
          
          adm2_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    date_i,".Rds")))
          
          time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    adm2_i,".Rds")))
          
          
          if(metric_i %in% "Count"){
            
            map_data <- adm2_level_df %>%
              dplyr::select(value, label_level) %>%
              dplyr::rename(html_label = label_level)
            
            table_data <- adm2_level_df %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df
            
          } else if (metric_i %in% "% Change"){
            
            map_data <- adm2_level_df %>%
              dplyr::select(value_perchange_base, label_base) %>%
              dplyr::rename(value = value_perchange_base,
                            html_label = label_base)
            
            table_data <- adm2_level_df %>%
              dplyr::select(name, value_perchange_base) %>%
              dplyr::rename(value = value_perchange_base)
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "Z-Score"){
            
            map_data <- adm2_level_df %>%
              dplyr::select(value_zscore_base, label_base) %>%
              dplyr::rename(value = value_zscore_base,
                            html_label = label_base)
            
            table_data <- adm2_level_df %>%
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
            pal_val_max = max(adm2_level_df$value, na.rm = T),
            map_title = paste0(unit_i_singular,
                               " ", variable_i, ": ",
                               date_i),
            table_title = paste0("Top ",
                                 unit_i,
                                 ": ", 
                                 date_txt,
                                 date_i),
            table_subtitle = "",
            line_title = paste0("Trends in ",variable_i," in ", adm2_i)
          )
          
        }
        
        # ****** 4.2.2.4 Movement In/Out ---------------------------------------
        if(variable_i %in% c("Movement Into",
                             "Movement Out of")){
          
          adm2_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    date_i,".Rds")))
          
          time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                             paste0(unit_i,"_",
                                                    variable_i, "_",
                                                    timeunit_i, "_",
                                                    adm2_i,".Rds")))
          
          adm2_time_level_df <- readRDS(file.path("data_inputs_for_dashboard",
                                                  paste0(unit_i,"_",
                                                         variable_i, "_",
                                                         timeunit_i, "_",
                                                         adm2_i,"_",
                                                         date_i, ".Rds")))
          
          
          if(metric_i %in% "Count"){
            
            map_data <- adm2_time_level_df %>%
              dplyr::mutate(html_label = label_level) %>%
              dplyr::select(value, html_label) 
            
            table_data <- adm2_level_df %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "% Change"){
            
            map_data <- adm2_time_level_df %>%
              dplyr::mutate(value = value_perchange_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label) 
            
            table_data <- adm2_level_df %>%
              dplyr::mutate(value = value_perchange_base) %>%
              dplyr::select(name, value) 
            
            line_data <- time_level_df 
            
          } else if (metric_i %in% "Z-Score"){
            
            map_data <- adm2_time_level_df %>%
              dplyr::mutate(value = value_zscore_base,
                            html_label = label_base) %>%
              dplyr::select(value, html_label) 
            
            table_data <- adm2_level_df %>%
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
            pal_val_max = max(adm2_level_df$value, na.rm = T),
            map_title =  ifelse(input$select_variable %in% c("Movement Out of adm2_name",
                                                             "Movement Out of adm1_name"),
                                paste0("Number of People Moving from ", 
                                       adm2_i, 
                                       " to other ",
                                       unit_i,
                                       ": ", 
                                       date_i),
                                paste0("Number of People Moving into ", 
                                       adm2_i, 
                                       " from other ",
                                       unit_i,
                                       ": ", 
                                       date_txt,
                                       date_i)) ,
            
            table_title =  ifelse(input$select_variable %in% c("Movement Out of adm2_name",
                                                               "Movement Out of adm1_name"),
                                  paste0(unit_i, " with Most Movement Out: ", date_txt, date_i),
                                  paste0(unit_i, " with Most Movement In: ", date_txt, date_i)) ,
            
            table_subtitle = paste0("Total from all ", unit_i),
            
            line_title =  ifelse(input$select_variable %in% c("Movement Out of adm2_name",
                                                              "Movement Out of adm1_name"),
                                 paste0("Total Movement out of ", adm2_i, " over Time"),
                                 paste0("Total Movement into ", adm2_i, " over Time")) 
            
            
          )
          
        }
        
        out
        
      })
      
      
      # ** 4.3 Figures - - - - - - - - - - - - - - - - - - - - - - - - - - -----
      
      # **** 4.3.1 Indicator Map -----------------------------------------------
      
      # ******* 4.3.1.1 Map Data -----------------------------------------------
      map_data_list <- reactive({
        
        od_index = 1
        
        #### Grab polygon
        map_data <- adm2_sp_filter()
        
        #### Grab data, and create vector for values and labels
        adm2_data_sp_react <- adm2_data_sp_filtered()
        map_values <- adm2_data_sp_react$map_data$value
        map_labels <- adm2_data_sp_react$map_data$html_label
        
        #### Grab Origin/Destination Index
        # For Movement In/Out, grab the index of the origin/destional region.
        # Needed when adding the origin/destination polygon in red to the map.
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% c("Movement Into adm2_name",
                                          "Movement Out of adm2_name",
                                          "Movement Into adm1_name",
                                          "Movement Out of adm1_name")){
            
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
              #if(!(input$select_variable %in% "Net Movement")){
              map_values <- log_neg(map_values)
              #} 
            }
            
          }
        }
        
        #### Cases where all NAs
        # In some cases, all the values will be NA (eg, Movement In/Out % change
        # where there was no movement out of adm1_name, and we define masked value
        # as NA). Here, make all values 0.
        if(sum(!is.na(map_values)) %in% 0) map_values <- rep(0, length(map_values))
        
        #### Map Aesthetics
        # Legend color and labels. Not used in map, just to define the legend, 
        # so should mimic what we do with palette applied to map. 
        legend_colors <- rev(viridis(5))
        legend_labels <- c("High", "", "", "", "Low")
        
        # Define pallete
        pal_adm2 <- colorNumeric(
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
            
            pal_adm2 <- colorNumeric(
              palette = "PuOr", # "PuOr",
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
          pal_adm2 <- colorNumeric(
            palette = "viridis",
            domain = c(0, 0, 0),
            na.color = "#440154FF",
            reverse = F
          )
        }
        
        # Alpha value. Originally had if map is zoomed in (few units), we made 
        # more transparent. As of now not doing that, but including here in 
        # case want to change.
        alpha = 1
        
        # Return
        return(list(map_data = map_data,
                    map_labels = map_labels,
                    map_values = map_values,
                    pal_adm2 = pal_adm2,
                    legend_colors = legend_colors,
                    legend_labels = legend_labels,
                    od_index = od_index,
                    alpha = alpha))
        
      })
      
      # ******* 4.3.1.2 Leaflet Without Sparkline ------------------------------
      #### Basemap
      output$mapadm2 <- renderLeaflet({
        
        l <- NULL
        
        if(!is.null(input$select_variable)){
          if(grepl("^Movement", input$select_variable)){
            
            map_sp <- adm2_sp_filter()
            map_extent <- map_sp %>% extent()
            
            l <- leaflet(height = "1000px") %>%
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
            pal_adm2 = map_data_l$pal_adm2
            legend_colors = map_data_l$legend_colors
            legend_labels = map_data_l$legend_labels
            od_index = map_data_l$od_index
            alpha = map_data_l$alpha
            
            #### Main Leaflet Map 
            l <- leafletProxy("mapadm2", data = map_data) %>%
              #l <- leaflet(height = "720px") %>%
              #  addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
              addPolygons(label = ~ lapply(map_labels, htmltools::HTML),
                          color = ~ pal_adm2(map_values),
                          
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
              # onRender("function(el,x) {
              #       this.on('tooltipopen', function() {HTMLWidgets.staticRender();})
              #    }") %>%
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
            
            #### Add Origin/Desintation Polygon in Red
            if(!is.null(input$select_variable)){
              if(input$select_variable %in% c("Movement Into adm2_name",
                                              "Movement Out of adm2_name",
                                              "Movement Into adm1_name",
                                              "Movement Out of adm1_name")){
                
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
      
      # ******* 4.3.1.3 Leaflet With Sparkline ---------------------------------
      output$map_spark <- renderUI({
        
        l <- NULL
        
        if(!is.null(input$select_variable)){
          if(!grepl("^Movement", input$select_variable)){
            
            #### Load Data
            map_data_l <- map_data_list()
            
            map_data = map_data_l$map_data
            map_labels = map_data_l$map_labels
            map_values = map_data_l$map_values
            pal_adm2 = map_data_l$pal_adm2
            legend_colors = map_data_l$legend_colors
            legend_labels = map_data_l$legend_labels
            od_index = map_data_l$od_index
            alpha = map_data_l$alpha
            
            #### Load Sparkline
            if(!is.null(input$select_unit) & !is.null(input$select_variable) &
               !is.null(input$select_timeunit)){
              
              data_spark <- readRDS(file.path("data_inputs_for_dashboard",
                                              paste0("spark_", input$select_unit, "_",input$select_variable,"_",input$select_timeunit, "_date",input$date_adm2, ".Rds")))
              
              map_labels <- paste0(map_labels, "<br><br>","<center>", "<b>",input$select_variable, "</b>", "<br>", data_spark$l_spark, "</center>")
              
            }
            
            l <- leaflet(height = "700px") %>%
              addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
              addPolygons(data = map_data,
                          label = ~ lapply(map_labels, htmltools::HTML),
                          color = ~ pal_adm2(map_values),
                          
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
              onRender("function(el,x) {
                 this.on('tooltipopen', function() {HTMLWidgets.staticRender();})
              }") %>%
              clearControls() %>%
              addLegend(
                values = c(map_values), # c(0, map_values)
                colors = legend_colors,
                labels = legend_labels,
                opacity = 0.7,
                title = "Legend",
                position = "topright",
                na.label = "Origin"
              ) %>%
              add_deps("sparkline") %>%
              browsable()
            
          } 
        }
        
        l
        
      })
      
      
      
      # **** 4.3.2 Line Graph --------------------------------------------------
      output$adm2_line_time <- renderPlotly({
        
        #### Grab data
        adm2_data_sp_react <- adm2_data_sp_filtered()
        data_line <- adm2_data_sp_react$line_data
        
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
            geom_point(data=data_line[as.character(data_line$Date) %in% as.character(input$date_adm2),],
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
              
              dow_i <- input$date_adm2 %>% as.Date() %>% wday()
              data_dow_i <- data_line[data_line$dow %in% dow_i,] 
              
              data_dow_i <- data_dow_i[month(data_dow_i$Date) %in% 3,]
              
              # p <- p + 
              #   geom_point(data=data_dow_i, aes(x = Date,
              #                                   y = N), color="orange4") +
              #   geom_hline(yintercept = mean(data_dow_i$N), color="black", size=.2)
              
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
      
      output$adm2_top_5_in <- renderUI({
        
        #### Grab Data
        adm2_data_sp_react <- adm2_data_sp_filtered()
        data <- adm2_data_sp_react$table_data 
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
        # if(!is.null(input$select_province)){
        #   if(!(input$select_province %in% "All")){
        #     data <- data[data$province %in% input$select_province,]
        #     table_max <- nrow(data)
        #   }
        # }
        
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
            df_out <- readRDS(file.path("data_inputs_for_dashboard",
                                        paste0(input$select_unit,"_",
                                               input$select_variable %>% str_replace_all(" adm1_name| adm2_name", "") , "_",
                                               input$select_timeunit, "_",
                                               data_for_table$name[i],".Rds"))) %>%
              dplyr::mutate(group = i) 
            
            if(input$select_timeunit %in% "Daily"){
              # df_out <- df_out %>%
              #    filter(date <= input$date_adm2)
            } else{
              df_out$Date_short <- df_out$date %>%
                as.character() %>%
                substring(1,6) %>%
                factor(levels = WEEKLY_VALUES_ALL,
                       ordered = T)
              
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
      
      #### Total Subscribers
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
          scale_y_continuous(labels = scales::comma,
                             limits=c(0, 31000000)) # limits=c(4500000, 5500000)
        
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
          scale_y_continuous(labels = scales::comma,
                             limits=c(0, 31000000)) # limits=c(4500000, 5500000)
        
        ggplotly(p) %>%
          config(displayModeBar = F)
      })
      
      
      # ** 4.4 Titles - - - - - - - - - - - - - - - - - - - - - - - - - - - -----
      
      # Define titles that dynamically changed depending on inputs
      
      # **** 4.4.1 Map Title ---------------------------------------------------
      output$map_title <- renderText({
        adm2_data_sp_react <- adm2_data_sp_filtered()
        title <- adm2_data_sp_react$map_title
        
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
              out <- paste(out, 'Baseline values are same types of days (weekdays or weekends) in March.')
            }
            
            if(input$select_timeunit %in% "Weekly"){
              out <- paste(out, 'Baseline values are weeks in March.')
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
            #out <- "adm2s are ranked by the standadized difference in value compared to similar days in February."
            out <- ""
          } else{
            out <- ""
          }
        }
        
        out
        
      })
      
      # **** 4.4.4 Table Title -------------------------------------------------
      output$table_title <- renderText({
        adm2_data_sp_react <- adm2_data_sp_filtered()
        paste0(adm2_data_sp_react$table_title,
               "<br>",
               adm2_data_sp_react$table_subtitle)
      })
      
      # **** 4.4.5 Line Title --------------------------------------------------
      output$line_title <- renderText({
        adm2_data_sp_react <- adm2_data_sp_filtered()
        adm2_data_sp_react$line_title
      })
      
      # **** 4.4.6 Map Instructions --------------------------------------------
      output$map_instructions <- renderText({
        
        out <- ""
        
        if (!is.null(input$select_variable)){
          
          if (input$select_variable %in% "Movement Out of adm2_name") { 
            out <- "Click a adm2_name on the map to select different origin adm2_name"
          } else if (input$select_variable %in% "Movement Into adm2_name") {
            out <- "Click a adm2_name on the map to select different destination adm2_name"
          } else if (input$select_variable %in% "Movement Out of adm1_name") {
            out <- "Click a adm1_name on the map to select different destination adm1_name"
          } else if (input$select_variable %in% "Movement Into adm1_name") {
            out <- "Click a adm1_name on the map to select different destination adm1_name"
          } else{
            out <- ""
          }
          
        }
        
        out
        
      })
      
      # **** 4.4.7 Line Title Instructions -------------------------------------
      output$line_instructions <- renderText({
        
        if(input$select_unit %in% "adm2_name"){
          out <- "Click a adm2_name on the map to change adm2_name"
        } else if(input$select_unit %in% "adm1_name"){
          out <- "Click a adm1_name on the map to change adm1_name"
        } else{
          out <- "Click a adm2_name on the map to change adm2_name"
        }
        
        out
        
      })
      
      # **** 4.4.8 Select Province Instructions --------------------------------
      
      # output$select_province_title <- renderText({
      #   
      #   if(input$select_unit %in% "adm2_name"){
      #     out <- "View adm2s in Select Province"
      #   } else if(input$select_unit %in% "adm1_name"){
      #     out <- "View adm1_name in Select Province"
      #   } else{
      #     out <- "View adm2s in Select Province"
      #   }
      #   
      #   out
      #   
      # })
      
      output$legend_note_title <- renderText({
        
        out <- "'High' and 'Low' colors are relative to the selected date chosen."
        
        if(!is.null(input$select_variable)){
          if(input$select_variable %in% c("Movement Into adm2_name",
                                          "Movement Out of adm2_name")){
            out <- "'High' and 'Low' colors are relative to the selected date and adm2_name chosen."
          }
          
          if(input$select_variable %in% c("Movement Into adm1_name",
                                          "Movement Out of adm1_name")){
            out <- "'High' and 'Low' colors are relative to the selected date and adm1_name chosen."
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
        
        if(input$select_unit %in% "adm2_name"){
          out <- selectizeInput("select_region_zoom",
                                #h5("Select adm2_name"), 
                                NULL,
                                choices = sort(adm2_sp$name), 
                                selected = NULL, 
                                multiple = FALSE,
                                options = list(
                                  placeholder = 'Type adm2_name Name',
                                  onInitialize = I('function() { this.setValue(""); }')
                                )
          )
        }
        
        if(input$select_unit %in% "adm1_name"){
          out <- selectizeInput("select_region_zoom",
                                #h5("Select adm1_name"), 
                                NULL,
                                choices = sort(adm1_name_sp$name), 
                                selected = NULL, 
                                multiple = FALSE,
                                options = list(
                                  placeholder = 'Type adm1_name Name',
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
              choices = c("Count",
                          "% Change",
                          "Z-Score"),
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
          "date_adm2",
          NULL,
          value = "2020-03-15",
          min = "2020-03-01",
          max = "2020-04-30"
        )
        
        
        if (input$select_timeunit %in% "Daily") {
          
          # If a change since baseline metric (not count), then only see March
          if(!is.null(input$select_metric)){
            
            if(input$select_metric %in% c("Count")){
              
              out <- dateInput(
                "date_adm2",
                NULL,
                value = "2020-03-15",
                min = "2020-03-01",
                max = "2020-04-30" # max = "2020-03-29"
              )
              
            } else{
              
              
              out <- dateInput(
                "date_adm2",
                NULL,
                value = "2020-04-01",
                min = "2020-04-01",
                max = "2020-04-30" # max = "2020-03-29"
              )
              
              
            }
          }
          
        }
        
        if (input$select_timeunit %in% "Weekly") {
          
          # If a change since baseline metric (not count), then only see March
          if(input$select_metric %in% c("Count")){
            
            out <-   selectInput(
              "date_adm2",
              label = NULL,
              choices = WEEKLY_VALUES_ALL,
              
              multiple = F
            )
            
            
          } else{
            
            
            out <-   selectInput(
              "date_adm2",
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
                      "Movement Into adm2_name",
                      "Movement Out of adm2_name",
                      "Mean Distance Traveled"),
          multiple = F
        )
        
        if(input$select_unit %in% "adm2_name"){
          out <- selectInput(
            "select_variable",
            label = h4("Select Variable"),
            choices = c("Density",
                        "Net Movement",
                        "Movement Into adm2_name",
                        "Movement Out of adm2_name",
                        "Mean Distance Traveled"),
            multiple = F
          )
        }
        
        if(input$select_unit %in% "adm1_name"){
          out <- selectInput(
            "select_variable",
            label = h4("Select Variable"),
            choices = c("Density",
                        "Net Movement",
                        "Movement Into adm1_name",
                        "Movement Out of adm1_name",
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
