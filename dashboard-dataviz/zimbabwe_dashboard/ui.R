
source("globals.R")

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
                 ),
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
            ),
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
            ),
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
    
    # ** Data Description ------------------------------------------------------
    tabPanel("Data Description",
             fluidRow(column(4,
                             ""),
                      column(
                        4,
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
             #fluidRow(
             #   column(12, align="center",
             #          "World Bank COVID Mobility Analytics Task Force"
             #   )
             # )
    ),
    
    
    # ** Risk analysis ------------------------------------------------------
    tabPanel(
      "Risk analysis",
      # Title and text
      fluidRow(column(7,
                      ""),
               
               column(
                 8,
                 offset = 2,
                 fluidRow(
                   h1("Risk analysis", align = "center"),
                   
                   #h4("Data Sources"),
                   HTML(risk_analysis_text)
                   
                   
                   
                   
                   # h4("Methods"),
                   # data_methods_text
                   
                   
                 )
                 
               ) 
               
              
      ),
      
      # Map and controls
      fluidRow(
        column(12," "),
        column(4,
               align = "center",
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
                 multiple = F)
               ),
        leafletOutput("riskmap"),              
        
      ),
      
      
      # Data table
      fluidRow(
        column(12," "),
        dataTableOutput('risk_table')              
        
      )
      
    )
    
    
  )
)

