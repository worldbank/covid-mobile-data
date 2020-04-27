# Globals ======================================================================


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

#### Data descriptions
data_methods_text <- read.table("data_methods.txt", sep="{")[[1]] %>% 
  as.character()
data_source_description_text <- read.table("data_source_description.txt", sep="{")[[1]] %>%
  as.character()

#### Risk analysis text
risk_analysis_text <- read.table("risk_analysis.txt", sep="{")[[1]] %>% 
  as.character()

risk_analysis_text <- paste(risk_analysis_text[1],
                            risk_analysis_text[2],
                            sep = "<br>")


#### Default parameters on load
unit_i <- "Wards"
variable_i <- "Density"
timeunit_i <- "Daily"
date_i <- "2020-02-01"
previous_zoom_selection <- ""
metric_i <- "Count"
