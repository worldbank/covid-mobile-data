# Clean Districts

# Cleans district dataset to be used in Shiny dashboard and to be used in 
# cleaning telecom data.

# (1) Add + standardize variables
# (2) Order by region

# Load Data --------------------------------------------------------------------
district_sp <- readOGR(dsn = file.path(GEO_PATH),
                       layer = "adm2")

# Clean Data -------------------------------------------------------------------

#### Standardize Variable Names
district_sp@data <- district_sp@data %>%
  dplyr::rename(name = NAME_2,
                region = ID_2,
                province = NAME_1) %>%
  dplyr::select(name, region, province)

#### Add Area
district_sp$area <- geosphere::areaPolygon(district_sp) / 1000^2

#### Order by region
district_sp$region <- district_sp$region %>% as.character()
district_sp <- district_sp[order(district_sp$region),]

# Export -----------------------------------------------------------------------
saveRDS(district_sp, file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))
saveRDS(district_sp, file.path(DASHBOARD_DATA_ONEDRIVE_PATH, "districts.Rds"))
