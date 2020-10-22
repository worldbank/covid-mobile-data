# Clean ADM2 File

# Load Data --------------------------------------------------------------------
# LOAD DATA HERE

# Subset/Add Variables ---------------------------------------------------------
adm3@data <- adm3@data %>%
  dplyr::select(NAME_3) %>%
  dplyr::rename(name = NAME_3) %>%
  dplyr::mutate(region = name)

adm3$area <- geosphere::areaPolygon(adm3) / 1000^2

# Simplify (to speed up plotting) ----------------------------------------------
# For ms_simplify, polygon IDs and other ID need to match
pid <- sapply(slot(adm3, "polygons"), function(x) slot(x, "ID")) 
row.names(adm3) <- pid

adm3 <- rmapshaper::ms_simplify(adm3)

# Arrange ----------------------------------------------------------------------
#### Order by region
adm3$region <- adm3$region %>% as.character()
adm3 <- adm3[order(adm3$region),]

# Export -----------------------------------------------------------------------
saveRDS(adm3, file.path(GEO_PATH, "adm3.Rds"))
