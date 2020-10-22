# Clean ADM2 File

# Load Data --------------------------------------------------------------------
# LOAD DATA HERE

# Subset/Add Variables ---------------------------------------------------------
adm2@data <- adm2@data %>%
  dplyr::select(NAME_2) %>%
  dplyr::rename(name = NAME_2) %>%
  dplyr::mutate(region = name)

adm2$area <- geosphere::areaPolygon(adm2) / 1000^2

adm2$province <- NA

# Simplify (to speed up plotting) ----------------------------------------------
# For ms_simplify, polygon IDs and other ID need to match
pid <- sapply(slot(adm2, "polygons"), function(x) slot(x, "ID")) 
row.names(adm2) <- pid

adm2 <- rmapshaper::ms_simplify(adm2)

# Arrange ----------------------------------------------------------------------
#### Order by region
adm2$region <- adm2$region %>% as.character()
adm2 <- adm2[order(adm2$region),]

# Export -----------------------------------------------------------------------
saveRDS(adm2, file.path(GEO_PATH, "adm2.Rds"))
