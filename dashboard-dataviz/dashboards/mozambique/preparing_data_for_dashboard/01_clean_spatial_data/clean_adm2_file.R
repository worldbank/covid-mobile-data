# Clean ADM2 File

# Load Data --------------------------------------------------------------------
adm2 <- readRDS(file.path(GADM_PATH, "gadm36_MOZ_2_sp.rds"))

# Remove Special Characters ----------------------------------------------------
adm2$NAME_2 <- adm2$NAME_2 %>%
  str_replace_all("ú", "u") %>%
  str_replace_all("ó", "o") %>%
  str_replace_all("è", "e") %>%
  str_replace_all("á", "a") %>%
  str_replace_all("ç", "c") %>%
  str_replace_all("í", "i")

# Deal with Duplicate Names ----------------------------------------------------
adm2$NAME_2[adm2$NAME_2 %in% "Boane" & adm2$NAME_1 %in% "Maputo"] <- "Maputo - Boane"
adm2$NAME_2[adm2$NAME_2 %in% "Boane" & adm2$NAME_1 %in% "Maputo City"] <- "Maputo City - Boane"

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
