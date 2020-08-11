# Clean ADM2 File

# Load Data --------------------------------------------------------------------
adm3 <- readRDS(file.path(GADM_PATH, "gadm36_MOZ_3_sp.rds"))

# Remove Special Characters ----------------------------------------------------
adm3$NAME_3 <- adm3$NAME_3 %>%
  str_replace_all("ú", "u") %>%
  str_replace_all("ó", "o") %>%
  str_replace_all("è", "e") %>%
  str_replace_all("á", "a") %>%
  str_replace_all("ç", "c") %>%
  str_replace_all("í", "i")

# Deal with Duplicate Names ----------------------------------------------------
adm3$NAME_3[adm3$NAME_2 %in% "Govuro" & adm3$NAME_3 %in% "Save"] <- "Govuro - Save"
adm3$NAME_3[adm3$NAME_2 %in% "Guro" & adm3$NAME_3 %in% "Dacata"] <- "Guro - Dacata"
adm3$NAME_3[adm3$NAME_2 %in% "Machaze" & adm3$NAME_3 %in% "Save"] <- "Machaze - Save"
adm3$NAME_3[adm3$NAME_2 %in% "Mossurize" & adm3$NAME_3 %in% "Dacata"] <- "Mossurize - Dacata"
adm3$NAME_3[adm3$NAME_2 %in% "Namapa" & adm3$NAME_3 %in% "Lurio"] <- "Namapa - Lurio"
adm3$NAME_3[adm3$NAME_2 %in% "Cuamba" & adm3$NAME_3 %in% "Lurio"] <- "Cuamba - Lurio"
adm3$NAME_3[adm3$NAME_2 %in% "Lago" & adm3$NAME_3 %in% "Lago Niassa"] <- "Lago - Lago Niassa"

# Subset/Add Variables ---------------------------------------------------------
adm3@data <- adm3@data %>%
  dplyr::select(NAME_3) %>%
  dplyr::rename(name = NAME_3) %>%
  dplyr::mutate(region = name)

adm3$area <- geosphere::areaPolygon(adm3) / 1000^2

adm3$province <- NA

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
