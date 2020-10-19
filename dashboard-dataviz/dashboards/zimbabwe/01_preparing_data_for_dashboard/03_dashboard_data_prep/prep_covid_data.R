# Prep COVID Cases at District Level

# Load Data --------------------------------------------------------------------
## Covid Cases
covid_df <- read_excel(file.path(PROOF_CONCEPT_PATH,
                                "covid_cases",
                                "Line list 25th June.xlsx"))

## Districts
admin2_sp <- readRDS(file.path(CLEAN_DATA_ADM2_PATH, "districts.Rds"))

# Clean and Summarise COVID Data by Districts ----------------------------------
## Cleanup
names(covid_df) <- covid_df[2,] %>% as.character()

covid_df <- covid_df[-c(1:2),]

covid_df <- covid_df %>%
  dplyr::select(patinfo_resadmin2) %>%
  dplyr::rename(adm2 = patinfo_resadmin2)

## Clean district names
covid_df <- covid_df %>%
  mutate(adm2 = adm2 %>% 
           tolower() %>%
           str_squish())

## Collapse - Bases per district
covid_df <- covid_df %>%
  group_by(adm2) %>%
  dplyr::summarise(N = n())

covid_df <- covid_df[!is.na(covid_df$adm2),]

# Match district names ---------------------------------------------------------
admin2_sp$name <- admin2_sp$name %>% as.character() %>% tolower()

#### Change District Names
admin2_sp$name[admin2_sp$name %in% "gokwe north"] <- "gokwe"
admin2_sp$name[admin2_sp$name %in% "gokwe south"] <- "gokwe"

#### Change COVID Names
covid_df$adm2[covid_df$adm2 %in% "chinhoyi"] <- "makonde" # https://en.wikipedia.org/wiki/Chinhoyi
covid_df$adm2[covid_df$adm2 %in% "karoi"] <- "hurungwe" # https://en.wikipedia.org/wiki/Karoi_District
covid_df$adm2[covid_df$adm2 %in% "macheke"] <- "murehwa" # https://en.wikipedia.org/wiki/Macheke
covid_df$adm2[covid_df$adm2 %in% "mhondoro"] <- "kwekwe" # from mapping [TODO: Check] 
covid_df$adm2[covid_df$adm2 %in% "mt drawin"] <- "mount darwin"
covid_df$adm2[covid_df$adm2 %in% "muzarabani"] <- "centenary" # https://www.mindat.org/feature-894459.html
covid_df$adm2[covid_df$adm2 %in% "norton"] <- "chegutu" # from mapping location
covid_df$adm2[covid_df$adm2 %in% "rafingora"] <- "zvimba" # https://en.wikipedia.org/wiki/Raffingora
covid_df$adm2[covid_df$adm2 %in% "ruwa"] <- "goromonzi" # from mapping location
covid_df$adm2[covid_df$adm2 %in% "sanyati"] <- "kadoma" # https://en.wikipedia.org/wiki/Sanyati
covid_df$adm2[covid_df$adm2 %in% "shamwa"] <- "shamva" # https://en.wikipedia.org/wiki/Shamva_District

#### Collapse/Merge
admin2_sp <- raster::aggregate(admin2_sp, by = "name")

covid_df <- covid_df %>%
  group_by(adm2) %>%
  dplyr::summarise(N = sum(N)) %>%
  dplyr::rename(name = adm2)

admin2_sp <- merge(admin2_sp, covid_df, by = "name", all.x=T, all.y=F)

admin2_sp$N[is.na(admin2_sp$N)] <- 0
admin2_sp$name <- admin2_sp$name %>% tools::toTitleCase()
admin2_sp$label <- paste0("<b>", admin2_sp$name, " District</b><br>",
                          "<b><span style='color:red'>", admin2_sp$N, "</span></b> COVID-19 Cases")

# Export -----------------------------------------------------------------------
saveRDS(admin2_sp, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,"covid_cases_districts.Rds"))

## Save as centroids
admin2_coords_df <- admin2_sp %>% 
  coordinates() %>%
  as.data.frame() %>%
  dplyr::rename(longitude = V1,
                latitude = V2)

admin2_df <- admin2_sp@data
admin2_df <- cbind(admin2_df, admin2_coords_df)

saveRDS(admin2_df, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,"covid_cases_districts_centroids.Rds"))




