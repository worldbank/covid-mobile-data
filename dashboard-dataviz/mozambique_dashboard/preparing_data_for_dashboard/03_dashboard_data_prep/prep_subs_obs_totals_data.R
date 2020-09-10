# Prep Subscribers / Observations Total Data

# Prep datsets for line graphs on about page.

# Subscribers ------------------------------------------------------------------
subs_adm2 <- read.csv(file.path(RAW_INDICATORS, paste0("indicator_01_02_adm3_hour_result.csv")),
                   stringsAsFactors=F)

subs_adm2 <- subs_adm2 %>%
  group_by(pdate) %>%
  dplyr::summarise(Subscribers = sum(totalimei)) %>%
  dplyr::rename(Date = pdate) %>%
  mutate(Date = Date %>% ymd)

saveRDS(subs_adm2, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,"subscribers_total.Rds"))

# Observations -----------------------------------------------------------------
obs_adm2 <- read.csv(file.path(RAW_INDICATORS, paste0("indicator_01_02_adm3_hour_result.csv")),
                      stringsAsFactors=F)

obs_adm2 <- obs_adm2 %>%
  group_by(pdate) %>%
  dplyr::summarise(Observations = sum(total)) %>%
  dplyr::rename(Date = pdate) %>%
  mutate(Date = Date %>% ymd)

saveRDS(obs_adm2, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,"observations_total.Rds"))
