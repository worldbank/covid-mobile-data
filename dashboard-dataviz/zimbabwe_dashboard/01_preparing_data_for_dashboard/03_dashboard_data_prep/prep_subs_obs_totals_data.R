# Prep Subscribers / Observations Total Data

# Prep datsets for line graphs on about page.

# Subscribers ------------------------------------------------------------------
subs_adm2 <- read.csv(file.path(PANELINDICATORS_PATH,
                                "clean",
                                "i3_2.csv"))

subs_adm2 <- subs_adm2 %>%
  mutate(date = day %>% substring(1,10) %>% as.Date()) %>%
  group_by(date) %>%
  summarise(Subscribers = sum(count, na.rm=T)) %>%
  dplyr::rename(Date = date)

saveRDS(subs_adm2, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,"subscribers_total.Rds"))

# Observations -----------------------------------------------------------------
obs_adm3 <- read.csv(file.path(PANELINDICATORS_PATH,
                                "clean",
                                "i1_3.csv"))

obs_adm3 <- obs_adm3 %>%
  mutate(date = hour %>% substring(1,10) %>% as.Date()) %>%
  group_by(date) %>%
  summarise(Observations = sum(count, na.rm=T)) %>%
  dplyr::rename(Date = date)

saveRDS(obs_adm3, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,"observations_total.Rds"))
