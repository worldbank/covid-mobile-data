# Prep Subscribers / Observations Total Data

# Prep datsets for line graphs on about page.

# Subscribers ------------------------------------------------------------------
subs_adm2 <- read.csv(file.path(RAW_DATA_ADM2_PATH, 
                                "count_unique_subscribers_per_region_per_day.csv"))

subs_adm2 <- subs_adm2 %>%
  mutate(date = visit_date %>% substring(1,10) %>% as.Date()) %>%
  group_by(date) %>%
  summarise(Subscribers = sum(subscriber_count)) %>%
  dplyr::rename(Date = date)

saveRDS(subs_adm2, file.path(DASHBOARD_DATA_PATH,"subscribers_total.Rds"))

# Observations -----------------------------------------------------------------
obs_adm2 <- read.csv(file.path(RAW_DATA_ADM2_PATH, 
                               "total_calls_per_region_per_day.csv"))

obs_adm2 <- obs_adm2 %>%
  mutate(date = call_date %>% substring(1,10) %>% as.Date()) %>%
  group_by(date) %>%
  summarise(Observations = sum(total_calls)) %>%
  dplyr::rename(Date = date)

saveRDS(obs_adm2, file.path(DASHBOARD_DATA_PATH,"observations_total.Rds"))