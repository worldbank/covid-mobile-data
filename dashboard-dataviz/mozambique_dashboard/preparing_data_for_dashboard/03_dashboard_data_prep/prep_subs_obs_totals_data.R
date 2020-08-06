# Prep Subscribers / Observations Total Data

# Prep datsets for line graphs on about page.

# Subscribers ------------------------------------------------------------------
subs_adm2 <- read.csv(file.path(RAW_INDICATORS, paste0("indicator_03_","adm2","_day_result.csv")),
                   stringsAsFactors=F)

subs_adm2 <- subs_adm2 %>%
  mutate(date = pdate %>% substring(1,10) %>% as.Date()) %>%
  group_by(date) %>%
  summarise(Subscribers = sum( total)) %>%
  dplyr::rename(Date = date)

saveRDS(subs_adm2, file.path(DASHBOARD_DATA_ONEDRIVE_PATH,"subscribers_total.Rds"))

