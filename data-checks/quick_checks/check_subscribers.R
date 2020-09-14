# Check subscribers data

FIG_PATH <- file.path(PROJECT_PATH, "proof-of-concept",
                      "outputs", "data-checks", "figures_indicators", "subscribers_daily")

# Load Data --------------------------------------------------------------------
ISAAC_DATA_PATH_2 <- file.path(PROJECT_PATH, "Zimbabwe", "Isaac-results", "Isaac_apr_may", "admin2_flowminder")
ISAAC_DATA_PATH_3 <- file.path(PROJECT_PATH, "Zimbabwe", "Isaac-results", "Isaac_apr_may", "admin3_flowminder")

#### Raw Data
df_day_adm2_raw <- read.csv(file.path(ISAAC_DATA_PATH_2,
                                 "count_unique_subscribers_per_region_per_day.csv"),
                           stringsAsFactors=F) %>%
  dplyr::rename(value_raw = subscriber_count,
                date = visit_date) %>%
  dplyr::mutate(region = region %>% as.character(),
                date = date %>% as.Date())

df_week_adm2_raw <- read.csv(file.path(ISAAC_DATA_PATH_2,
                                  "count_unique_subscribers_per_region_per_week.csv"),
                            stringsAsFactors=F) %>%
  dplyr::rename(value_raw = subscriber_count,
                date = visit_week) %>%
  dplyr::mutate(region = region %>% as.character())

df_day_adm3_raw <- read.csv(file.path(ISAAC_DATA_PATH_3,
                                 "count_unique_subscribers_per_region_per_day.csv"),
                           stringsAsFactors=F) %>%
  dplyr::rename(value_raw = subscriber_count,
                date = visit_date) %>%
  dplyr::mutate(region = region %>% as.character(),
                date = date %>% as.Date())

df_week_adm3_raw <- read.csv(file.path(ISAAC_DATA_PATH_3,
                                  "count_unique_subscribers_per_region_per_week.csv"),
                            stringsAsFactors=F) %>%
  dplyr::rename(value_raw = subscriber_count,
                date = visit_week) %>%
  dplyr::mutate(region = region %>% as.character())

#### Cleaned Data
df_day_adm2 <- readRDS(file.path(CLEAN_DATA_ADM2_PATH,
                                 "count_unique_subscribers_per_region_per_day.Rds")) %>%
  left_join(df_day_adm2_raw, by=c("date", "region"))

df_week_adm2 <- readRDS(file.path(CLEAN_DATA_ADM2_PATH,
                                  "count_unique_subscribers_per_region_per_week.Rds"))

df_day_adm3 <- readRDS(file.path(CLEAN_DATA_ADM3_PATH,
                                 "count_unique_subscribers_per_region_per_day.Rds")) %>%
  left_join(df_day_adm3_raw, by=c("date", "region")) %>%
  mutate(value_raw = value_raw %>% as.numeric())

df_week_adm3 <- readRDS(file.path(CLEAN_DATA_ADM3_PATH,
                                  "count_unique_subscribers_per_region_per_week.Rds"))

# Trends Over Time -------------------------------------------------------------
df_day_adm2 %>%
  group_by(date) %>%
  summarise(value = sum(value),
            value_raw = sum(value_raw)) %>%
  ggplot() +
  geom_line(aes(x=date, y=value), color="black") +
  geom_point(aes(x=date, y=value), color="black") +
  geom_vline(xintercept = as.Date("2020-03-27"), color="red")

lapply(unique(df_day_adm3$province), function(province_i){
  print(province_i)
  
  p <- df_day_adm3 %>% 
    filter(province %in% province_i) %>%
    ggplot(aes(x=date)) +
    geom_line(aes(y=value_raw), color="red", alpha=0.2, size=1.5) +
    geom_line(aes(y=value)) +
    facet_wrap(~region,
               scales = "free_y")
  ggsave(p, filename = file.path(FIG_PATH, paste0(province_i, ".png")), height = 25, width = 25)
  
  return(NULL)
})





