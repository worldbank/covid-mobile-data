# Check subscribers data

FIG_PATH <- file.path(PROJECT_PATH, "proof-of-concept",
                      "outputs", "data-checks", "figures_indicators", "subscribers_neighbors_daily")

FIG_PATH_OUTLIER <- file.path(PROJECT_PATH, "proof-of-concept",
                              "outputs", "data-checks", "figures_indicators", "subscribers_neighbors_daily_outlier")

# Load Data --------------------------------------------------------------------
ISAAC_DATA_PATH_2 <- file.path(PROJECT_PATH, "Zimbabwe", "Isaac-results", "Isaac_apr_may", "admin2_flowminder")
ISAAC_DATA_PATH_3 <- file.path(PROJECT_PATH, "Zimbabwe", "Isaac-results", "Isaac_apr_may", "admin3_flowminder")

#### Wards
wards_sp <- readRDS(file.path(CLEAN_DATA_ADM3_PATH, "wards_aggregated.Rds"))

#### Tower down
towers_down <- read.csv(file.path(PROOF_CONCEPT_PATH, 
                                  "outputs", 
                                  "data-checks", 
                                  "days_wards_with_low_hours_I1_panel.csv"))

towers_down <- towers_down %>%
  dplyr::select(region, date) %>%
  mutate(tower_down = T) %>%
  mutate(date = date %>% as.character %>% as.Date(),
         region = region %>% as.character())

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

# Create Ward Neighbors --------------------------------------------------------
#### Region and id datasets
ward_id_df <- wards_sp@data %>%
  dplyr::select(region) %>%
  mutate(id = 1:n())

#### Create neighbor matrix
neighbor_df <- gTouches(wards_sp, byid=TRUE) %>%
  as.data.frame() %>%
  mutate(id = 1:n()) %>%
  pivot_longer(-id) %>%
  dplyr::rename(n_id = name,
                neighbors = value) %>%
  dplyr::mutate(n_id = n_id %>% as.numeric()) %>%
  
  # id_n (neighbor) region
  left_join(ward_id_df, by = c("n_id" = "id")) %>%
  dplyr::rename(n_region = region) %>%
  
  # id region
  left_join(ward_id_df, by = "id") %>%
  
  # restrict to neighbors
  filter(neighbors %in% T)

#### Merge data to neighbor matrix
ward_data <- df_day_adm3 %>%
  dplyr::select(region, date, value, value_raw)

neighbor_df <- neighbor_df %>%
  
  # neighbor data
  left_join(ward_data, by = c("n_region" = "region")) %>%
  dplyr::rename(value_n = value,
                value_raw_n = value_raw) %>%
  
  # ward data
  left_join(ward_data, by = c("region", "date"))

#### Merge in Neighbor down
neighbor_df <- neighbor_df %>%
  left_join(towers_down, by = c("region", "date")) %>%
  
  # tower down on any day?
  group_by(region) %>%
  mutate(tower_down_anyday = (TRUE %in% tower_down)) %>%
  
  # restrict to observations where tower down on any day
  filter(tower_down_anyday %in% T)

#### Merge in province
prov_df <- wards_sp@data %>%
  dplyr::select(region, province) 

neighbor_df <- neighbor_df %>%
  left_join(prov_df, by="region")

# Neighbor Stats ---------------------------------------------------------------
# TODO: Not naming things well, should be value_n_raw_avg, for example
#### Average neighbor value
neighbor_df <- neighbor_df %>%
  group_by(region, date) %>%
  mutate(value_n_avg = mean(value_raw_n, na.rm=T))

#### Percen change of neighbor value from average
neighbor_df <- neighbor_df %>%
  group_by(n_region) %>%
  mutate(region_n_value_avg = mean(value_raw_n, na.rm=T)) %>%
  mutate(region_n_value_pc = (value_raw_n - region_n_value_avg)/region_n_value_avg) %>%
  mutate(region_n_value_pc_max = max(region_n_value_pc, na.rm=T))

# Export Datset ----------------------------------------------------------------
#neighbor_df_clean <- neighbor_df %>%
#  dplyr::select(region, n_region, date, value_n)

#head(neighbor_df)



# Trends Over Time -------------------------------------------------------------
neighbor_df %>% 
  filter(id %in% 10) %>%
  ggplot() +
  geom_vline(data = . %>% filter(tower_down), aes(xintercept = date),
             color = "gray50", size=2, alpha = 0.2) +
  geom_line(aes(x=date, y=value_raw_n, 
                group=n_id %>% as.factor(), 
                color=n_id %>% as.factor())) +
  geom_line(aes(x=date, y=value_raw), size=2, color="black") +
  theme_minimal() +
  theme(legend.position = "none")


lapply(unique(neighbor_df$province), function(province_i){
  print(province_i)
  
  p <- neighbor_df %>% 
    filter(province %in% province_i) %>%
    ggplot() +
    geom_vline(data = . %>% filter(tower_down), aes(xintercept = date),
               color = "gray50", size=2, alpha = 0.2) +
    geom_line(aes(x=date, y=value_raw), size=1.5, color="black") +
    geom_line(aes(x=date, y=value_n_avg), size=1.5, color="red") +
    geom_line(aes(x=date, y=value_raw_n, 
                  group=n_id %>% as.factor(), 
                  color=n_id %>% as.factor()),
              size=.4) +
    theme_minimal() +
    theme(legend.position = "none") +
    facet_wrap(~region,
               scales = "free_y")
  
  ggsave(p, filename = file.path(FIG_PATH, paste0(province_i, ".png")), height = 25, width = 25)
  
  return(NULL)
})

# Bad Cases -------------------------------------------------------------
for(percent in c(50, 75, 100)){
  
  print(percent)
  
  neighbor_df_bad <- neighbor_df %>%
    mutate(keep = (tower_down %in% TRUE) & (region_n_value_pc > percent/100)) %>%
    group_by(region) %>%
    mutate(keep_any = (TRUE %in% keep)) %>%
    ungroup() %>%
    filter(keep_any %in% TRUE) %>%
    filter(region_n_value_pc_max > percent/100)
  
  p_bad <- neighbor_df_bad %>%
    ggplot() +
    geom_vline(data = . %>% filter(tower_down), aes(xintercept = date),
               color = "gray50", size=2, alpha = 0.2) +
    geom_line(aes(x=date, y=value_raw), size=1.75, color="black") +
    geom_line(aes(x=date, y=value_raw_n, 
                  group=n_id %>% as.factor(), 
                  color=n_id %>% as.factor()),
              size=1) +

    #geom_line(aes(x=date, y=value_n_avg), size=1.5, color="red") +
    theme_minimal() +
    theme(legend.position = "none") +
    facet_wrap(~region,
               scales = "free_y")
  
  ggsave(p_bad, filename = file.path(FIG_PATH_OUTLIER, paste0(percent, "percent_thresh.png")), height = 25, width = 25)
}


