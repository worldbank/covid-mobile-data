# i3 Figures

unit <- "wards"

# Load Data --------------------------------------------------------------------
if(unit %in% "wards"){
  CLEAN_DATA_PATH <- CLEAN_DATA_ADM3_PATH
}

if(unit %in% "districts"){
  CLEAN_DATA_PATH <- CLEAN_DATA_ADM2_PATH
}

data <- readRDS(file.path(CLEAN_DATA_PATH, "i5_net_daily.Rds"))

data <- data %>%
  group_by(region) %>%
  mutate(value_pre = mean(value[date < "2020-03-30"], na.rm = T),
         value_post = mean(value[date > "2020-03-30"], na.rm = T)) %>%
  ungroup() %>%
  mutate(value_change = value_post - value_pre) %>%
  mutate(value_change_rank = rank(value_change))

data$value_change_rank[is.na(data$value_change)] <- NA

data <- data[!is.na(data$date),]
data$date <- data$date %>% as.Date()

# Figures ----------------------------------------------------------------------
rank_high <- data$value_change_rank %>% unique() %>% sort() %>% head(5)

p_high <- data %>%
  dplyr::filter(value_change_rank %in% rank_high) %>%
  ggplot(aes(x = date, y = value)) +
  geom_vline(aes(xintercept = "2020-03-30" %>% as.Date()), color="red", alpha = 0.7) +
  geom_line() +
  labs(x = "",
       y = "Number of Subscribers",
       title = "Largest Decreases") +
  facet_wrap(~name,
             scales = "free_y",
             nrow = 1) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 12),
        strip.text.x = element_text(face = "bold"))
p_high

datarank_low <- data$value_change_rank %>% unique() %>% sort() %>% tail(5)

p_low <- data %>%
  dplyr::filter(value_change_rank %in% rank_low) %>%
  ggplot(aes(x = date, y = value)) +
  geom_vline(aes(xintercept = "2020-03-30" %>% as.Date()), color="red", alpha = 0.7) +
  geom_line() +
  labs(x = "",
       y = "",
       title = "Largest Increases") +
  facet_wrap(~name,
             scales = "free_y",
             nrow = 1) +
  theme(plot.title = element_text(hjust = 0.5, face = "bold", size = 12),
        strip.text.x = element_text(face = "bold"))

p_all <- ggarrange(p_high, p_low, nrow = 2)
ggsave(p_all, filename = file.path(figures_path, 
                                   paste0(unit, "_netmovement_top_chng.png")),
       height = 5, width=12)


data$value[data$date < "2020-03-30"] %>% log() %>% hist()
