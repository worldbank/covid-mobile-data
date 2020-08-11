# i3 Figures

unit <- "wards"

# Load Data --------------------------------------------------------------------
if(unit %in% "wards"){
  CLEAN_DATA_PATH <- CLEAN_DATA_ADM3_PATH
}

if(unit %in% "districts"){
  CLEAN_DATA_PATH <- CLEAN_DATA_ADM2_PATH
}

data <- readRDS(file.path(CLEAN_DATA_PATH, "i5_daily.Rds"))

data_into <- data %>%
  group_by(region_dest, name_dest, date) %>%
  summarise(value = sum(value, na.rm=T)) %>%
  dplyr::rename(region = region_dest,
                name = name_dest)

data_out <- data %>%
  group_by(region_origin, name_origin, date) %>%
  summarise(value = sum(value, na.rm=T)) %>%
  dplyr::rename(region = region_origin,
                name = name_origin)

##
data_into <- data_into %>%
  group_by(region) %>%
  mutate(value_pre = mean(value[date < "2020-03-30"], na.rm = T),
         value_post = mean(value[date > "2020-03-30"], na.rm = T)) %>%
  ungroup() %>%
  mutate(value_change = value_post - value_pre) %>%
  mutate(value_change_rank = rank(value_change))
data_into$value_change_rank[is.na(data_into$value_change)] <- NA

data_out <- data_out %>%
  group_by(region) %>%
  mutate(value_pre = mean(value[date < "2020-03-30"], na.rm = T),
         value_post = mean(value[date > "2020-03-30"], na.rm = T)) %>%
  ungroup() %>%
  mutate(value_change = value_post - value_pre) %>%
  mutate(value_change_rank = rank(value_change))
data_out$value_change_rank[is.na(data_out$value_change)] <- NA


## FIX
data_into <- data_into[!is.na(data_into$date),]
data_into$date <- data_into$date %>% as.Date()

data_out <- data_out[!is.na(data_out$date),]
data_out$date <- data_out$date %>% as.Date()


# Into -------------------------------------------------------------------------
rank_high <- data_into$value_change_rank %>% unique() %>% sort() %>% head(5)

p_high <- data_into %>%
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

rank_low <- data_into$value_change_rank %>% unique() %>% sort() %>% tail(5)

p_low <- data_into %>%
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

p_low

p_all <- ggarrange(p_high, p_low, nrow = 2)
ggsave(p_all, filename = file.path(figures_path, 
                                   paste0(unit, "_netmovement_top_chng.png")),
       height = 5, width=12)



# Out Of -------------------------------------------------------------------------
rank_high <- data_out$value_change_rank %>% unique() %>% sort() %>% head(5)

p_high <- data_out %>%
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

rank_low <- data_out$value_change_rank %>% unique() %>% sort() %>% tail(5)

p_low <- data_out %>%
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

p_low

p_all <- ggarrange(p_high, p_low, nrow = 2)
ggsave(p_all, filename = file.path(figures_path, 
                                   paste0(unit, "_netmovement_top_chng.png")),
       height = 5, width=12)




