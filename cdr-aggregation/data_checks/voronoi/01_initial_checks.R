


# Load Data ------------------------------------------------------------
voronoi <- read_sf(
  file.path(zw_voronoi_data_file_path, 
            "zw_econet_voronoi.geojson")) %>% as("Spatial")


# *** By Region Function =======================================================
by_region_checks_i <- function(region, df, values_var, dates_var){
  
  values <- df[[values_var]][df$region %in% region]
  dates  <- df[[dates_var]][df$region %in% region]
  
  deviation <- values %>% scale() %>% abs()
  
  out_df <- data.frame(region = region,
                       max_dev = deviation %>% max(),
                       N_obs = length(values),
                       N_zeros = sum(values == 0),
                       dates_zero = dates[values == 0] %>% paste(collapse=";"),
                       dates_high_deviation = dates[deviation > 4] %>% paste(collapse=";"))
  return(out_df)
}