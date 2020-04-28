# Draft risk coropleth

library(leaflet)
library(wesanderson)
library(geosphere)



# Load Spatial Data ------------------------------------------------------

a2 <- readOGR(GEO_PATH,
        "ZWE_adm2")

# Risk analysis Data ------------------------------------------------------
ra <- fread(file.path(RISK_ANALYSIS_PATH, 
                      "severe_disease_risk_district.csv"))


# OD Data ------------------------------------------------------

od <- fread(file.path(PROJECT_PATH, 
                      "proof-of-concept/databricks-results/zw/indicator 5/admin2",
                      "origin_destination_connection_matrix_per_day.csv"))

# Merge data ------------------------------------------------------

a2 <- merge(a2, ra, by = "NAME_2")

# Leaflet map ------------------------------------------------------



vars <- c("mean_hiv_pop_weighted_cat",       
            "mean_anaemia_pop_weighted_cat",   
            "mean_overweight_pop_weighted_cat",
            "mean_smoker_pop_weighted_cat",    
            "mean_resp_risk_pop_weighted_cat", 
            "severe_covid_risk"  
            )

groups <- c("HIV prevalence quintile", 
            "Anaemia prevalence quintile",
            "Respiratory illness prevalence quintile",
            "Overweight prevalence quintile", 
            "Smoking prevalence quintile",
            "Severe COVID-19 risk")

group_df <- 
  data.frame(var = vars,
             group = groups,
             stringsAsFactors = F)


# Add Layer custom function
add_cus_layer  <- function(map,
                           var,
                           # pal,
                           gdf = group_df){
  
  pal <- colorBin("YlOrRd", 
                  domain = a2@data[[var]])
  
  nmap <- 
  addPolygons(map,
    fillColor = ~pal(a2@data[[var]]),
    weight = 2,
    opacity = 1,
    color = "white",
    # dashArray = "3",
    group = gdf$group[gdf$var == var],
    fillOpacity = 0.7) #%>% 
    # addLegend(title = gdf$group[gdf$var == var],
    #           position = 'topleft',
    #           colors = pal(1:5),
    #           labels = 1:5,
    #           group = gdf$group[gdf$var == var])
    
  
  return(nmap)
  
}



districts_sp <- readRDS(file.path("data_inputs_for_dashboard",
                     "districts.Rds"))

library(geosphere)

district <- "Harare"

dist_o <- districts_sp[districts_sp$name %in% district,]

l_all <- lapply(1:nrow(districts_sp), function(i){
  print(i)
  l <- gcIntermediate(dist_o %>% 
                        coordinates() %>%
                        as.vector(),
                      districts_sp[i,] %>% 
                        coordinates %>% 
                        as.vector(),
                      n=20,
                      addStartEnd=TRUE,
                      sp=TRUE)
  return(l)
}) %>% do.call(what="rbind")



move_df <- readRDS(file.path("data_inputs_for_dashboard",
                             paste0("Districts_Movement Out of_Weekly_",district,"_Feb 15 - Feb 21.Rds")))
value <- paste("Value: ", move_df$value) 
move_df$value <- log( move_df$value+1, 2)
move_alpha <- move_df$value / max(move_df$value, na.rm=T)
move_weight <- move_alpha*3

pal_ward <- colorNumeric(
  palette = "viridis",
  domain = c(move_df$value), # c(0, map_values)
  na.color = "gray",
  reverse = F
)




leaflet() %>%
  addTiles() %>%
  addPolylines(data=l_all,
               opacity = move_alpha,
               weight=move_weight,
               color = pal_ward(move_df$value),
               label=value)


# Set map boundaries
map_extent <- a2 %>% extent()

# Create map
map <- 
leaflet(a2) %>%
  addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
  fitBounds(
    lng1 = map_extent@xmin,
    lat1 = map_extent@ymin,
    lng2 = map_extent@xmax,
    lat2 = map_extent@ymax
  ) %>% 
  add_cus_layer(var = group_df$var[1]) %>% 
  add_cus_layer(var = group_df$var[2]) %>% 
  add_cus_layer(var = group_df$var[3]) %>% 
  add_cus_layer(var = group_df$var[4]) %>% 
  add_cus_layer(var = group_df$var[5]) %>% 
  add_cus_layer(var = group_df$var[6]) %>% 
  
addLayersControl(
  baseGroups = group_df$group,
  options = layersControlOptions(collapsed = FALSE)
  ) 
