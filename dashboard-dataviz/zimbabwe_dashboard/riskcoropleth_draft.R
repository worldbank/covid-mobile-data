# Draft risk coropleth

library(leaflet)

# Load Spatial Data ------------------------------------------------------

a2 <- readOGR(GEO_ADM2_PATH,
        "ZWE_adm2")

# Risk analysis Data ------------------------------------------------------
ra <- fread(file.path(RISK_ANALYSIS_PATH, 
                      "severe_disease_risk_district.csv"))



# Merge data ------------------------------------------------------

a2 <- merge(a2, ra, by = "NAME_2")

# Leaflet map ------------------------------------------------------


pal1 <- colorBin("YlOrRd", 
                domain = a2$severe_covid_risk,
                bins = c(7, 9, 11, 13, 15, 17, 19, 21) )

pal2 <- colorBin("YlOrRd", 
                 domain = a2$mean_hiv_pop_weighted_cat)



layers <- c("mean_hiv_pop_weighted_cat",       
            "mean_anaemia_pop_weighted_cat",   
            "mean_overweight_pop_weighted_cat",
            "mean_smoker_pop_weighted_cat",    
            "mean_resp_risk_pop_weighted_cat", 
            "severe_covid_risk"  
            )

# Add Layer custom function
add_cus_layer  <- function(map, var,
                           pal = pal2){
  
  pal <- colorBin("YlOrRd", 
                  domain = a2@data[[var]])
  
  addPolygons(map,
    fillColor = ~pal(a2@data[[var]]),
    weight = 2,
    opacity = 1,
    color = "white",
    # dashArray = "3",
    group = var,
    fillOpacity = 0.7)
}


# Set map boundaries
map_extent <- a2 %>% extent()

# Create map
leaflet(a2) %>%
  addProviderTiles(providers$OpenStreetMap.Mapnik) %>%
  fitBounds(
    lng1 = map_extent@xmin,
    lat1 = map_extent@ymin,
    lng2 = map_extent@xmax,
    lat2 = map_extent@ymax
  ) %>% 
  add_cus_layer(var = layers[1]) %>% 
  add_cus_layer(var = layers[2]) %>% 
  add_cus_layer(var = layers[3]) %>% 
  add_cus_layer(var = layers[4]) %>% 
  add_cus_layer(var = layers[5]) %>% 
  add_cus_layer(var = layers[6]) %>% 
  
addLayersControl(
  baseGroups = layers,
  options = layersControlOptions(collapsed = FALSE)
  )
