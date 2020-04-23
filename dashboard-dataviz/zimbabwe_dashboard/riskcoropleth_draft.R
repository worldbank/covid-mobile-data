# Draft risk coropleth

library(leaflet)
library(wesanderson)


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



# Set map boundaries
map_extent <- a2 %>% extent()

# Create map
# map <- 
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
