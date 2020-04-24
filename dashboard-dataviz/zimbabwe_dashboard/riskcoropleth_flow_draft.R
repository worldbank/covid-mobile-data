

# district centroids

ct_coords <- gCentroid(a2, byid = T)@coords

ct_df <- data.frame(ID_2 = a2$ID_2,
                    lon = ct_coords[,2],
                    lat = ct_coords[,1],
                    stringsAsFactors = F)

a2 <- merge(a2, ct_df, by = "ID_2")
a2$ID_2 <- as.integer(as.character(a2$ID_2))

# Merge coords to OD matrix
od <- od %>% 
  merge( a2@data[,c("ID_2",
                    "lat",
                    "lon")], 
         by.x = "region_from",
         by.y = "ID_2") %>% rename(lat_O = lat,
                                   lon_O = lon) %>% 
  merge( a2@data[,c("ID_2",
                    "lat",
                    "lon")], 
         by.x = "region_to",
         by.y = "ID_2") %>% rename(lat_D = lat,
                                   lon_D = lon)

od$date <- od$connection_date  %>% as.Date()


od42 <- od %>% subset(region_from == "42" & date == "2020-03-08")


# Create flow objects
flows <- gcIntermediate(
  od42[,c("lat_O", "lon_O")], 
  od42[,c("lat_D", "lon_D")], sp = TRUE, addStartEnd = TRUE)

flows$counts <- od42$total_count
flows$origins <- od42$region_to
flows$destinations <- od42$region_from

flows <- spTransform(flows, a2@proj4string)

hover <- paste0(flows$origins, " to ", 
                flows$destinations, ': ', 
                as.character(flows$counts))

pal <- colorFactor(brewer.pal(4, 'Set2'), flows$origins)

map %>%
  addPolylines(data = flows, 
               weight = ~log(counts), 
               label = hover, 
               group = ~origins, 
               opacity = .9,
               color = "gray")


ggplot(a2) +
  geom_polygon(aes(y = lat,
                   x = long,
                   group = group),
               fill = "white",
               col = "black") +
  geom_path(data = flows,
            aes(y = lat,
                x = long))












