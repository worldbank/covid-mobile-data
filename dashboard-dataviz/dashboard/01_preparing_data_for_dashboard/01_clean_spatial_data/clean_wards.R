# Clean Wards

# Cleans ward dataset to be used in Shiny dashboard and to be used in 
# cleaning telecom data.

# Some wards are not close to a tower. We join these wards to other wards based
# on the distance to centroid tower clusters. Specifically, we assign a tower 
# cluster to a ward based on the closest centroid of the cluster. When joining a 
# ward with 0 clusters, we join the ward that contains the closest cluster.

# (1) Aggregate wards
# (2) Add + standardize variables
# (3) Simplify polygon 
# (4) Order by region

# Load / Prep Data -------------------------------------------------------------

#### Load all wards
ward_sp <- read_sf(file.path(GEO_PATH, "admin3.geojson")) %>% 
  as("Spatial")

####  Load tower clusters. Spatially set as centroid cluster
tower_cluster_df <- read.csv(file.path(GEO_PATH, 
                                       "towers_to_wards_clusters.csv"), 
                             stringsAsFactors=F) 
coordinates(tower_cluster_df) <- ~centroid_LNG+centroid_LAT
crs(tower_cluster_df) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")

# Load District Data for Mapping IDs -------------------------------------------
district_sp <- readOGR(dsn = file.path(GEO_PATH),
                       layer = "adm2")
district_sp$ID_2 <- district_sp$ID_2 %>% as.character() %>% as.numeric()
district_sp$NAME_2 <- district_sp$NAME_2 %>% as.character()

# Find district match for each ward. Use distance of centroid of ward to distract.
# For all but one ward, this will find a distance of 0 -- finding ward centroid
# within a district. One ward on edge has centroid that doesn't overlap, so
# where distance will be slightly larger than 0.
dist_match <- lapply(1:nrow(ward_sp), function(i){
  
  if((i %% 100) == 0) print(i)
  
  ward_sp_i <- ward_sp[i,] %>% gCentroid(byid=T)
  close_dist_id <- gDistance(ward_sp_i, district_sp, byid=T) %>% as.numeric %>% which.min()
  
  df_out <- data.frame(district_id = district_sp$ID_2[close_dist_id],
             district_name = district_sp$NAME_2[close_dist_id])
  
  return(df_out)
}) %>% bind_rows()

ward_sp@data <- bind_cols(ward_sp@data, dist_match)

# Aggregate Wards --------------------------------------------------------------

#### Divide into wards with and without clusters
ward_no_cluster_sp <- ward_sp[!(ward_sp$ADM3_PCODE %in% 
                                  tower_cluster_df$ADM3_PCODE),]

ward_with_cluster_sp <- ward_sp[(ward_sp$ADM3_PCODE %in% 
                                   tower_cluster_df$ADM3_PCODE),]

#### Determine ward to aggregate to
# For ward without a tower cluster, determine ward with a tower cluster. Ward
# with the closest centroid tower distance chosen. Returns ward id
# (ADM3_PCODE) of ward that should merge into
ward_no_cluster_sp$ADM3_PCODE_tower <- lapply(1:nrow(ward_no_cluster_sp), 
                                              function(i){
                                                
  if((i %% 10) == 0) print(i) # tracking where at
                                                
  ward_no_cluster_sp_i <- ward_no_cluster_sp[i,]
  
  index_min_dist <- gDistance(ward_no_cluster_sp_i, 
                              tower_cluster_df, 
                              byid=T) %>% 
    which.min()
  
  ward_with_closest_tower_cluster <- tower_cluster_df$ADM3_PCODE[index_min_dist]
  
  return(ward_with_closest_tower_cluster)
}) %>% unlist()

#### For ward with a cluster, ADM3_PCODE_tower is itself
ward_with_cluster_sp$ADM3_PCODE_tower <- ward_with_cluster_sp$ADM3_PCODE

#### Append files
ward_to_aggregate_sp <- rbind(ward_no_cluster_sp, ward_with_cluster_sp)

#### Aggregate
ward_agg_sp <- raster::aggregate(ward_to_aggregate_sp, 
                                          by="ADM3_PCODE_tower")

# Clean File -------------------------------------------------------------------
# Clean variable names, add relevant variables, simplify shapefile

#### Rename ADM
ward_agg_sp@data <- ward_agg_sp@data %>%
  dplyr::rename(ADM3_PCODE = ADM3_PCODE_tower)

#### Add original variables back
ward_agg_sp <- merge(ward_agg_sp, ward_sp@data, 
                              by="ADM3_PCODE", all.x=T, all.y=F)

##### Rename / select variables
ward_agg_sp@data <- ward_agg_sp@data %>%
  mutate(name = paste(ADM2_EN, ADM3_EN) %>%
                     str_replace_all("/", "-")) %>%
  dplyr::rename(province = ADM1_EN,
                region   = ADM3_PCODE) %>%
  dplyr::select(name, region, province,
                district_id, district_name) 

#### Calculate Area
ward_agg_sp$area <- geosphere::areaPolygon(ward_agg_sp) / 1000^2

#### Simplify polygon
# Make faster to load without distorting features much
ward_agg_sp <- rmapshaper::ms_simplify(ward_agg_sp)

#### Order by region
ward_agg_sp$region <- ward_agg_sp$region %>% as.character()
ward_agg_sp <- ward_agg_sp[order(ward_agg_sp$region),]

# Export -----------------------------------------------------------------------
saveRDS(ward_agg_sp, file.path(CLEAN_DATA_ADM3_PATH, 
                               "wards_aggregated.Rds"))

saveRDS(ward_agg_sp, file.path(DASHBOARD_DATA_ONEDRIVE_PATH, 
                               "wards_aggregated.Rds"))

ward_agg_sf <- ward_agg_sp %>% st_as_sf()
ward_agg_sf$district_id <- ward_agg_sf$district_id %>% as.character() %>% as.numeric()

ward_agg_sf <- ward_agg_sf %>%
  dplyr::rename(ward_name = name,
                ward_id = region,
                province_name = province)

st_write(ward_agg_sf, file.path(GEO_PATH, "wards_aggregated.geojson"),
         delete_dsn=T)

ward_agg_df <- ward_agg_sf
ward_agg_df$geometry <- NULL
write.csv(ward_agg_df, file.path(GEO_PATH, "wards_aggregated.csv"), row.names = F)

