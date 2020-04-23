# Aggregate Wards

# Some wards are not close to a tower. We join these wards to other wards based
# on the distance to centroid tower clusters. Specifically, we assign a tower 
# cluster to a ward based on the closest centroid of the cluster. When joining a 
# ward with 0 clusters, we join the ward that contains the closest cluster.

# Load / Prep Data -------------------------------------------------------------

#### Load all wards
ward_sp <- read_sf(file.path(GEO_ADM3_PATH, "zimbabwe_admin3.geojson")) %>% as("Spatial")

####  Load tower clusters. Spatiall set as centroid cluster
tower_cluster_df <- read.csv(file.path(GEO_ADM3_PATH, "econet_towers_to_wards_clusters.csv"), stringsAsFactors=F) 
coordinates(tower_cluster_df) <- ~centroid_LNG+centroid_LAT
crs(tower_cluster_df) <- CRS("+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0")

# Aggregate Wards --------------------------------------------------------------

#### Divide into wards with and without clusters
ward_no_cluster_sp <- ward_sp[!(ward_sp$ADM3_PCODE %in% tower_cluster_df$ADM3_PCODE),]
ward_with_cluster_sp <- ward_sp[(ward_sp$ADM3_PCODE %in% tower_cluster_df$ADM3_PCODE),]

#### Determine ward to aggregate to
# For ward without a tower cluster, determine ward with a tower cluster. Ward
# with the closest centroid tower distance chosen.
# TODO: This is slowish and may be quicker way
ward_no_cluster_sp$ADM3_PCODE_tower <- lapply(1:nrow(ward_no_cluster_sp), function(i){
  if((i %% 10) == 0) print(i)
  ward_no_cluster_sp_i <- ward_no_cluster_sp[i,]
  index_min_dist <- gDistance(ward_no_cluster_sp_i, tower_cluster_df, byid=T) %>% which.min()
  ward_with_closest_tower_cluster <- tower_cluster_df$ADM3_PCODE[index_min_dist]
  return(ward_with_closest_tower_cluster)
}) %>% unlist()

#### For ward with a cluster, ADM3_PCODE_tower is itself
ward_with_cluster_sp$ADM3_PCODE_tower <- ward_with_cluster_sp$ADM3_PCODE

#### Append files
ward_to_aggregate_sp <- rbind(ward_no_cluster_sp, ward_with_cluster_sp)

#### Aggregate
ward_to_aggregate_sp <- raster::aggregate(ward_to_aggregate_sp, by="ADM3_PCODE_tower")

# Clean File -------------------------------------------------------------------
# Clean variable names, add relevant variables, simplify shapefile

#### Rename
ward_to_aggregate_sp@data <- ward_to_aggregate_sp@data %>%
  dplyr::rename(ADM3_PCODE = ADM3_PCODE_tower)

#### Add original variables back
ward_to_aggregate_sp <- merge(ward_to_aggregate_sp, ward_sp@data, by="ADM3_PCODE", all.x=T, all.y=F)

##### Restrict Variables
ward_to_aggregate_sp$ward_name <- paste(ward_to_aggregate_sp$ADM2_EN, ward_to_aggregate_sp$ADM3_EN)

# Limit to relevant variables
ward_to_aggregate_sp@data <- ward_to_aggregate_sp@data %>%
  dplyr::rename(region = ADM3_PCODE) %>%
  dplyr::select(ward_name, region, ADM1_EN, ADM2_EN)

#### Calculate Area
ward_to_aggregate_sp$area_km <- geosphere::areaPolygon(ward_to_aggregate_sp) / 1000^2

#### Simplify shapefile
# Make faster to load without distorting features much
ward_to_aggregate_sp_s <- rmapshaper::ms_simplify(ward_to_aggregate_sp)

#### Names
# Replace slash with dash
ward_to_aggregate_sp_s$ward_name <- ward_to_aggregate_sp_s$ward_name %>%
  str_replace_all("/", "-")

# Export -----------------------------------------------------------------------
saveRDS(ward_to_aggregate_sp_s, file.path(CLEAN_DATA_ADM3_PATH, "ward_aggregated.Rds"))


