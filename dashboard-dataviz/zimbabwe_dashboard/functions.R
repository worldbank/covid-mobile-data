# Functions ====================================================================
get_arrowhead <- function(fromPoint, toPoint, length_km){
  
  # dx,dy = arrow line vector
  dx <- toPoint$x - fromPoint$x;
  dy <- toPoint$y - fromPoint$y;
  
  # normalize
  length <- sqrt(dx * dx + dy * dy);
  unitDx <- dx / length;
  unitDy <- dy / length;
  
  # increase this to get a larger arrow head
  arrowHeadBoxSize = length_km/111.12;
  
  arrowPoint1 <- list(x = (toPoint$x - unitDx * arrowHeadBoxSize - unitDy * arrowHeadBoxSize),
                      y = (toPoint$y - unitDy * arrowHeadBoxSize + unitDx * arrowHeadBoxSize));
  arrowPoint2 <- list(x = (toPoint$x - unitDx * arrowHeadBoxSize + unitDy * arrowHeadBoxSize),
                      y = (toPoint$y - unitDy * arrowHeadBoxSize - unitDx * arrowHeadBoxSize));
  
  return( mapply(c, arrowPoint1, toPoint, arrowPoint2) )
  
}

extract_arrows <- function(i, df, length_km, move_type_i){
  line_coords <- df[i,] %>% 
    coordinates %>% 
    as.data.frame
  line_coords <- line_coords[c(1, nrow(line_coords)),]
  
  if(move_type_i %in% "Movement Into"){
    line_coords <- line_coords %>% map_df(rev) %>% as.data.frame()
    
    # Arrow in middle
    line_coords$lon[2] <- line_coords$lon %>% mean()
    line_coords$lat[2] <- line_coords$lat %>% mean()
  }

  fromPoint <- list (x = line_coords[1,1], y = line_coords[1,2])
  toPoint <- list (x = line_coords[2,1], y = line_coords[2,2])
  
  # get coordinates of arrowhead
  arrow_data <- get_arrowhead(fromPoint, toPoint, length_km)
  
  arrow_line <- SpatialLines(list(Lines(list(Line(coordinates(arrow_data))),"X"))) 
  arrow_line$value_weight <- df$value_weight[i]
  
  return(arrow_line)
}
