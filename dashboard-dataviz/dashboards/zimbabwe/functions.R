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

#### Log values with negatives
# Define function to take the log of values that can deal with negative
# values. Just takes the absoltue value, logs, then reapplies negative
log_neg <- function(values){
  # Log that takes into account zero. Only for logging values for
  # displaying!
  
  values_pos_index <- (values > 0)  %in% T # %in% T to account for NAs 
  values_neg_index <- (values <= 0) %in% T
  
  values_pos_log <- log(values[values_pos_index]+1)
  values_neg_log <- -log(-(values[values_neg_index])+1)
  
  values[values_pos_index] <- values_pos_log
  values[values_neg_index] <- values_neg_log
  
  return(values)
}


as.character.htmlwidget <- function(x, ...) {
  htmltools::HTML(
    htmltools:::as.character.shiny.tag.list(
      htmlwidgets:::as.tags.htmlwidget(
        x
      ),
      ...
    )
  )
}

add_deps <- function(dtbl, name, pkg = name) {
  tagList(
    dtbl,
    htmlwidgets::getDependency(name, pkg)
  )
}

# https://stackoverflow.com/questions/49885176/is-it-possible-to-use-more-than-2-colors-in-the-color-tile-function
color_tile2 <- function (...) {
  formatter("span", style = function(x) {
    style(display = "block",
          padding = "0 4px", 
          font.weight = "bold",
          `border-radius` = "4px", 
          `background-color` = csscolor(matrix(as.integer(colorRamp(...)(normalize(as.numeric(x)))), 
                                               byrow=TRUE, dimnames=list(c("red","green","blue"), NULL), nrow=3)))
  })}
