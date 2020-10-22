# Functions ====================================================================

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