# Clean Aggregated Telecom Data

These scripts clean and standardize aggregated telecom data. This is a necessary
part of the process towards preparing datasets for the dashboard.

## telecom prep [tp] functions

The `_tp_functions.R` file defines a number of functions to help standardize
the cleaning process.

#### Set/Standardize Variables

* __tp_standardize_vars:__ Renames the date, region and value variable names to
`date`, `region` and `value`. The remaining `tp_` functions take these variable
names as defaults.
* __tp_standardize_vars_od:__ Renames variables for origin-destination matrices.
Inputs include the date, region_origin, region_destination and value variables. This function
standardizes those variables and creates a new variable that concatenates region_origin and
region_destination as a unique identifier for the origin-destination pair.

#### Clean Dataset

* __tp_fill_regions:__ Checks for any regions that are missing in the telecom data that are in the polygon/admin data. Adds these regions to the dataset.
* __tp_clean_day:__ If `date` is day of week, cleans into a `Date` variable.
* __tp_clean_week:__ Transforms `date` to represent the week (e.g., `Feb 01 - Feb 07`). Handles
both integers (e.g., week `6`) and groups day of week (e.g., `2020-02-01`)
* __tp_agg_day_to_week:__ Aggregates the dataset from daily to weekly.
* __tp_complete_date_region:__ Completes data with all data/region pairs.
* __tp_complete_date_region_od:__ Completes data with all data/region pairs for
origin-destination datasets.
* __tp_add_polygon_data:__ Adds polygon data to dataset (primarily for `name`)
* __tp_add_polygon_data_od:__ Adds polygon data to dataset for origin-destination data.
Adds all polygon variables as `_origin` and `_dest`

#### Clean Value Variable

* __tp_interpolate_outliers:__ Interpolates outliers on the `value` variable. Includes
options for replacing negative, positive or both types of outliers, and for what is considered
and outlier. Defaults to 4 standard deviations.
* __tp_replace_zeros:__ Interpolates values of zero. Only interpolates when the
number of zeros is equal to or less than `N_zero_thresh`.

#### Add Variables

* __tp_add_percent_change:__ Adds percent change from the last time period (day or week)
on the `value` variable
* __tp_add_baseline_comp_stats:__ Adds percent change and z-score change values
compared to baseline using `value` variable.

#### Add Labels for Leaflet

* __tp_add_label_level:__ Adds label for the original (level) value to be used in
leaflet in the dashboard.
* __tp_add_label_baseline:__ Adds label for change metrics since baseline to be used
in leaflet in the dashboard.

## Dataset

A number of indicators are cleaned. To facilitate further processing for the datasets
to be used in the dashboard, all cleaned datasets have the following standardized
variables:

* __region__: Unique identifier of the unit
* __name__: Unit name
* __date__: Either day (e.g., `2020-02-01`), or week (e.g., `Feb 01 - Feb 07`)
* __value__: Value (e.g., number of subscribers, number of trips, distance traveled)
* __value_lag__: Value from the previous time period
* __value_base__: Baseline value.
* __value_perchange_base__: Percent change from baseline.
* __value_zscore_base__: Z-score change since baseline.
* __label_level__: Label for when level of variable is shown
* __label_base__: Label for when change since baseline value is shown.

## Example cleaning

The following shows an example of cleaning data. Here we have two datasets:

1. __df_day:__ Which is a daily dataset of the number of subscribers at the ward level and contains three
relevant variables: `visit_date` (e.g., `2020-02-01T00:00:00.000Z`), `region` (e.g., `ZW123456`) and
`subscriber_count` (e.g., `1000`).

2. __admin_sp:__ Which is a SpatialPolygonsDataFrame of wards. It contains the variables
described in `01_clean_spatial_data` (i.e., `name`, `region`, `area` and `province`).

```r
df_day_clean <- df_day %>%

  # Standardizes variable names so can avoid defining variable names in the
  # tp_ functions.
  tp_standardize_vars("visit_date", "region", "subscriber_count") %>%

  # Clean dataset
  tp_clean_date() %>%
  tp_fill_regions(admin_sp) %>%
  tp_complete_date_region() %>%
  tp_add_polygon_data(admin_sp) %>%

  # Interpolate/Clean Values
  tp_interpolate_outliers(NAs_as_zero = T) %>%
  tp_replace_zeros(NAs_as_zero = T) %>%

  # Add change metrics
  tp_add_baseline_comp_stats() %>%
  tp_add_percent_change() %>%

  # Add labels
  tp_add_label_level(timeunit = "day", OD = F) %>%
  tp_add_label_baseline(timeunit = "day", OD = F)
```
