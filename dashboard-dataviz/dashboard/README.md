# Dashboard

This dashboard is build using R Shiny.

# Preparing Data for Dashboard

`01_preparing_data_for_dashboard` contains three folders with scripts for cleaning and preparing data for the dashboard.

## Clean Spatial Data

The files in `01_clean_spatial_data` clean spatial polygons to be used in the dashboard and subsequent cleaning steps. The following cleaning steps are conducted:

1. Aggregate units when needed (e.g., aggregating wards)
2. Add additional variables (e.g., area)
3. Standardize variable names
4. Orders spatial data by region

#### Standardize Variable Names
Each spatial dataset should have standardized variable names. Standardizing
variable names helps ensure different units (eg, admin2, admin3) can be
easily switched in the dashboard

| variable | format | example | description |
|---|---|---|---|
| region | string | ZONE123456 | Unique identifier of the spatial unit |
| name | string | name-here | Spatial unit name |
| area | numeric | 1234 | Area of the spatial unit in kilometers squared |
| adm1| string | name-here | Name of the province |

#### Order Spatial Data
Spatial datasets are ordered by region. When cleaning other datasets at the
region level, we also order by region and ensure all regions are present. This
ensures that no reordering needs to be done in the dashboard.

## Clean Telecom Data

The files in `02_clean_telecom_data` clean telecom data. They clean variable values (eg, accounting for outliers), standardize variable names and add variables needed for the dashboard.

#### Dataset

A number of indicators are cleaned. To facilitate further processing for the datasets
to be used in the dashboard, all cleaned datasets have the following standardized
variables:

| variable | format | example | description |
|---|---|---|---|
| region | string | ZONE123456 | Unique identifier of the spatial unit |
| name | string | Name1 | Spatial unit name |
| date | date or string | 2020-02-01 | The date |
| value | numeric | 1000 | Value (e.g., number of subscribers, number of trips, distance traveled) |
| value_lag | numeric | 1000 | Value from the previous time period |
| value_base | numeric | 1000 | Baseline value |
| value_perchange_base | numeric | 50 | Percent change from baseline |
| value_zscore_base | numeric | 50 | Z-score change since baseline |
| label_level | string | Name1 <br>This day's value: 1000<br>...  | Label for when level of variable is shown |
| label_base| string | Name1 <br>This day's value: 1000<br>...  | Label for when change since baseline value is shown. |

## Dashboard Data Prep

The files in `03_dashboard_data_prep` further process data into datasets that are used for the dashboard. Due to the high volume of data, data transformations (e.g., aggregating, filtering, etc) are done outside of the dashboard in order to minimize the processing and data needed to be loaded in memory at any point as the dashboard is running. These scripts filter the cleaned telecom data into individual datasets so that no additional filtering or transformations need to be applied within the dashboard; the dashboard can just read the files then immediately use the data in the map, line graph and table. Here, we create smaller datasets that contain the same variables as above. Indicators include density, movement in, movement out, mean distance traveled, etc.

The following datasets are made.

| Dataset Type | Naming Convention | Description |
| --- | --- | --- |
| unit-level | [Unit Type (eg, ADM1, ADM2, etc)]\_[Indicator Name]\_[Daily/Weekly]\_[Date/Week].Rds | For a given day or week, this dataset contains information for all units for a specified indicator. For O-D level datasets, values are aggregated to the specified origin or destination unit (eg, movement into unit from all other units). |
| time-level |  [Unit Type (eg, ADM1, ADM2, etc)]\_[Indicator Name]\_[Daily/Weekly]\_[Unit Name].Rds | For a given admin unit, this dataset contains a time series of values for a specified indicator. |
| unit-time-level |  [Unit Type (eg, ADM1, ADM2, etc)]\_[Indicator Name]\_[Daily/Weekly]\_[Unit Name]\_[Date/Week].Rds | These datasets are only used for O-D variables. The show, for a given origin or destination unit, the movement in or out of that unit to all other units for the specified day/week. |










