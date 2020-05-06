# Dashboard Data Prep

Due to the high volume of data, data transformations (e.g., aggregating, filtering, etc) are done outside of the dashboard in order to minimize the processing and data needed to be loaded in memory at any point as the dashboard is running. These scripts filter the cleaned telecom data into individual datasets so that no additional filtering or transformations need to be applied within the dashboard; the dashboard can just read the files then immediately use the data in the map, line graph and table. Here, we create smaller datasets that contain the same variables as above. Indicators include density, movement in, movement out, mean distance traveled, etc.

The following datasets are made.

| Dataset Type | Naming Convention | Description |
| --- | --- | --- |
| ward-level | [Wards/Districts]\_[Indicator Name]\_[Daily/Weekly]\_[Date/Week].Rds | For a given day or week, this dataset contains information for all wards or districts for a specified indicator. For O-D level datasets, values are aggregated to the specified origin or destination unit (eg, movement into ward from all other wards). |
| time-level |  [Wards/Districts]\_[Indicator Name]\_[Daily/Weekly]\_[Ward/District Name].Rds | For a given admin unit, this dataset contains a time series of values for a specified indicator. |
| ward-time-level |  [Ward/Districts]\_[Indicator Name]\_[Daily/Weekly]\_[Ward/District Name]\_[Date/Week].Rds | These datasets are only used for O-D variables. The show, for a given origin or destination unit, the movement in or out of that unit to all other units for the specified day/week. |

