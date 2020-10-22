# Clean Spatial Data

Cleans spatial datasets:
1. Aggregate units when needed (e.g., aggregating wards)
2. Add additional variables (e.g., area)
3. Standardize variable names
4. Orders spatial data by region

### Standardize Variable Names
Each spatial dataset should have standardized variable names. Standardizing
variable names helps ensure different units (eg, admin2, admin3) can be
easily switched in the dashboard

| variable | format | example | description |
|---|---|---|---|
| region | string | ZW123456 | Unique identifier of the spatial unit |
| name | string | Name| | Spatial unit name |
| area | numeric | 1234 | Area of the spatial unit in kilometers squared |
| province | string | Name| Name of the province |

### Order Spatial Data
Spatial datasets are ordered by region. When cleaning other datasets at the
region level, we also order by region and ensure all regions are present. This
ensures that no reordering needs to be done in the dashboard.
