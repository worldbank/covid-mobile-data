# Data Checks

This folder contains draft code for basic checks of aggregated CDR indicators. The data quality checks are intended to achieve the following:
1. **Ensure the data is complete.** This means that there are no missing values in two main dimensions: spatial-all admin areas should have data; and temporal: all time slots (month, day and hour) should have data. This check is required for all indicators.
2. **Cell tower down checks**. This is a special type of missing data where the data may be missing due to cell tower. This check is required for all indicators?
3. **Consistentency checks**. This check can be done for a single indicator to check for several things. But it can also be done cross indicators to ensure consistency of total numbers.

