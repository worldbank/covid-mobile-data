# CDR indicators description

__Timeframe__: Indicators are built using data from Feb 1 2020 and going forward in real time.

__Data Type__: CDR or, if available, netwark probes.

__Description__:

Towers are clustered using Ward’s hierarchical clustering, with a maximum distance constraint of 1km from the centroid of the cluster resulting in ‘flat’ clusters. We use the clustering function: https://docs.scipy.org/doc/scipy-0.14.0/reference/generated/scipy.cluster.hierarchy.fcluster.html with criterion: distance and t = 1km. We then assign admin units based on the location of the centroid of the cluster (all observations in the cluster are then assigned to that admin unit). 

## Indicators:

Priority 1 indicators are given preference, priority 2 indicators are meant to help with assessing changes in the underlying data that could affect the indicators of interest, and priority 3 indicators are secondary.

| | Indicator |	Geographic Level | Time Level | How to Calculate | Priority |
|-|-----------|------------------|------------|------------------|----------|
|1|	Count of observations|	Lowest admin level | Hour |	Sum across all observations in the given hour and lowest admin area.| 2 |
|2|	Count of unique subscribers|	Tower cluster level, Lowest admin level | Hour |Sum all unique subscribers with an observation in the given admin area and time period. | 1|
|3|	Count of unique subscribers |	Lowest admin level, Admin Level 2, Country | Day	| Sum all unique subscribers with an observation in the given admin area and time period. |1|
|4| Percent of all subscribers active that day | Country | Day | Use separate information on total subscribers; Divide the number of subscribers with at least one observation that day by this total number. Alternative if not possible: count of unique subscribers in the country for the month.|2|
|5|	Origin Destination Matrix - trips between two regions |	Lowest admin level and Admin Level 2 | Day |	1. For each subscriber, list the unique regions that they visited within the time day, ordered by time. Create pairs of regions by pairing the nth region with all other regions that come after it. For example, the sequence [A, A, B, C, D] would result in the pairings [AA, AB, AC, AD, BC, BD, CD]. For each pair, count the number of times that pair appears. <br>2. For each subscriber, look at the location of the first observation of the day. Look back and get the location of the last observation before this one (no matter how far back it goes) to form a pair, keep the date assigned to this pair as the date of the first observation of the day <br>3. Sum all pairs from 1 and from 2 for each day (also keep the sum of 1 and sum of 2 as variables). |1|
|6|	Residents living in area | Lowest admin level |Week | Look at the location of the last observation on each day of the week and take the modal location to be the home location. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific week. |3|
|7|	Mean and Standard Deviation of distance traveled (by home location)|Lowest admin level and Admin 2 level | Day | Calculate distance traveled by each subscriber daily based on location of all calls (compute distance between tower cluster centroids). Group subscribers by their home location. Calculate mean distance traveled and SD for all subscribers from the same home region. |3|
|8|	Mean and Standard Deviation of distance traveled (by home location)| Lowest admin level and Admin 2 level | Week |	Calculate distance traveled by each subscriber weekly based on location of all calls (compute distance between tower cluster centroids). Group subscribers by their home location. Calculate mean distance traveled and SD for all subscribers from the same home region. |3|
 
 ### Indicators for Epidemiological modeling 
| | Indicator |	Geographic Level | Time Level | How to Calculate | Priority |
|-|-----------|------------------|------------|------------------|----------|
|9|	Daily locations based on Home Region with average stay time and SD of stay time | Admin level 2 | Day |	1. For days with at least one observation, assign the district where the most time is spent (so each subscriber present on a day should have one location) <br>2. Use Weekly assigned home location (#6 above) for each person. <br>3. Create matrix of home location vs day location: for each home location sum the total number of people that from that home location that are in each district (including the home one) based on where they spent the most time and get mean and SD for time spent in the location (by home location). |3|
|10|	Simple Origin Destination Matrix - trips between consecutive in time regions with time | Lowest admin level and Admin 2 Level	| Day |	1.  For each SIM, calculate movement between consecutive observations. <br>2. Calculate time spent in the origin and time spent in the destination for each trip <br>3. For each day, sum all people going from X to Y area and the average time spent in X before going and spent in Y after arriving.|1|
