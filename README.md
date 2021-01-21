# covid-mobile-data
The COVID19 Mobility Task Force will use data from Mobile Network Operators (MNOs) to support data-poor countries with analytics on mobility to inform mitigation policies for preventing the spread of COVID-19.

There is code for three high level tasks in this repository, `cdr-aggregation`, `data-checks` and `dashboard-datviz`. *All code in this repository is under active development.*

### cdr-aggregation

This code aggregates and thereby anonymizes CDR data. This code builds on much of the work that has been developed by [Flowminder](https://web.flowminder.org) for the purpose of supporting MNOs in producing basic indicators. Their code can be found in their [GitHub account](https://github.com/Flowminder).

The aggregation code is written in python for an Azure Databricks environment but a docker image has been provided so that it runs in other environments. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).

### data-checks

This code reads the output from the cdr-aggregation and runs quality checks. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/data-checks).

### dashboard-dataviz

Once the data has passed the data-checks, it is presented in an R shiny dashboard. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/dashboard-dataviz).


## Summary of indicators

Here is a short summary of the key indicators produced, how they should be interpreted, and their limitations. It also contains indicators standard levels of aggregation as produced by our team, but these may vary depending on country context. 

### Indicator 1 - Count of observations

**Standard time aggregation:** Hour

**Standard geographic aggregation:** Lowest administrative level.

Sum of all transactions (calls or messages) made within an hour and ward 

**Analysis**

This indicator is a simple measure of transaction volume and can show variations in cell phone usage over time. It's main use is to help in potentially scaling other indicators in case there are sudden changes in usage patterns which could in turn affect the measures of other indicators.

### Indicator 2 - Count of unique subscribers

**Standard time aggregation:** Hour

**Standard geographic aggregation:** Lowest administrative level.

Number of unique subscriber IDs that made a transaction within an hour and region.

**Analysis:**

his indicator is a proxy for population and can be used to asses changes in population density. The hourly nature of the indicator is more conducive to use in urban settings where there is higher phone usage and changes over the course of a day. This indicator is especially useful to identify areas that might experience sudden influxes of people (for example an area with a market), and therefore useful for identifying possible hotspots for spread of disease. 

**Caveats and limitations:** 

As variations are a function of usage, it can reflect changes in cellphone usage instead of changes in population. In areas with few towers, this indicator is unlikely to be useful since it will primarily capture phone useage rather than people going in and out of an area. or is a simple measure of transaction volume and can show variations in cell phone usage over time. It's main use is to help in potentially scaling other indicators in case there are sudden changes in usage patterns which could in turn affect the measures of other indicators.


### Indicator 3 - Count of unique subscribers	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Number of unique subscriber IDs that made a transaction within a day and region.

**Analysis:**

This indicator is a proxy for population and can be used to asses changes in population density. 							

**Caveats and limitations:** 

As variations are a function of usage, it can reflect changes in cellphone usage instead of changes in population. Given that it is highly dependent on phone usage, and how many subscribers are active on a given day, scaling by the total subscribers active in the country is one way to try to account for this.

### Indicator 4 - Ratio of residents active that day based on those present during baseline

**Standard time aggregation:** Day

**Standard geographic aggregation:** Country 

Ratio of residents active that day based on those present during baseline, set as at least a month before lockdwon measures to prevent the spread of SARS-CoV-2. Active users are defined as those that have made at least one transaction on that day, restricted to SIMs that had at least one observation during the baseline period.

**Analysis:**

A proxy to changes in cellphone usage through time that can be used to scale other indicators to account for days/locations where people may be less or more likely to use their phone.

**Caveats and limitations:** 

This indicator uses a weekly home location (see indicator 6), it may be difficult to distinguish changes in home location to changes in usage. Additionally, there is natural attrition that happens as some people stop using their SIM and also as new SIMs are added, and this is not able to take account of this natural fluctuation so it will bias the results to look like the active subscribers is slowly going down over time, so then if this is used to scale other variables, it will lead to errors. We tested several other ways of doing this indicator, and the best seemed to be to look across all users during the entire period of interest, and then look at how many of them are active on any given day, and count it based on home region in case there is variability by region in active users.

### Indicator 5 - Origin Destination Matrix - trips between two regions

**Standard time aggregation:** 

**Standard geographic aggregation:** Lowest administrative level and up. 

Origin Destination Matrix - trips between two regions. This variable is a combination of two trips types. The first is all consecutive trips across days going back up to 7 days (so if someone made a call from location A and then there next call was 6 days later from location B, this would count as a trip occuring on the day they make the call from B). The second is movement within a day, counting all forward pairs of locations in the same day (so calls from A then B then C then A then B would result in trip pairs AB, AC, BC, BA,CA,CB).							

**Analysis:**

This indicator is one way to look at mobility. The goal is to look at all the places that are connected through people traveling through them. Especially since on long distance trips, people may pass through multiple admin areas and make/receive calls/texts in each of them, if we only look at consecutive pairs, we miss out on the connections across longer distances, which is important for spread of disease. Additionally though, we don't want to just focus on the location at the end of the day and how people change their long term location because for communicable diseases, such as COVID-19, even traveling through a location could lead to spread of the disease if you stop at a gas station or for lunch or any other reason. Additionally, we also want to account for less frequent users and so also include mobility that happens across days, not just within days, but for this we only consider consecutive observations across days. This type of mobility indicator would be most useful for highly communicable diseases where any time in a location could result in transmission, such as COVID-19. It is not as useful for  diseases that might require an overnight stay, such as malaria, where the mosquito that transmits the disease is most active at dusk and dawn, and therefore short movements during the day are unlikely to lead to spread of the disease.

**Caveats and limitations:** 

The indicator cannot captue movements within the same tower-cluster/administrative area.  Also, as only movements associated with a cellphone transaction are observed, time comparisons can be biased if users change their cellphone usage behaviour. Additionally, based on the current definition, for an individual SIM, we only count a link between two locations they visited in the same day once, so someone that might travel back and forth between two areas on the same day will just count as one link. This might underestimate the risk if we think that someone with a travel patterns of ABABABABAB is higher risk than someone that makes a single move from A to B. Nevertheless, the choice in defining the variable in this way is useful to limit possible measurement error arising from people that live on the border between administrative areas, where their calls might be routed through towers in different admin areas making it look like there is lots of movement happening between these neighboring areas.


### Indicator 6 - Number of residents

**Standard time aggregation:** Week

**Standard geographic aggregation:** Lowest administrative level.

Residency is defined as the modal location of the last observation on each day of the week. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific week.

**Analysis:**

It is a proxy for location of residency.  Weekly home locations may not capture the precise home location because people might make few calls in a week, so the location that is the mode may only have appeared on one or two days and may not represent the actual home location. Nevertheless, the weekly definition reflects the typical place a person is located in a given week and helps to capture if they deviate from the typical place. This is useful in situations like the announcement of a lockdown, when people might change their permanent location for some period of time during the lockdown, so this allows for an adjustment to reflect the new locations where people are located. Otherwise, if they are still assigned to their initial permanent home, it will look like there is a lot of movement and people not at home, but that is not contributing to risk necessarily if the person only moved once and then remained in the new location. This variable therefore helps to provide a flexible home location reflecting changing situations.

**Caveats and limitations:**

Due to the limited time dimension, weekly home location can be a poor measure for permanent residency depending on the phone usage, for example, if someone only has one or two observations a week. For permanent residency, usually a month or more of data would provide a better proxy, especially for infrequent users.

### Indicator 7 - Mean and Standard Deviation of distance traveled (by home location)	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Total distance travelled by each SIM in that day, aggregated by averaging across SIMs with the same home location. Distance is defined as the distance between tower centroids of the current transaction and that of the previous transaction.

**Analysis:**

Proxy for daily distance travelled. Since it looks at movement between tower clusters, it not only captures movement across administrative areas (as all other mobility indicators), but also movement within an administrative area if there are multiple tower clusters in it. Therefore, it can help to better account for the amount of mobility happening.

**Caveats and limitations:** 

Distances are a function of the mobile phone towers density. In a rural area where there are fewer towers with wide coverage, distances may increase in discrete jumps as moving within the coverage of a tower will not be detected. Additionally, moving within the area of a single tower will not be detected, so in areas with only a few towers, many people will look like they have a distance of 0. In contrast, in urban settings with multiple towers there is significantly more precision. For that reason, this variable might be more useful in urban, high tower-density areas

### Indicator 8 - Mean and Standard Deviation of distance traveled (by home location)	

**Standard time aggregation:** Week

**Standard geographic aggregation:** Lowest administrative level and up. 

Total distance travelled by each SIM in that week, aggregated by averaging across SIMs with the same home location. Distance is defined as the distance between tower centroids of the current transaction and that of the previous transaction.

**Analysis:**

Proxy for weekly distance travelled. Since it looks at movement between tower clusters, it not only captures movement across administrative areas (as all other mobility indicators), but also movement within an administrative area if there are multiple tower clusters in it. Therefore, it can help to better account for the amount of mobility happening. Also, the weekly nature (compared to the daily) better helps to capture changes for individuals with less observations for whom no distance traveled might be captured when looking daily, but when looking weekly, we may see them traveling across areas.

**Caveats and limitations:** 

'Distances are a function of the mobile phone towers density. In a rural area where there are fewer towers with wide coverage, distances may increase in discrete jumps as moving within the coverage of a tower will not be detected. Additionally, moving within the area of a single tower will not be detected, so in areas with only a few towers, many people will look like they have a distance of 0. In contrast, in urban settings with multiple towers there is significantly more precision. For that reason, this variable might be more useful in urban, high tower-density areas.

### Indicator 9 - Daily locations based on Home Region with average stay time and SD of stay time	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Region where users spent most of the time in that day and duration of that stay. Time is defined as the difference between time of first call in reference region to the time of first call outside the region. The users are than aggregated by their home regoin and the region where they spent the most time that day. Users that do not have any observations that day are not included.

**Analysis:**

This variable is used for epidemiological Agent Based Modeling. It makes it possible to calculate the probability that an individual from a given home location is likely to be in their home location on a given day. This then allows for assigning agents in an ABM a probability of being in some other region outside of their home on any given day. Since the weekly home location definition is used, the focus is on movement away from the home happening in the given week. If monthly home location were used, it could look like many people have a probability of traveling on a given day, but actually they are now semi-permanently in a new location and do not represent a high risk because they are no longer moving after the initial move. 

**Caveats and limitations:** 

'This way of looking at mobility misses out on all the movement that might happen on a given day between different locations where the person only spends a little bit of time, which could be relevant for highly transmissable infectious diseases such as respiratory infections. If we try to look at the probability of being outside of the home location at any point that day, we end up in situations where for some admin areas, there's more than a 90% likelihood of going outside of the home area. For the ABM, it was decided that this would overmeasure movement (this is especially true for people that live at the border of admin areas and might look like they are engaging in lots of movement outside of the home region, but actually their calls just are routed through different nearby towers that are within range of them), and so we went with the more conservative measure of having had to spend the majority of the day in a location.

### Indicator 10 - Simple Origin Destination Matrix - trips between consecutive in time regions with time	

**Standard time aggregation:** Day

**Standard geographic aggregation:** Lowest administrative level and up. 

Simple Origin Destination Matrix - Similar to indicator 5, but only counts consecutive trips. For example, the sequence [A, A, B, C, D] would result in the pairings [AA, AB,  BC,  CD]. It counts the number of times each pair appears and time spent at both origin and destination regions. It also includes consecutive trips across days up to 7 days.

**Analysis:**

It is a proxy for daily mobility across regions with more information on commuting patterns since it also contains information on the approximate duration of stay in a location. It can be used in two ways. First, it counts all the pairs of consecutive moves. So unlike indicator 5, in the situation of ABABABAB in a single day, it will count 7 different moves rather than just 2. Second, combined with the duration, it can help to calculate an estimate of imported disease incidence if combined with incidence data. If we know the incidence in all locations at a given point in time, we can multiply it times the average duration to calculate the probability of being infected and sum across the total number of people entering to calculate total imported incidence. The standard deviation can be used in cases where there is an incubation period and the average duration is less than the incubation period, to recreate the distribution and still count those that stay for a duration longer than the incubation period.

**Caveats and limitations:** 

Importantly, since the indicator only looks at consecutive movement, if someone is passing through a location on their way to their final destination, the link between the origin and final destination will be missed. This especially impacts longer travel and will underestimate longer distance travel and durations.  Also, as only movements associated with a cellphone transaction are observed, we miss movement happening when there are no observations from a given location and time comparisons can be biased if users change their cellphone usage behaviour.

### Indicator 11 - Number of residents

**Standard time aggregation:** Month

**Standard geographic aggregation:** Lowest administrative level and up. 

Residency is defined as the modal location of the last observation on each day of the month. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific week.

**Analysis:**

It is a proxy for location of permanent residency. It can help to demonstrate if there are more permanent shifts in the location of a population.

**Caveats and limitations:** 

It could fail to capture temporary relocations, for example, if people move to the country area to spend lockdown away from the city. Additionally, if a permanent relocation happens in the middle of a month, then within that month, for part of the month it will look like the person is constantly away from their permanent residence, when actually they have moved to a new one, this could lead to bias when calculating number of people away from home, especially if a large event causes a lot of people to relocate permanently. 

