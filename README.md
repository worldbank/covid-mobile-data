# covid-mobile-data
The COVID19 Mobility Task Force will use data from Mobile Network Operators (MNOs) to support data-poor countries with analytics on mobility to inform mitigation policies for preventing the spread of COVID-19.

There is code for three high level tasks in this repository, `cdr-aggregation`, `data-checks` and `dashboard-datviz`. *All code in this repository is under active development.*

### cdr-aggregation

This code aggregates and thereby anonymizes CDR data. This code builds on much of the work that has been developed by [Flowminder](https://web.flowminder.org) for the purpose of supporting MNOs in producing basic indicators. Their code can be found in their [GitHub account](https://github.com/Flowminder).

The aggregation code is written in python for an Azure Databricks environment but a docker image has been provided so that it runs in other environments. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).

### data-checks

This R code reads the output from the cdr-aggregation and runs quality checks. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/data-checks).

### dashboard-dataviz

Once the data has passed the data-checks, it is presented in an R shiny dashboard. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/dashboard-dataviz).
