# covid-mobile-data
The COVID19 Mobility Task Force will use data from Mobile Network Operators (MNOs) to support data-poor countries with analytics on mobility to inform mitigation policies for preventing the spread of COVID-19.

There are code for three high level tasks in this repository, `cdr-aggregation`, `data-checks` and `dashboard-datviz`.

### cdr-aggregation

This code aggregates and thereby anonymizes CDR data. This code is written in python for an Azure Databricks environment but can be modified to run in other environments. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).

### data-checks

This R code reads the output from the cdr-aggregation and runs quality checks. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/data-checks).

### dashboard-dataviz

Once the data has passed the data-checks, it is presented in an R shiny dashboard. See more [here](https://github.com/worldbank/covid-mobile-data/tree/master/dashboard-dataviz).
