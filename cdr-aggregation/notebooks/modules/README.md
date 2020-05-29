# Module organization

## Aggregation
The base class `aggregator` defined in [aggregator.py](https://github.com/worldbank/covid-mobile-data/tree/cdr-master/cdr-aggregation/notebooks/modules/aggregator.py) implements methods and attributes shared by all aggregator classes. At the next level, `flowminder_aggregator` and `priority_aggregator` implement sql queries from [Flowminder](https://github.com/Flowminder), and priority indicators designed by this task force written in pyspark, respectively. Beyond that, the classes `scaled_aggregator` and `custom_aggregator` implement priority indicators scaled by a resident count, and additional custom pyspark indicators, respectively. Both inherit from the `priority_aggregator` class.

```
|-- aggregator
| |-- flowminder_aggregator
| |-- priority_aggregator
|   |-- scaled_aggregator
|   |-- custom_aggregator
```

## Clustering and tesselation
Modules `voronoi` and `tower_clustering` implement voronoi tesselation given tower locations, these will be of use in the setup phase to create tower-region mappings.

## Outlier analysis
Module `outliers` can be used to study outlier observations.
