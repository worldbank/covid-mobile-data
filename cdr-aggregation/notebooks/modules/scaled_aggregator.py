# Databricks notebook source
import os
if os.environ['HOME'] != '/root':
    from modules.import_packages import *
    from modules.utilities import *
    from modules.priority_aggregator import *
else:
    databricks = True

class scaled_aggregator(priority_aggregator):
    """Class to handle scaled priority aggregations

    Attributes
    ----------
    [check inherited attributes described in aggregator class]

    weight : a pyspark dataframe. This should hold the weights to be used per
        admin unit
    df : a pyspark dataframe. Merges in weight to all observation based on
        home_location


    Methods
    -------
    [check inherited methods described in aggregator class]

    This class takes over all methods from the priority_aggregator, but scales
        some of the results using the weight attribute
    """
    def __init__(self,
                 result_stub,
                 datasource,
                 regions,
                 re_create_vars = False):

        super().__init__(result_stub,datasource,regions,re_create_vars)


        self.weight = getattr(datasource, self.level + '_weight')\
            .withColumnRenamed('region', 'weight_region')
        self.df = self.df\
            .join(self.weight.select('weight_region', 'weight'),
                                    on = (self.df.home_region == \
                                    self.weight.weight_region),
                                    how = 'left')\
            .drop('weight_region')

    ## Indicator 1
    def transactions(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby(frequency, 'region')\
        .agg(F.sum('constant').alias('count'),
             F.sum('weight').alias('weighted_count_population_scale'))\
        .where(F.col('count') > self.privacy_filter)
      sum_of_counts = result.select('count')\
        .groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
        F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)
      return result

    ## Indicator 2 + 3
    def unique_subscribers(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .withColumn('array', F.array('msisdn', 'weight'))\
        .groupby(frequency, 'region')\
        .agg(F.collect_set('array').alias('array'))\
        .select(frequency, 'region', F.explode('array').alias('array'))\
        .withColumn('msisdn', F.col('array').getItem(0).cast('int'))\
        .withColumn('weight', F.col('array').getItem(1))\
        .drop('array')\
        .groupby(frequency, 'region')\
        .agg(F.count('msisdn').alias('count'),
             F.sum('weight').alias('weighted_count_population_scale'))\
        .where(F.col('count') > self.privacy_filter)
      sum_of_counts = result.select('count').groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
        F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)
      return result

    ## Indicator 3
    def unique_subscribers_country(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .withColumn('array', F.array('msisdn', 'weight'))\
        .groupby(frequency)\
        .agg(F.collect_set('array').alias('array'))\
        .select(frequency, F.explode('array').alias('array'))\
        .withColumn('msisdn', F.col('array').getItem(0).cast('int'))\
        .withColumn('weight', F.col('array').getItem(1))\
        .drop('array')\
        .groupby(frequency)\
        .agg(F.count('msisdn').alias('count'),
             F.sum('weight').alias('weighted_count_population_scale'))\
        .where(F.col('count') > self.privacy_filter)
      sum_of_counts = result.select('count')\
        .groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby().agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
        F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)
      return result

    ## Indicator 4
    def percent_of_all_subscribers_active(self, time_filter, frequency):
      prep = self.df.where(time_filter)\
        .select('msisdn')\
        .distinct()\
        .count()
      result = self.unique_subscribers_country(time_filter, frequency)\
        .withColumn('percent_active', F.col('count') / prep)
      return result

    ## Indicator 5
    def directed_regional_pair_connections(self, time_filter, frequency):
      left_side = self.df.where(time_filter)\
        .groupby('msisdn', frequency, 'region')\
        .agg(F.min('call_datetime').alias('earliest_visit'),
             F.max('call_datetime').alias('latest_visit'),
             F.first('weight').alias('weight'),
             F.first('constant').alias('constant'))\
        .withColumnRenamed('region', 'region_to')
      right_side = left_side\
        .withColumnRenamed('region_to', 'region_from')\
        .withColumnRenamed('latest_visit', 'latest_visit_right_side')\
        .withColumnRenamed(frequency, 'right_frequency')\
        .drop('weight').drop('constant')
      result = left_side.withColumnRenamed('earliest_visit',
        'earliest_visit_left_side')\
        .join(right_side, ((left_side.msisdn == right_side.msisdn) & \
                           (left_side[frequency] == right_side.right_frequency)),
                            'full_outer')\
        .where((F.col('region_to') != F.col('region_from')) & \
         (F.col('earliest_visit_left_side') < F.col('latest_visit_right_side')))\
        .groupby(frequency, 'region_to','region_from')\
        .agg(F.sum('constant').alias('count'),
             F.sum('weight').alias('weighted_count_population_scale'))
      sum_of_counts = result.select('count')\
        .groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
        F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)\
        .withColumnRenamed(frequency, 'connection_frequency')
      return result

    def origin_destination_connection_matrix(self, time_filter, frequency):
      left_side = self.directed_regional_pair_connections(time_filter, frequency)
      right_side = self.df.where(time_filter)\
        .withColumn('day_lag', F.lag('day').over(user_window))\
        .where((F.col('region_lag') != F.col('region')) & \
            ((F.col('day') > F.col('day_lag'))))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.sum('constant').alias('od_count'),
             F.sum('weight').alias('weighted_od_count_population_scale'))
      sum_of_counts = right_side.select('od_count').groupby()\
        .agg(F.sum('od_count')).collect()[0][0]
      sum_of_weights = right_side.select('weighted_od_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_od_count_population_scale')).collect()[0][0]
      right_side = right_side.withColumn('weighted_od_count_observed_scale',
        F.col('weighted_od_count_population_scale') / sum_of_weights * sum_of_counts)
      result = left_side.join(right_side,
                             (left_side.region_to == right_side.region)\
                           & (left_side.region_from == right_side.region_lag)\
                           & (left_side.connection_frequency == right_side[frequency]),
                           'full')\
        .withColumn('region_to',
            F.when(F.col('region_to').isNotNull(),
            F.col('region_to')).otherwise(F.col('region')))\
        .withColumn('region_from',
            F.when(F.col('region_from').isNotNull(),
            F.col('region_from')).otherwise(F.col('region_lag')))\
        .withColumn('connection_frequency',
            F.when(F.col('connection_frequency').isNotNull(),
            F.col('connection_frequency')).otherwise(F.col(frequency)))\
        .na.fill({'od_count' : 0,
                  'count' : 0,
                  'weighted_count_observed_scale' : 0,
                  'weighted_count_population_scale' : 0,
                  'weighted_od_count_observed_scale' : 0,
                  'weighted_od_count_population_scale' : 0})\
        .withColumn('total_count', F.col('count') + F.col('od_count'))\
        .withColumn('total_weighted_count_observed_scale',
            F.col('weighted_count_observed_scale') + \
            F.col('weighted_od_count_observed_scale'))\
        .withColumn('total_weighted_count_population_scale',
            F.col('weighted_count_population_scale') + \
            F.col('weighted_od_count_population_scale'))\
        .drop('region').drop('region_lag').drop('day')\
        .where(F.col('count') > self.privacy_filter)
      return result

    ## Indicator 6 helper method
    def assign_home_locations(self, time_filter, frequency):
      user_day = Window\
        .orderBy(F.desc_nulls_last('call_datetime'))\
        .partitionBy('msisdn', 'day')
      user_frequency = Window\
        .orderBy(F.desc_nulls_last('last_region_count'))\
        .partitionBy('msisdn', frequency)
      result = self.df.where(time_filter)\
        .na.fill({'region' : self.missing_value_code })\
        .withColumn('last_timestamp',
            F.first('call_datetime').over(user_day))\
        .withColumn('last_region',
            F.when(F.col('call_datetime') == F.col('last_timestamp'),
            1).otherwise(0))\
        .orderBy('call_datetime')\
        .groupby('msisdn', frequency, 'region')\
        .agg(F.sum('last_region').alias('last_region_count'),
             F.first('constant').alias('constant'),
             F.first('weight').alias('weight'))\
        .withColumn('modal_region',
            F.when(F.first('last_region_count').over(user_frequency) == \
            F.col('last_region_count'),1).otherwise(0))\
        .where(F.col('modal_region') == 1)\
        .groupby('msisdn', frequency)\
        .agg(F.last('region').alias('home_region'),
             F.last('constant').alias('constant'),
             F.last('weight').alias('weight'))
      return result

    ## Indicator 6
    def unique_subscriber_home_locations(self, time_filter, frequency):
      result = self.assign_home_locations(time_filter, frequency)\
        .groupby(frequency, 'home_region')\
        .agg(F.sum('constant').alias('count'),
             F.sum('weight').alias('weighted_count_population_scale'))\
        .where(F.col('count') > self.privacy_filter)
      sum_of_counts = result.select('count')\
        .groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
        F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)
      return result

    ## Indicator 7 + 8
    def mean_distance(self, time_filter, frequency):
      prep = self.df.where(time_filter)\
        .withColumn('location_id_lag', F.lag('location_id').over(user_window))
      result = prep.join(self.distances_df,
             (prep.location_id==self.distances_df.destination) &\
             (prep.location_id_lag==self.distances_df.origin),
             'left')\
        .groupby('msisdn', 'home_region', frequency)\
        .agg(F.sum('distance').alias('distance'),
             F.last('weight').alias('weight'),
             F.last('constant').alias('constant'))\
        .groupby('home_region', frequency)\
        .agg(F.mean('distance').alias('mean_distance'),
             F.stddev_pop('distance').alias('stdev_distance'),
             F.sum('constant').alias('count'),
             F.sum('weight').alias('weighted_count_population_scale'))
      sum_of_counts = result.select('count')\
        .groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
        F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)
      return result

   ## Indicator 9
    def home_vs_day_location(self,
                             time_filter,
                             frequency,
                             home_location_frequency = 'week',
                             **kwargs):
      home_locations = self.assign_home_locations(time_filter,
                                                  home_location_frequency)
      prep = self.df.where(time_filter)\
        .withColumn('call_datetime_lead',
            F.when(F.col('call_datetime_lead').isNull(),
            self.dates['end_date']).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration',
        (F.col('call_datetime_lead').cast('long') - \
        F.col('call_datetime').cast('long')))\
        .groupby('msisdn', 'region', frequency, home_location_frequency)\
        .agg(F.sum('duration').alias('total_duration'))\
        .orderBy('msisdn', frequency, 'total_duration')\
        .groupby('msisdn', frequency, home_location_frequency)\
        .agg(F.last('region').alias('region'),
             F.last('total_duration').alias('duration'))\
        .withColumnRenamed('msisdn', 'msisdn2')\
        .withColumnRenamed(home_location_frequency,
            home_location_frequency + '2')
      result = prep.join(home_locations,
        (prep.msisdn2 == home_locations.msisdn) &\
        (prep[home_location_frequency + '2'] == \
        home_locations[home_location_frequency]), 'left')\
        .na.fill({'home_region' : self.missing_value_code })\
        .groupby(frequency, 'region', 'home_region')\
        .agg(F.mean('duration').alias('mean_duration'),
             F.stddev_pop('duration').alias('stdev_duration'),
             F.sum('constant').alias('count'),
             F.sum('weight').alias('weighted_count_population_scale'))\
        # .where(F.col('count') > self.privacy_filter)
      sum_of_counts = result.select('count')\
        .groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
        F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)
      return result

    ## Indicator10
    def origin_destination_matrix_time(self, time_filter, frequency):
      user_frequency_window = Window.partitionBy('msisdn').orderBy('call_datetime')
      result = self.df.where(time_filter)\
        .where((F.col('region_lag') != F.col('region')) | \
            (F.col('region_lead') != F.col('region')) | \
            (F.col('call_datetime_lead').isNull()))\
        .withColumn('call_datetime_lead',
            F.when(F.col('call_datetime_lead').isNull(),
            self.dates['end_date']).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration',
            (F.col('call_datetime_lead').cast('long') - \
            F.col('call_datetime').cast('long')))\
        .withColumn('duration_next',
            F.lead('duration').over(user_frequency_window))\
        .withColumn('duration_change_only',
            F.when(F.col('region') == F.col('region_lead'),
            F.col('duration_next') + F.col('duration')).otherwise(F.col('duration')))\
        .withColumn('duration_change_only',
            F.when(F.col('duration_change_only') > (21 * 24 * 60 * 60),
            (21 * 24 * 60 * 60)).otherwise(F.col('duration_change_only')))\
        .withColumn('duration_change_only_lag',
            F.lag('duration_change_only').over(user_frequency_window))\
        .where(F.col('region_lag') != F.col('region'))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.sum(F.col('duration_change_only'))\
            .alias('total_duration_destination'),
           F.avg(F.col('duration_change_only'))\
            .alias('avg_duration_destination'),
           F.count(F.col('duration_change_only'))\
            .alias('count_destination'),
           F.stddev_pop(F.col('duration_change_only'))\
            .alias('stddev_duration_destination'),
           F.sum(F.col('duration_change_only_lag'))\
            .alias('total_duration_origin'),
           F.avg(F.col('duration_change_only_lag'))\
            .alias('avg_duration_origin'),
           F.count(F.col('duration_change_only_lag'))\
            .alias('count_origin'),
           F.stddev_pop(F.col('duration_change_only_lag'))\
            .alias('stddev_duration_origin'),
           F.sum('constant')\
            .alias('count'),
           F.sum('weight')\
            .alias('weighted_count_population_scale'))
      sum_of_counts = result.select('count')\
        .groupby()\
        .agg(F.sum('count')).collect()[0][0]
      sum_of_weights = result.select('weighted_count_population_scale')\
        .groupby()\
        .agg(F.sum('weighted_count_population_scale')).collect()[0][0]
      result = result.withColumn('weighted_count_observed_scale',
      F.col('weighted_count_population_scale') / sum_of_weights * sum_of_counts)
      return result
