# Databricks notebook source
import os
if os.environ['HOME'] != '/root':
    from modules.flowminder_aggregations import *
    from modules.import_packages import *
    from modules.utilities import *
else:
    databricks = True

class custom_aggregator(aggregator):
    """Class to handle custom aggregations
    Attributes
    ----------
    calls : a dataframe. This should hold the CDR data to be processed
    sites_handler : instance of tower_clusterer.
    result_path : a string. Where to save results
    dates_sql : a dictionary. From when to when to run the queries
    intermediate_tables : a list. Tables that we don't want written to csv
    spark : An initialised spark connection
    Methods
    -------
    run_and_save_sql(df)
        - applies the aggregation and produces a dataframe
        - saves the result to a csv
    """

    def __init__(self,
                 result_stub,
                 datasource,
                 regions,
                 re_create_vars = False):
        """
        Parameters
        ----------
        """
        super().__init__(result_stub,datasource,regions)

        if regions == 'admin2_tower_map':
            self.level = 'admin2'
        elif regions == 'admin3_tower_map':
            self.level = 'admin3'
        else:
            self.level = 'voronoi'
        self.distances_df = datasource.distances
        self.table_names = []
        self.period_filter = (F.col('call_datetime') >= self.dates['start_date']) &\
                             (F.col('call_datetime') <= self.dates['end_date'])
        self.weeks_filter = (F.col('call_datetime') >= self.dates['start_date_weeks']) &\
                            (F.col('call_datetime') <= self.dates['end_date_weeks'])

        if self.level == 'admin2':
            self.incidence = getattr(datasource, 'admin2_incidence')

        if databricks:
          try:
            # does the file exist?
            dbutils.fs.ls(os.path.join(self.datasource.standardize_path,
                self.datasource.parquetfile_vars + self.level + '.parquet'))
            create_vars = False
          except Exception as e:
            create_vars = True

        else:
            create_vars = (os.path.exists(os.path.join(self.datasource.standardize_path,
                self.datasource.parquetfile_vars + self.level + '.parquet')) == False)

        if (re_create_vars | create_vars):
            print('Creating vars parquet-file...')
            self.df = self.calls.join(self.cells, self.calls.location_id == self.cells.cell_id, how = 'left').drop('cell_id')\
              .join(self.spark.sql(self.sql_code['home_locations']).withColumnRenamed('region', 'home_region'), 'msisdn', 'left')\
              .orderBy('msisdn', 'call_datetime')\
              .withColumn('region_lag', F.lag('region').over(user_window))\
              .withColumn('region_lead', F.lead('region').over(user_window))\
              .withColumn('call_datetime_lag', F.lag('call_datetime').over(user_window))\
              .withColumn('call_datetime_lead', F.lead('call_datetime').over(user_window))\
              .withColumn('hour_of_day', F.hour('call_datetime').cast('byte'))\
              .withColumn('hour', F.date_trunc('hour', F.col('call_datetime')))\
              .withColumn('week', F.date_trunc('week', F.col('call_datetime')))\
              .withColumn('month', F.date_trunc('month', F.col('call_datetime')))\
              .withColumn('constant', F.lit(1).cast('byte'))\
              .withColumn('day', F.date_trunc('day', F.col('call_datetime')))\
              .withColumn('call_date', F.date_trunc('day', F.col('call_datetime')))\
              .na.fill({'region' : 99999, 'region_lag' : 99999, 'region_lead' : 99999})

            self.df = save_and_load_parquet(self.df,
                os.path.join(self.datasource.standardize_path,self.datasource.parquetfile_vars + self.level + '.parquet'), self)

        else:
            self.df = self.spark.read.format("parquet").load(
                os.path.join(self.datasource.standardize_path,self.datasource.parquetfile_vars + self.level + '.parquet'))

    def run_and_save_all(self, time_filter, frequency):
      if frequency == 'hour':
        # indicator 1
        self.table_names.append(self.save_and_report(self.transactions(time_filter, frequency), 'transactions_per_' + frequency))
        # indicator 2
        self.table_names.append(self.save_and_report(self.unique_subscribers(time_filter, frequency), 'unique_subscribers_per_' + frequency))

      elif frequency == 'day':
        # indicator 3
        self.table_names.append(self.save_and_report(self.unique_subscribers(time_filter, frequency), 'unique_subscribers_per_' + frequency))
        # indicator 4
#         self.table_names.append(self.save_and_report(self.percent_of_all_subscribers_active(time_filter, frequency), 'percent_of_all_subscribers_active_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.active_residents_from_specific_period(time_filter, frequency ,exlusion_start = dt.datetime(2020,4,1)), 'percent_of_all_subscribers_active_option1_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.active_residents_from_specific_period(time_filter, frequency), 'percent_of_all_subscribers_active_option2_per_' + frequency))
        self.table_names.append(self.save_and_report(self.active_residents_from_specific_period(time_filter, frequency, active_only_at_home = False), 'percent_of_all_subscribers_active_option3_per_' + frequency))
        # indicator 5
        self.table_names.append(self.save_and_report(self.origin_destination_connection_matrix(time_filter, frequency), 'origin_destination_connection_matrix_per_' + frequency))
        # indicator 7
        self.table_names.append(self.save_and_report(self.mean_distance(time_filter, frequency), 'mean_distance_per_' + frequency))
        # indicator 9
        self.table_names.append(self.save_and_report(self.home_vs_day_location(time_filter, frequency, home_location_frequency = 'week'), 'week_home_vs_day_location_per_' + frequency))
        self.table_names.append(self.save_and_report(self.home_vs_day_location(time_filter, frequency, home_location_frequency = 'month'), 'month_home_vs_day_location_per_' + frequency))
        # indicator 10
        self.table_names.append(self.save_and_report(self.origin_destination_matrix_time(time_filter, frequency), 'origin_destination_matrix_time_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.origin_destination_matrix_time_longest_only(time_filter, frequency), 'origin_destination_matrix_time_longest_only_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.origin_destination_unique_users_matrix(time_filter, frequency), 'origin_destination_unique_users_matrix_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.origin_destination_matrix(time_filter, frequency), 'origin_destination_matrix_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.percent_residents_day_equal_night_location(time_filter, frequency), 'percent_residents_day_equal_night_location_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.different_areas_visited(time_filter, frequency), 'different_areas_visited_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.median_distance(time_filter, frequency), 'median_distance_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.only_in_one_region(time_filter, frequency), 'only_in_one_region_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.new_sim(time_filter, frequency), 'new_sims_per_' + frequency))
      elif frequency == 'week':
        # indicator 6
        self.table_names.append(self.save_and_report(self.unique_subscriber_home_locations(time_filter, frequency), 'unique_subscriber_home_locations_per_' + frequency))
        # indicator 8
        self.table_names.append(self.save_and_report(self.mean_distance(time_filter, frequency), 'mean_distance_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.origin_destination_matrix(time_filter, frequency), 'origin_destination_matrix_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.origin_destination_matrix_time_longest_only(time_filter, frequency), 'origin_destination_matrix_time_longest_only_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.origin_destination_unique_users_matrix(time_filter, frequency), 'origin_destination_unique_users_matrix_per_' + frequency))
#         self.table_names.append(self.save_and_report(self.origin_destination_matrix_time(time_filter, frequency), 'origin_destination_matrix_time_per_' + frequency))
      elif frequency == 'month':
        pass
#         self.table_names.append(self.save_and_report(self.unique_subscribers(time_filter, frequency), 'unique_subscribers_per_' + frequency))
      else:
        print('What is the frequency')

    def run_and_save_all_frequencies(self):
      self.run_and_save_all(self.period_filter, 'day')
      self.run_and_save_all(self.period_filter, 'hour')
      self.run_and_save_all(self.weeks_filter, 'week')
      self.run_and_save_all(self.weeks_filter, 'month')

    def run_save_and_rename_all(self):
      self.run_and_save_all_frequencies()
      self.rename_all_csvs()

    def attempt_aggregation(self, indicators_to_produce = 'all', no_of_attempts = 4):
        attempts = 0
        while attempts < no_of_attempts:
            try:
                # all indicators
                if indicators_to_produce == 'all':
                  self.run_save_and_rename_all()

                # single indicator
                else:
                    for table in indicators_to_produce.keys():
                        table_name = indicators_to_produce[table][0]
                        frequency = indicators_to_produce[table][1]

                        # more than the standard arguments
                        if isinstance(frequency, list):
                          other_args = frequency[1]
                          frequency = frequency[0]
                          if frequency == 'week':
                            filter_var = self.weeks_filter
                          else:
                            filter_var = self.period_filter

                          result = getattr(self, table_name)(filter_var, frequency, **other_args)
                          try:
                            table_name = other_args['home_location_frequency'] + '_' + table_name
                          except:
                            pass

                        # only the standard arguments
                        else:
                          if frequency == 'week':
                            filter_var = self.weeks_filter
                          else:
                            filter_var = self.period_filter
                          result = getattr(self, table_name)(filter_var, frequency)

                        # save and rename
                        table_name = table_name  + '_per_' + frequency
                        table_name = self.save_and_rename_one(result, table_name)
                print('Custom indicators saved.')
                break
            except Exception as e:
                attempts += 1
                print(e)
                print('Try number {} failed. Trying again.'.format(attempts))
                if attempts == 4:
                    print('Tried creating and saving indicators 4 times, but failed.')

    ##### Priority Indicators

    ## Indicator 1
    def transactions(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby(frequency, 'region')\
        .count()\
        .where(F.col('count') > 15)
      return result

    ## Indicator 2 + 3
    def unique_subscribers(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby(frequency, 'region')\
        .agg(F.countDistinct('msisdn').alias('count'))\
        .where(F.col('count') > 15)
      return result

    ## Indicator 3
    def unique_subscribers_country(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby(frequency)\
        .agg(F.countDistinct('msisdn').alias('count'))\
        .where(F.col('count') > 15)
      return result

    ## Indicator 4
    def percent_of_all_subscribers_active(self, time_filter, frequency):
      prep = self.df.where(time_filter)\
        .select('msisdn')\
        .distinct()\
        .count()
      result = self.unique_subscribers_country(time_filter, frequency).withColumn('percent_active', F.col('count') / prep)
      return result

    ## Indicator 5
    def origin_destination_connection_matrix(self, time_filter, frequency):
      assert frequency == 'day', 'This indicator is only defined for daily frequency'
      result = self.spark.sql(self.sql_code['directed_regional_pair_connections_per_day'])
      prep = self.df.where(time_filter)\
        .withColumn('day_lag', F.lag('day').over(user_window))\
        .where((F.col('region_lag') != F.col('region')) & ((F.col('day') > F.col('day_lag'))))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.count(F.col('msisdn')).alias('od_count'))\
        .where(F.col('od_count') > 15)
      result = result.join(prep, (prep.region == result.region_to)\
                           & (prep.region_lag == result.region_from)\
                           & (prep.day == result.connection_date), 'full')\
        .withColumn('region_to', F.when(F.col('region_to').isNotNull(), F.col('region_to')).otherwise(F.col('region')))\
        .withColumn('region_from', F.when(F.col('region_from').isNotNull(), F.col('region_from')).otherwise(F.col('region_lag')))\
        .withColumn('connection_date', F.when(F.col('connection_date').isNotNull(), F.col('connection_date')).otherwise(F.col('day')))\
        .na.fill({'od_count' : 0, 'subscriber_count' : 0})\
        .withColumn('total_count', F.col('subscriber_count') + F.col('od_count'))\
        .drop('region').drop('region_lag').drop('day')
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
        .na.fill({'region' : 99999})\
        .withColumn('last_timestamp', F.first('call_datetime').over(user_day))\
        .withColumn('last_region', F.when(F.col('call_datetime') == F.col('last_timestamp'), 1).otherwise(0))\
        .orderBy('call_datetime')\
        .groupby('msisdn', frequency, 'region')\
        .agg(F.sum('last_region').alias('last_region_count'))\
        .withColumn('modal_region', F.when(F.first('last_region_count').over(user_frequency) == F.col('last_region_count'),1).otherwise(0))\
        .where(F.col('modal_region') == 1)\
        .groupby('msisdn', frequency)\
        .agg(F.last('region').alias('home_region'))
      return result

    ## Indicator 6
    def unique_subscriber_home_locations(self, time_filter, frequency):
      result = self.assign_home_locations(time_filter, frequency)\
        .groupby(frequency, 'home_region')\
        .count()\
        .where(F.col('count') > 15)
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
        .agg(F.sum('distance').alias('distance'))\
        .groupby('home_region', frequency)\
        .agg(F.mean('distance').alias('mean_distance'), F.stddev_pop('distance').alias('stdev_distance'))
      return result

   ## Indicator 9
    def home_vs_day_location(self, time_filter, frequency, home_location_frequency = 'week', **kwargs):
      home_locations = self.assign_home_locations(time_filter, home_location_frequency)
      prep = self.df.where(time_filter)\
        .withColumn('call_datetime_lead', F.when(F.col('call_datetime_lead').isNull(), self.dates['end_date']).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration', (F.col('call_datetime_lead').cast('long') - F.col('call_datetime').cast('long')))\
        .groupby('msisdn', 'region', frequency, home_location_frequency)\
        .agg(F.sum('duration').alias('total_duration'))\
        .orderBy('msisdn', frequency, 'total_duration')\
        .groupby('msisdn', frequency, home_location_frequency)\
        .agg(F.last('region').alias('region'), F.last('total_duration').alias('duration'))\
        .withColumnRenamed('msisdn', 'msisdn2')\
        .withColumnRenamed(home_location_frequency, home_location_frequency + '2')
      result = prep.join(home_locations, (prep.msisdn2 == home_locations.msisdn) & (prep[home_location_frequency + '2'] == home_locations[home_location_frequency]), 'left')\
        .na.fill({'home_region' : 99999})\
        .groupby(frequency, 'region', 'home_region')\
        .agg(F.mean('duration').alias('mean_duration'), F.stddev_pop('duration').alias('stdev_duration'), F.count('msisdn').alias('count'))\
        .where(F.col('count') > 15)
      return result

    ## Indicator10
    def origin_destination_matrix_time(self, time_filter, frequency):
      user_frequency_window = Window.partitionBy('msisdn').orderBy('call_datetime')
      result = self.df.where(time_filter)\
        .where((F.col('region_lag') != F.col('region')) | (F.col('region_lead') != F.col('region')) | (F.col('call_datetime_lead').isNull()))\
        .withColumn('call_datetime_lead', F.when(F.col('call_datetime_lead').isNull(), self.dates['end_date']).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration', (F.col('call_datetime_lead').cast('long') - F.col('call_datetime').cast('long')))\
        .withColumn('duration_next', F.lead('duration').over(user_frequency_window))\
        .withColumn('duration_change_only', F.when(F.col('region') == F.col('region_lead'), F.col('duration_next') + F.col('duration')).otherwise(F.col('duration')))\
        .withColumn('duration_change_only', F.when(F.col('duration_change_only') > (21 * 24 * 60 * 60), (21 * 24 * 60 * 60)).otherwise(F.col('duration_change_only')))\
        .withColumn('duration_change_only_lag', F.lag('duration_change_only').over(user_frequency_window))\
        .where(F.col('region_lag') != F.col('region'))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.sum('duration_change_only').alias('total_duration_destination'),
           F.avg('duration_change_only').alias('avg_duration_destination'),
           F.count('duration_change_only').alias('count_destination'),
           F.stddev_pop('duration_change_only').alias('stddev_duration_destination'),
           F.sum('duration_change_only_lag').alias('total_duration_origin'),
           F.avg('duration_change_only_lag').alias('avg_duration_origin'),
           F.count('duration_change_only_lag').alias('count_origin'),
           F.stddev_pop('duration_change_only_lag').alias('stddev_duration_origin'))
      return result

    ##### Non-priority Indicators

    def origin_destination_matrix(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .where(F.col('region_lag') != F.col('region'))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.count(F.col('msisdn')).alias('count'))
      return result

    def origin_destination_unique_users_matrix(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .where(F.col('region_lag') != F.col('region'))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.countDistinct(F.col('msisdn')).alias('count'))
      return result

    def percent_residents_day_equal_night_location(self, time_filter, frequency):
      user_day_window = Window.partitionBy('msisdn', 'call_date')
      user_day_night_window = Window.partitionBy('msisdn', 'home_region', 'call_date', frequency)\
        .orderBy('day_night')
      result = self.df.where(time_filter)\
        .withColumn('day_night', F.when((F.col('hour_of_day') < 9) | (F.col('hour_of_day') > 17), 1).otherwise(0))\
        .withColumn('night_day', F.when((F.col('hour_of_day') > 9) & (F.col('hour_of_day') < 17), 1).otherwise(0))\
        .withColumn('day_and_night', F.when((F.sum(F.col('day_night')).over(user_day_window) > 0) &\
                             (F.sum(F.col('night_day')).over(user_day_window) > 0), 1).otherwise(0))\
        .where(F.col('day_and_night') == 1)\
        .groupby('msisdn', 'home_region', 'call_date', frequency, 'day_night', 'region')\
        .agg(F.count('location_id').alias('region_count'))\
        .orderBy('region_count')\
        .groupby('msisdn', 'home_region', 'call_date', frequency, 'day_night')\
        .agg(F.last('region_count').alias('max_region'))\
        .withColumn('day_equal_night', F.when(F.col('max_region') == F.lag('max_region').over(user_day_night_window), 1).otherwise(0))\
        .where(F.col('day_night') == 1)\
        .groupby('home_region', frequency)\
        .agg(F.sum('day_equal_night').alias('day_equal_night_count'), F.count('day_equal_night').alias('total'))\
        .withColumn('pct_day_is_night', F.col('day_equal_night_count') / F.col('total'))
      return result

    def median_distance(self, time_filter, frequency):
      prep = self.df.where(time_filter)
      prep = prep.withColumn('location_id_lag', F.lag('location_id').over(user_window))
      prep = prep.join(self.distances_df,
             (prep.location_id==self.distances_df.destination) &\
             (prep.location_id_lag==self.distances_df.origin),
             'left')\
        .groupby('msisdn', 'home_region', frequency)\
        .agg(F.sum('distance').alias('distance'))
      prep.createOrReplaceTempView("df")
      result = self.spark.sql("select {}, home_region, percentile_approx(distance,0.5) as median_distance from df group by home_region, {}".format(frequency, frequency))
      return result

    def different_areas_visited(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby('msisdn', 'home_region', frequency)\
        .agg(F.countDistinct(F.col('region')).alias('distinct_regions_visited'))\
        .groupby('home_region', frequency)\
        .agg(F.avg('distinct_regions_visited').alias('count'))
      return result

    def only_in_one_region(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby('msisdn', 'home_region', frequency)\
        .agg(F.countDistinct('region').alias('region_count'))\
        .where(F.col('region_count') == 1)\
        .groupby('home_region', frequency)\
        .agg(F.countDistinct('msisdn').alias('count'))
      return result

    def new_sim(self, time_filter, frequency):
      assert frequency == 'day', 'This indicator is only defined for daily frequency'
      region_month_window = Window.orderBy(F.col('frequency_sec'))\
        .partitionBy('region')\
        .rangeBetween(-days(28), Window.currentRow)
      window_into_the_past = Window.orderBy(F.col('frequency_sec'))\
        .partitionBy('msisdn')\
        .rangeBetween(Window.unboundedPreceding, Window.currentRow)
      result = self.df.where(time_filter)\
        .orderBy(F.col(frequency))\
        .withColumn('frequency_sec', F.col(frequency).cast("long"))\
        .withColumn('new_sim', F.when(F.count('msisdn').over(window_into_the_past) == 1, 1).otherwise(0))\
        .groupby('region', frequency, 'frequency_sec')\
        .agg(F.sum('new_sim').alias('new_sims'))\
        .withColumn('new_sims_month', F.sum('new_sims').over(region_month_window))\
        .drop('frequency_sec')
      return result

    def accumulated_incidence(self, time_filter, incubation_period_end = dt.datetime(2020,3,30), incubation_period_start = dt.datetime(2020,3,8), **kwargs):
      user_window_incidence = Window\
        .partitionBy('msisdn').orderBy('stop_number')
      user_window_incidence_rev = Window\
        .partitionBy('msisdn').orderBy(F.desc_nulls_last('stop_number'))
      result = self.df\
        .withColumn('call_datetime_lag', F.when(F.col('call_datetime_lag').isNull(), self.dates['start']).otherwise(F.col('call_datetime_lag')))\
        .withColumn('call_datetime_lead', F.when(F.col('call_datetime_lead').isNull(), self.dates['end']).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration', (F.col('call_datetime_lead').cast('long') - F.col('call_datetime').cast('long')))\
        .withColumn('stop_number', F.row_number().over(user_window_incidence))\
        .where((F.col('day') < incubation_period_end) & (F.col('day') > incubation_period_start))\
        .groupby('msisdn', 'day', 'region')\
        .agg(F.sum('duration').alias('total_duration'), F.max('stop_number').alias('stop_number'))\
        .join(self.incidence, 'region', 'left')\
        .withColumn('accumulated_incidence', F.col('incidence') * F.col('total_duration') / (21 * 24 * 60 * 60))\
        .withColumn('last_stop', F.when(F.col('stop_number') == F.max('stop_number').over(user_window_incidence_rev), 1).otherwise(0))\
        .withColumn('imported_incidence',
                    F.when(F.col('last_stop') == 1, F.sum(F.col('accumulated_incidence')).over(user_window_incidence)).otherwise(0))\
        .groupby('region')\
        .agg(F.sum('imported_incidence').alias('imported_incidence'))
      return result

    def accumulated_incidence_imported_only(self, time_filter, incubation_period_end = dt.datetime(2020,3,30), incubation_period_start = dt.datetime(2020,3,8), **kwargs):
      user_window_prep = Window\
        .partitionBy('msisdn').orderBy('call_datetime')
      user_window_incidence = Window\
        .partitionBy('msisdn').orderBy('stop_number')
      user_window_incidence_rev = Window\
        .partitionBy('msisdn').orderBy(F.desc_nulls_last('stop_number'))
      result = self.df.orderBy('call_datetime')\
        .withColumn('call_datetime_lead', F.when(F.col('call_datetime_lead').isNull(), self.dates['end']).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration', (F.col('call_datetime_lead').cast('long') - F.col('call_datetime').cast('long')))\
        .withColumn('stop_number', F.row_number().over(user_window_prep))\
        .where((F.col('day') < incubation_period_end) & (F.col('day') > incubation_period_start))\
        .groupby('msisdn', 'day', 'region')\
        .agg(F.sum('duration').alias('total_duration'), F.max('stop_number').alias('stop_number'))\
        .join(self.incidence, 'region', 'left')\
        .withColumn('accumulated_incidence', F.col('incidence') * F.col('total_duration') / (21 * 24 * 60 * 60))\
        .withColumn('last_stop', F.when(F.col('stop_number') == F.max('stop_number').over(user_window_incidence_rev), 1).otherwise(0))\
        .withColumn('same_region_as_last_stop', F.when((F.col('last_stop') == 0) & (F.col('region') == F.first('region').over(user_window_incidence_rev)), 1).otherwise(0))\
        .withColumn('stop_number_filtered', F.row_number().over(user_window_incidence))\
        .withColumn('stop_number_filtered_rev', F.row_number().over(user_window_incidence_rev))\
        .withColumn('same_region_as_last_stop_without_break', F.when(F.sum('same_region_as_last_stop').over(user_window_incidence_rev) == F.col('stop_number_filtered_rev') - 1,1).otherwise(0))\
        .withColumn('same_region_as_last_stop_with_break', F.when((F.col('same_region_as_last_stop') == 1) & (F.col('same_region_as_last_stop_without_break') == 0), 1).otherwise(0))\
        .withColumn('cutoff', F.sum('same_region_as_last_stop_with_break').over(user_window_incidence_rev))\
        .withColumn('cutoff_indicator', F.when((F.col('cutoff') == 0) &\
                                               (F.sum('same_region_as_last_stop_without_break').over(user_window_incidence) < F.max('stop_number_filtered').over(user_window_incidence)), 1).otherwise(0))\
        .withColumn('accumulated_incidence_cutoff', F.when((F.col('cutoff_indicator') == 1) & (F.col('same_region_as_last_stop_without_break') == 0), F.col('accumulated_incidence')).otherwise(0))\
        .withColumn('imported_incidence',
                    F.when(F.col('last_stop') == 1, F.sum(F.col('accumulated_incidence_cutoff')).over(user_window_incidence)).otherwise(0))\
        .groupby('region')\
        .agg(F.sum('imported_incidence').alias('imported_incidence'))
      return result

    def origin_destination_matrix_time_longest_only(self, time_filter, frequency):
      user_frequency_window = Window.partitionBy('msisdn', frequency).orderBy('call_datetime')
      result = self.df.where(time_filter)\
        .where((F.col('region_lag') != F.col('region')) | (F.col('region_lead') != F.col('region')))\
        .withColumn('duration_lead', (F.col('call_datetime_lead').cast('long') - F.col('call_datetime').cast('long')))\
        .withColumn('duration', F.col('duration_lead'))\
        .withColumn('duration_next', F.lead('duration').over(user_frequency_window))\
        .withColumn('duration_change_only', F.when(F.col('region') == F.col('region_lead'), F.col('duration_next') + F.col('duration')).otherwise(F.col('duration')))\
        .where(F.col('region_lag') != F.col('region'))\
        .withColumn('max_duration', F.when(F.col('duration_change_only') == F.max(F.col('duration_change_only')).over(user_frequency_window), 1).otherwise(0))\
        .where(F.col('max_duration') == 1)\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.sum(F.col('duration_change_only')).alias('total_duration'),
           F.avg(F.col('duration_change_only')).alias('avg_duration'),
           F.count(F.col('duration_change_only')).alias('count'),
           F.stddev_pop(F.col('duration_change_only')).alias('stddev_duration'))
      return result

    def active_residents_from_specific_period(self, time_filter, frequency, exlusion_start = dt.datetime(2020,3,1), active_only_at_home = True):
        user_window = Window.partitionBy('msisdn').orderBy('call_datetime')
        exclusion_filter = (F.col('call_datetime') >= self.dates['start_date']) &\
                           (F.col('call_datetime') < exlusion_start)
        home_locations = self.assign_home_locations(exclusion_filter, 'constant')\
          .withColumnRenamed('msisdn', 'msisdn2')
        home_location_count = home_locations\
          .groupby('home_region')\
          .agg(F.countDistinct('msisdn2').alias('home_location_count'))\
          .withColumnRenamed('home_region', 'home_region2')
        prep = self.df.where(time_filter)\
          .withColumn('first_observation', F.first('call_datetime').over(user_window))\
          .where(F.col('first_observation') < exlusion_start)\
          .drop('home_region')
        prep = prep\
          .join(home_locations, prep.msisdn == home_locations.msisdn2, 'left')
        if active_only_at_home:
            prep = prep.where(F.col('region') == F.col('home_region'))
        prep = prep\
          .groupby('home_region', frequency)\
          .agg(F.countDistinct('msisdn').alias('count'))
        result = prep\
          .join(home_location_count, prep.home_region == home_location_count.home_region2, 'left')\
          .withColumn('percent_active', F.col('count') / F.col('home_location_count'))\
          .drop('home_region2')
        return result
