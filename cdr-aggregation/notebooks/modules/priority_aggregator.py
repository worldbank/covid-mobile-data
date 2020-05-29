# Load modules depending whether we are on docker or on databricks
import os
if os.environ['HOME'] != '/root':
    from modules.aggregator import *
    from modules.import_packages import *
    from modules.utilities import *
else:
    databricks = True

class priority_aggregator(aggregator):
    """This class inherits from the aggregator class.
    It is the main class to handle priority indicators.

    Attributes
    ----------
    [check inherited attributes described in aggregator class]

    re_create_vars : a boolean. Whether to re-create/create a parquet file with
        intermediary steps to save computation. Do this whenever you've changed
        code.
    level : a string. Level this aggregator is supposed to run on, to hande
        level-specific queries such as incidence or weighting
    distances_df : a pyspark dataframe. Matrix of distances to be used for
        distance calculation
    table_names : a list. Keep a list of all tables we creat for re-naming
    period_filter : a pyspark filter. Time filter for hourly, daily and monthly
        indicators
    weeks_filter :  a pyspark filter. Time filter for weekly queries, includes
        only full weeks
    incidence : a pyspark dataframe. Incididence observed; for admin2 only

    Methods
    -------
    [check inherited methods described in aggregator class]

    run_and_save_all(time_filter, frequency)
        - as opposed to flowminder indicators, we don not produce all indicators
            this aggregator has to offer
        - we cherry pick only priority indicators in this method
        - for this we need to supply filter and frequency

    run_and_save_all_frequencies()
        convenience method to run over all frequencies, instead of looping in
            the run_and_save_all method

    run_save_and_rename_all()
        run all frequencies, then rename the resulting table

    attempt_aggregation(indicators_to_produce = 'all', no_of_attempts = 4)
        - run all priority indicators
        - handle errors and retries
        - we can drop this method when we find a better way of dealing with
            databrick time-out problems

    """

    def __init__(self,
                 result_stub,
                 datasource,
                 regions,
                 re_create_vars = False):
        """
        Parameters
        ----------
        result_stub : where to save results
        datasource : holds all dataframes and paths required
        regions : admin level this aggregator will be used for
        intermediate_tables : tables that we don't want written to csv
        re_create_vars : whether to re-create/create a parquet file with
        intermediary steps to save computation
        """

        # initiate with parent init
        super().__init__(result_stub,datasource,regions)

        # set the admin level
        if regions == 'admin2_tower_map':
            self.level = 'admin2'
        elif regions == 'admin3_tower_map':
            self.level = 'admin3'
        else:
            self.level = 'voronoi'

        # set the distance matrix for distance-based queries
        self.distances_df = datasource.distances

        # initiate a list for names of tables we produce
        self.table_names = []

        # set time filter based on date range given in config file, add one day
        #to end date to make it inclusive
        self.period_filter = (F.col('call_datetime') >= \
                             self.dates['start_date']) &\
                             (F.col('call_datetime') <= \
                             self.dates['end_date'] + dt.timedelta(1))

        # we only include full weeks, these have been inherited
        self.weeks_filter = (F.col('call_datetime') >= \
                            self.dates['start_date_weeks']) &\
                            (F.col('call_datetime') <= \
                            self.dates['end_date_weeks'] + dt.timedelta(1))

        self.privacy_filter = 15
        self.missing_value_code = 99999

        # for admin 2, we also have an incidence file
        if self.level == 'admin2':
            try:
                self.incidence = getattr(datasource, 'admin2_incidence')
            except Exception as e:
                print('No incidence file added.')
                pass

        # Check whether a parquet file with variable has already been created,
        # this differs from databricks to docker
        if databricks:
          try:
            # does the file exist?
            dbutils.fs.ls(os.path.join(self.datasource.standardize_path,
                self.datasource.parquetfile_vars + self.level + '.parquet'))
            create_vars = False
          except Exception as e:
            create_vars = True

        else:
            create_vars = (os.path.exists(os.path.join(
                self.datasource.standardize_path,
                self.datasource.parquetfile_vars + self.level + '.parquet')
                ) == False)

        # If the variable file doesn't exist yet, and we don't want to recreate
        # it, create it. These vars are used in most queries so we save them to
        # disk to save on query execution time
        if (re_create_vars | create_vars):
            print('Creating vars parquet-file...')
            self.df = self.calls.join(self.cells, self.calls.location_id == \
                self.cells.cell_id, how = 'left').drop('cell_id')\
              .join(self.spark.sql(self.sql_code['home_locations'])\
              .withColumnRenamed('region', 'home_region'), 'msisdn', 'left')\
              .orderBy('msisdn', 'call_datetime')\
              .withColumn('region_lag', F.lag('region').over(user_window))\
              .withColumn('region_lead', F.lead('region').over(user_window))\
              .withColumn('call_datetime_lag',
                F.lag('call_datetime').over(user_window))\
              .withColumn('call_datetime_lead',
                F.lead('call_datetime').over(user_window))\
              .withColumn('hour_of_day', F.hour('call_datetime').cast('byte'))\
              .withColumn('hour', F.date_trunc('hour', F.col('call_datetime')))\
              .withColumn('week', F.date_trunc('week', F.col('call_datetime')))\
              .withColumn('month', F.date_trunc('month', F.col('call_datetime')))\
              .withColumn('constant', F.lit(1).cast('byte'))\
              .withColumn('day', F.date_trunc('day', F.col('call_datetime')))\
              .na.fill({'region' : self.missing_value_code ,
                        'region_lag' : self.missing_value_code ,
                        'region_lead' : self.missing_value_code })

            self.df = save_and_load_parquet(self.df,
                os.path.join(self.datasource.standardize_path,
                    self.datasource.parquetfile_vars + self.level + '.parquet'),
                    self.datasource)

        ## When we don't want to re-create the variables parquet, we just load it
        else:
            self.df = self.spark.read.format("parquet").load(
                os.path.join(self.datasource.standardize_path,
                    self.datasource.parquetfile_vars + self.level + '.parquet'))

    # Run and save all priority indicators (list keeps on changing so there's
    # some commented lines)
    def run_and_save_all(self, time_filter, frequency):
      if frequency == 'hour':
        # indicator 1
        self.table_names.append(self.save_and_report(
            self.transactions(time_filter, frequency),
            'transactions_per_' + frequency))
        # indicator 2
        self.table_names.append(self.save_and_report(
            self.unique_subscribers(time_filter, frequency),
            'unique_subscribers_per_' + frequency))

      elif frequency == 'day':
        # indicator 3
        self.table_names.append(self.save_and_report(
            self.unique_subscribers(time_filter, frequency),
            'unique_subscribers_per_' + frequency))
        # indicator 4
        self.table_names.append(self.save_and_report(
            self.percent_of_all_subscribers_active(time_filter, frequency),
            'percent_of_all_subscribers_active_per_' + frequency))
        # indicator 5
        self.table_names.append(self.save_and_report(
            self.origin_destination_connection_matrix(time_filter, frequency),
            'origin_destination_connection_matrix_per_' + frequency))
        # indicator 7
        self.table_names.append(self.save_and_report(
            self.mean_distance(time_filter, frequency),
            'mean_distance_per_' + frequency))
        # indicator 9
        self.table_names.append(self.save_and_report(
            self.home_vs_day_location(time_filter, frequency,
            home_location_frequency = 'week'),
            'week_home_vs_day_location_per_' + frequency))
        self.table_names.append(self.save_and_report(
            self.home_vs_day_location(time_filter, frequency,
            home_location_frequency = 'month'),
            'month_home_vs_day_location_per_' + frequency))
        # indicator 10
        self.table_names.append(self.save_and_report(
            self.origin_destination_matrix_time(time_filter, frequency),
            'origin_destination_matrix_time_per_' + frequency))
      elif frequency == 'week':
        # indicator 6
        self.table_names.append(self.save_and_report(
            self.unique_subscriber_home_locations(time_filter, frequency),
            'unique_subscriber_home_locations_per_' + frequency))
        # indicator 8
        self.table_names.append(self.save_and_report(
            self.mean_distance(time_filter, frequency),
            'mean_distance_per_' + frequency))
      elif frequency == 'month':
        # indicator 11
        self.table_names.append(self.save_and_report(
            self.unique_subscriber_home_locations(time_filter, frequency),
            'unique_subscriber_home_locations_per_' + frequency))
      else:
        print('What is the frequency')

    # run all priority indicators for all frequencies
    def run_and_save_all_frequencies(self):
      self.run_and_save_all(self.period_filter, 'day')
      self.run_and_save_all(self.period_filter, 'hour')
      self.run_and_save_all(self.weeks_filter, 'week')
      self.run_and_save_all(self.weeks_filter, 'month')

    # run all priority indicators for all frequencies, rename them afterwards
    def run_save_and_rename_all(self):
      self.run_and_save_all_frequencies()
      self.rename_all_csvs()

    # Handle errors and retries. We can drop this method soon, as I don't think
    # we have the time-out problems on Databricks anymore
    def attempt_aggregation(self,
        indicators_to_produce = 'all',
        no_of_attempts = 4):
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

                          result = getattr(self, table_name)(filter_var,
                            frequency, **other_args)
                          try:
                            table_name = other_args['home_location_frequency'] \
                            + '_' + table_name
                          except:
                            pass

                        # only the standard arguments
                        else:
                          if frequency == 'week':
                            filter_var = self.weeks_filter
                          else:
                            filter_var = self.period_filter
                          result = getattr(self,
                            table_name)(filter_var, frequency)
                        # save and rename
                        table_name = table_name  + '_per_' + frequency
                        table_name = self.save_and_rename_one(result, table_name)
                print('Priority indicators saved.')
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
        .where(F.col('count') > self.privacy_filter)
      return result

    ## Indicator 2 + 3
    def unique_subscribers(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby(frequency, 'region')\
        .agg(F.countDistinct('msisdn').alias('count'))\
        .where(F.col('count') > self.privacy_filter)
      return result

    ## Indicator 3
    def unique_subscribers_country(self, time_filter, frequency):
      result = self.df.where(time_filter)\
        .groupby(frequency)\
        .agg(F.countDistinct('msisdn').alias('count'))\
        .where(F.col('count') > self.privacy_filter)
      return result

    ## Indicator 4
    def percent_of_all_subscribers_active(self, time_filter, frequency):
      prep = self.df.where(time_filter)\
        .select('msisdn')\
        .distinct()\
        .count()
      result = self.unique_subscribers_country(
        time_filter, frequency).withColumn('percent_active',
        F.col('count') / prep)
      return result

    ## Indicator 5
    def origin_destination_connection_matrix(self, time_filter, frequency):
    # This indicator is for now only defined for daily frequencies
      assert frequency == 'day', \
      'This indicator is only defined for daily frequency'
    # Create df with connections that happen within the same day,
    # using the flowminder definition
      od_intra_day = self.spark.sql(
        self.sql_code['directed_regional_pair_connections_per_day'])

    # Function to create a df with od_count connection counts of connections
    # that do not happen within one day. Specify a cutoff number of days for this.
    # We drop all connections where the day_lag < day
      def od_inter_day(suffix = '', cutoff_days = 90):
        result = self.df.where(time_filter)\
          .withColumn('day_lag', F.lag('day').over(user_window))\
          .withColumn('day_lag' + suffix,
            F.when((F.col('day').cast('long') - \
            F.col('day_lag').cast('long')) <= (cutoff_days * 24 * 60 * 60),
            F.col('day_lag')).otherwise(F.col('day')))\
          .where((F.col('region_lag') != F.col('region')) &\
           ((F.col('day') > F.col('day_lag'  + suffix))))\
          .groupby(frequency, 'region', 'region_lag')\
          .agg(F.count(F.col('msisdn')).alias('od_count'  + suffix))\
          .where(F.col('od_count' + suffix) > self.privacy_filter)
        return result

    # Function to join dfs with differing cutoff days,
    # as well as the intra-daily connections
      def join_counts(df, result):
          result = result.join(df, (df.region == result.region_to)\
                               & (df.region_lag == result.region_from)\
                               & (df.day == result.connection_date), 'full')\
            .withColumn('region_to',
                F.when(F.col('region_to').isNotNull(),
                F.col('region_to')).otherwise(F.col('region')))\
            .withColumn('region_from',
                F.when(F.col('region_from').isNotNull(),
                F.col('region_from')).otherwise(F.col('region_lag')))\
            .withColumn('connection_date',
                F.when(F.col('connection_date').isNotNull(),
                F.col('connection_date')).otherwise(F.col('day')))\
            .drop('region').drop('region_lag').drop('day')
          return result

    # Applying above functions, and bringing it all together into one df
      unrestricted = od_inter_day(suffix = '', cutoff_days = 90)
      restricted_seven = od_inter_day(suffix = '_seven', cutoff_days = 7)
      prep = join_counts(unrestricted, od_intra_day)
      result = join_counts(restricted_seven, prep)

    # Filling in NA's with 0s
      result = result.na.fill({'od_count' : 0,
                               'od_count_seven' : 0,
                               'subscriber_count' : 0})\
                 .withColumn('total_count',
                    F.col('subscriber_count') + F.col('od_count'))\

      return result

    ## Indicator 6 helper method
    def assign_home_locations(self, time_filter, frequency):
    # A window for each user-day with desending time
      user_day = Window\
        .orderBy(F.desc_nulls_last('call_datetime'))\
        .partitionBy('msisdn', 'day')
    # A window for each user-frequency with descending region count
      user_frequency = Window\
        .orderBy(F.desc_nulls_last('last_region_count'))\
        .partitionBy('msisdn', frequency)

        # Steps:
        # Get last timestamp of the day for each user
        # Dummy when an observation is the last of the day
        # Count how many times a region is the last of the day
        # Dummy for the region with highest count
        # Keep only the region with higest count
        # Keep only one observation of that region

      result = self.df.where(time_filter)\
        .na.fill({'region' : self.missing_value_code })\
        .withColumn('last_timestamp',
            F.first('call_datetime').over(user_day))\
        .withColumn('last_region',
            F.when(F.col('call_datetime') == F.col('last_timestamp'),
             1).otherwise(0))\
        .orderBy('call_datetime')\
        .groupby('msisdn', frequency, 'region')\
        .agg(F.sum('last_region').alias('last_region_count'))\
        .withColumn('modal_region',
            F.when(F.first('last_region_count').over(user_frequency) == \
            F.col('last_region_count'),1).otherwise(0))\
        .where(F.col('modal_region') == 1)\
        .groupby('msisdn', frequency)\
        .agg(F.last('region').alias('home_region'))
      return result

    ## Indicator 6 + 11
    def unique_subscriber_home_locations(self, time_filter, frequency):
      result = self.assign_home_locations(time_filter, frequency)\
        .groupby(frequency, 'home_region')\
        .count()\
        .where(F.col('count') > self.privacy_filter)
      return result

    ## Indicator 7 + 8
    def mean_distance(self, time_filter, frequency):

      def calculate_distance_stats(cutoff_days = 90, suffix = ''):
          prep = self.df.where(time_filter)\
            .withColumn('day_lag', F.lag('day').over(user_window))\
            .withColumn('location_id_lag' + suffix,
                F.lag('location_id').over(user_window))\
            .withColumn('location_id_lag' + suffix,
                F.when((F.col('day').cast('long') - \
                F.col('day_lag').cast('long')) <= (cutoff_days * 24 * 60 * 60),
                F.lag('location_id').over(user_window))\
                .otherwise(None))
          result = prep.join(self.distances_df,
                 (prep.location_id==self.distances_df.destination) &\
                 (prep['location_id_lag' + suffix]==self.distances_df.origin),
                 'left')\
            .groupby('msisdn', 'home_region', frequency)\
            .agg(F.sum('distance').alias('distance'))\
            .groupby('home_region', frequency)\
            .agg(F.mean('distance').alias('mean_distance' + suffix),
                F.stddev_pop('distance').alias('stdev_distance' + suffix))\
            .withColumnRenamed(frequency, frequency + suffix)\
            .withColumnRenamed('home_region', 'home_region' + suffix)
          return result
    # caclulate distance with cutoff
      prep = calculate_distance_stats(cutoff_days = 7, suffix = '_seven')
    # caclulate distance without cutoff
      result = calculate_distance_stats()
    # merge distances with and without cutoff
      result = result.join(prep,
        (prep['home_region' + '_seven'] == result.home_region) & \
        (prep[frequency + '_seven'] == result[frequency]),'full')\
        .drop('home_region' + '_seven').drop(frequency + '_seven')
      return result

   ## Indicator 9
    def home_vs_day_location(self,
                            time_filter,
                            frequency,
                            home_location_frequency = 'week',
                            **kwargs):
    # get home locations
      home_locations = self.assign_home_locations(time_filter,
        home_location_frequency)

    # find location where most time is spend in a day
      def find_day_location(cutoff_days = 90, suffix = ''):
          prep = self.df.where(time_filter)\
            .withColumn('call_datetime_lead',
                F.when(F.col('call_datetime_lead').isNull(),
                self.dates['end_date']).otherwise(F.col('call_datetime_lead')))\
            .withColumn('duration', (F.col('call_datetime_lead').cast('long') - \
                F.col('call_datetime').cast('long')))\
            .withColumn('duration', F.when(F.col('duration') <= \
                (cutoff_days * 24 * 60 * 60), 'duration').otherwise(0))\
            .groupby('msisdn', 'region', frequency, home_location_frequency)\
            .agg(F.sum('duration').alias('total_duration'))\
            .orderBy('msisdn', frequency, 'total_duration')\
            .groupby('msisdn', frequency, home_location_frequency)\
            .agg(F.last('region').alias('region'),
                F.last('total_duration').alias('duration'))\
            .withColumnRenamed(frequency, frequency + suffix)\
            .withColumnRenamed('msisdn', 'msisdn' + suffix)\
            .withColumnRenamed(home_location_frequency,
                home_location_frequency + suffix)
          return prep

    # join home location and calc stats
      def prepare_prep_and_join_to_home_locations(cutoff_days = 90,
                                          suffix = '',
                                          home_locations = home_locations,
                                          frequency = frequency,
                                          home_location_frequency = \
                                            home_location_frequency):
        prep = find_day_location(cutoff_days, suffix)
        result = prep.join(home_locations,
            (prep['msisdn' + suffix] == home_locations.msisdn) &\
            (prep[home_location_frequency + suffix] ==\
             home_locations[home_location_frequency]), 'left')\
             .drop('msisdn' + suffix)\
             .drop(home_location_frequency + suffix)\
             .na.fill({'home_region' : self.missing_value_code })\
             .groupby(frequency + suffix, 'region', 'home_region')\
             .agg(F.mean('duration').alias('mean_duration' + suffix),
                  F.stddev_pop('duration').alias('stdev_duration' + suffix),
                  F.count('msisdn').alias('count' + suffix))\
             .where(F.col('count' + suffix) > self.privacy_filter)\
             .withColumnRenamed('region', 'region' + suffix)\
             .withColumnRenamed('home_region', 'home_region' + suffix)
        return result

    # Applying above functions, and bringing it all together into one df
      result = prepare_prep_and_join_to_home_locations(90, suffix = '_full')
      result_seven = prepare_prep_and_join_to_home_locations(7, suffix = '_seven')
      result = result.join(result_seven,
        (result[frequency + '_full'] == result_seven[frequency + '_seven']) &\
        (result.region_full == result_seven.region_seven) &\
        (result.home_region_full == result_seven.home_region_seven), 'full')\
        .drop(frequency + '_seven')\
        .drop('region_seven')\
        .drop('home_region_seven')\
        .withColumnRenamed(frequency + '_full', frequency)\
        .withColumnRenamed('region_full', 'region')\
        .withColumnRenamed('home_region_full', 'home_region')
      return result

    ## Indicator 10
    def origin_destination_matrix_time(self, time_filter, frequency):
      user_frequency_window = Window.partitionBy('msisdn').orderBy('call_datetime')

      # create duration vars used for all vars
      prep = self.df.where(time_filter)\
        .where((F.col('region_lag') != F.col('region')) |\
            (F.col('region_lead') != F.col('region')) |\
            (F.col('call_datetime_lead').isNull()))\
        .withColumn('call_datetime_lead',
            F.when(F.col('call_datetime_lead').isNull(),
            self.dates['end_date']).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration', (F.col('call_datetime_lead').cast('long') - \
            F.col('call_datetime').cast('long')))\
        .withColumn('duration_next',
            F.lead('duration').over(user_frequency_window))\
        .withColumn('duration_change_only',
            F.when(F.col('region') == F.col('region_lead'),
            F.col('duration_next') + F.col('duration'))\
            .otherwise(F.col('duration')))\
        .withColumn('duration_change_only',
            F.when(F.col('duration_change_only') > (21 * 24 * 60 * 60),
            (21 * 24 * 60 * 60)).otherwise(F.col('duration_change_only')))\
        .withColumn('duration_change_only_lag',
            F.lag('duration_change_only').over(user_frequency_window))

      # first create vars for full period
      prep2 = prep\
            .where(F.col('region_lag') != F.col('region'))\
            .groupby(frequency, 'region', 'region_lag')\
            .agg(F.sum('duration_change_only')\
                .alias('total_duration_destination'),
               F.avg('duration_change_only')\
                .alias('avg_duration_destination'),
               F.count('duration_change_only')\
                .alias('count_destination'),
               F.stddev_pop('duration_change_only')\
                .alias('stddev_duration_destination'),
               F.sum('duration_change_only_lag')\
                .alias('total_duration_origin'),
               F.avg('duration_change_only_lag')\
                .alias('avg_duration_origin'),
               F.count('duration_change_only_lag')\
                .alias('count_origin'),
               F.stddev_pop('duration_change_only_lag')\
                .alias('stddev_duration_origin'))

      # then create vars for last seven day period
      prep3 = prep\
            .where((F.col('region_lag') != F.col('region')) &\
                   ((F.col('call_datetime').cast('long') - \
                    F.col('call_datetime_lag').cast('long')) <= \
                    (7 * 24 * 60 * 60)))\
            .groupby(frequency, 'region', 'region_lag')\
            .agg(F.sum('duration_change_only')\
                .alias('total_duration_destination_seven'),
               F.avg('duration_change_only')\
                .alias('avg_duration_destination_seven'),
               F.count('duration_change_only')\
                .alias('count_destination_seven'),
               F.stddev_pop('duration_change_only')\
                .alias('stddev_duration_destination_seven'),
               F.sum('duration_change_only_lag')\
                .alias('total_duration_origin_seven'),
               F.avg('duration_change_only_lag')\
                .alias('avg_duration_origin_seven'),
               F.count('duration_change_only_lag')\
                .alias('count_origin_seven'),
               F.stddev_pop('duration_change_only_lag')\
                .alias('stddev_duration_origin_seven'))\
            .withColumnRenamed('region', 'region3')\
            .withColumnRenamed('region_lag', 'region_lag3')\
            .withColumnRenamed(frequency, frequency + '3')

      # combine the results
      result = prep2.join(prep3, (prep2.region == prep3.region3)\
                           & (prep2.region_lag == prep3.region_lag3)\
                           & (prep2[frequency] == prep3[frequency + '3']),
                           'full')\
                    .drop('region3').drop('region_lag3').drop(frequency + '3')

      return result
