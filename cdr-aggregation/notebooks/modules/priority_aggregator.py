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
        level-specific queries such as weighting
    distances_df : a pyspark dataframe. Matrix of distances to be used for
        distance calculation
    table_names : a list. Keep a list of all tables we creat for re-naming
    period_filter : a pyspark filter. Time filter for hourly, daily and monthly
        indicators
    weeks_filter :  a pyspark filter. Time filter for weekly queries, includes
    privacy_filter : an integer. Minimum number of observations to keep statistic
    missing_value_code : an integer. Code for missing regions
    cutoff_days : an integer. Max number of days for leads and lags.
    max_duration : an integer. Max number of days to consider for duration.

    Methods to manage aggregation:
    -----------------------------
    [check inherited methods described in aggregator class]

    run_and_save_all(time_filter, frequency)
        - in this method we run all indicators defines as priority
        - for this we need to supply filter and frequency

    run_and_save_all_frequencies()
        convenience method to run over all frequencies, instead of looping in
            the run_and_save_all method

    run_save_and_rename_all()
        run all frequencies, then rename the resulting table

    attempt_aggregation(indicators_to_produce = 'all')
        - run all priority indicators
        - or specify a dicionary of indicators to produce

    Methods to produce priority indicators:
    --------------------------------------

    transactions(time_filter, frequency)
        - indicator 1

    unique_subscribers(time_filter, frequency)
        - indicator 2

    unique_subscribers_country(time_filter, frequency)
        - indicator 3

    percent_of_all_subscribers_active(time_filter, frequency)
        - indicator 4

    origin_destination_connection_matrix(time_filter, frequency)
        - indicator 5

    unique_subscriber_home_locations(time_filter, frequency)
        - indicator 6 + 11

    mean_distance(time_filter, frequency)
        - indicators 7 + 8

    home_vs_day_location(time_filter, frequency, home_location_frequency)
        - indicator 9

    origin_destination_matrix_time(time_filter, frequency)
        - indicator 10

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
        self.cutoff_days = 7
        self.max_duration = 21

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

      # hourly indicators
      if frequency == 'hour':

        # indicator 1
        self.table_names.append(self.save_and_report(
            self.transactions(time_filter, frequency),
            'transactions_per_' + frequency))

        # indicator 2
        self.table_names.append(self.save_and_report(
            self.unique_subscribers(time_filter, frequency),
            'unique_subscribers_per_' + frequency))

      # daily indicators
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

      # weekly indicators
      elif frequency == 'week':

        # indicator 6
        self.table_names.append(self.save_and_report(
            self.unique_subscriber_home_locations(time_filter, frequency),
            'unique_subscriber_home_locations_per_' + frequency))

        # indicator 8
        self.table_names.append(self.save_and_report(
            self.mean_distance(time_filter, frequency),
            'mean_distance_per_' + frequency))

      # monthly indicators
      elif frequency == 'month':

        # indicator 11
        self.table_names.append(self.save_and_report(
            self.unique_subscriber_home_locations(time_filter, frequency),
            'unique_subscriber_home_locations_per_' + frequency))

      # unkown frequency
      else:
        print('What is the frequency?!')

    # run all priority indicators for all frequencies
    def run_and_save_all_frequencies(self):
      self.run_and_save_all(self.period_filter, 'hour')
      self.run_and_save_all(self.period_filter, 'day')
      self.run_and_save_all(self.weeks_filter, 'week')
      self.run_and_save_all(self.weeks_filter, 'month')

    # run all priority indicators for all frequencies, rename them afterwards
    def run_save_and_rename_all(self):
      self.run_and_save_all_frequencies()
      self.rename_all_csvs()

    def attempt_aggregation(self,
        indicators_to_produce = 'all'):
        """This method handles multiple aggregations in a row.
        It calls the indicator methods to produce indicators with frequencies
        specified in Parameters to the method call.

        Parameters
        ----------
        indicators_to_produce : a string or dictionary. Specify either the string
        'all' or a dictionary in the format:

        'table_name' : ['indicator_name', 'frequency']

         where
            'table_name' is the file name for the resulting csv,
            'indicator_name' is the name of the method for the indicator, and
            'frequency' specifies the frequency at which you wish to compute
         the indicator. Indicator 9 takes a list of lists in the format:

         'table_name' : ['indicator_name',['frequency','home_location_frequency']]

         as value.
        """
        try:
            # if we want to produce all indicators
            if indicators_to_produce == 'all':
              self.run_save_and_rename_all()

            # if we want to produce a single or custom list of indicators
            elif isinstance(indicators_to_produce, dict):
                for table in indicators_to_produce.keys():

                    # get the name for the resulting csv file
                    table_name = indicators_to_produce[table][0]

                    # get the name of the indicator to produce
                    frequency = indicators_to_produce[table][1]

                    # if more than the standard arguments, we are producing
                    # indicator 9 (currently the only indicator that requires
                    # additional arguments, but we are keeping implementation
                    # abstract here in case this changes) and need to prefix
                    # the resulting csv filew ith the home location frequency.
                    # We also need to use the week filter to avoid incomparability
                    # across full and partial weeks.
                    if isinstance(frequency, list):
                      # get home_location_frequency
                      other_args = frequency[1]
                      # get frequency for indicator
                      frequency = frequency[0]

                      # set week filter if we want weekly frequency
                      if frequency == 'week':
                        filter_var = self.weeks_filter
                      else:
                        filter_var = self.period_filter

                      # compute the indicator
                      result = getattr(self, table_name)(filter_var,
                        frequency, **other_args)
                      # try to prefix the resulting table name
                      try:
                        table_name = other_args['home_location_frequency'] \
                        + '_' + table_name
                      except:
                        pass

                    # If we only have standard arguments, then we just check
                    # whether we need a week filter, produce and save the indicator
                    else:
                      if frequency == 'week':
                        filter_var = self.weeks_filter
                      else:
                        filter_var = self.period_filter
                      result = getattr(self,
                        table_name)(filter_var, frequency)
                    # save and rename
                    table_name = self.save_and_rename_one(result, table)
            else:
                print("""Wrong arguments for aggregation attempt. Specify either
                 'all' or a dictionary in the format
                 'table_name' : ['indicator_name', 'frequency']
                 where 'table_name' is the file name for the resulting csv,
                 'indicator_name' is the name of the method for the indicator, and
                 'frequency' specifies the frequency at which you wish to compute
                 the indicator. Indicator 9 takes a list of lists in the format
                 ['indicator_name',['frequency', 'home_location_frequency']] as
                 values.
                 """)
            print('Priority indicators saved.')
        except Exception as e:
            print(e)



    ######## Priority Indicators ########



    #### Indicator 1

    # result:
    # - apply sample period filter
    # - groupby region and frequency
    # - then count observations
    # - apply privacy filter

    def transactions(self, time_filter, frequency):

      result = self.df.where(time_filter)\
        .groupby(frequency, 'region')\
        .count()\
        .where(F.col('count') > self.privacy_filter)

      return result



    #### Indicator 2

    # result:
    # - apply sample period filter
    # - groupby region and frequency
    # - then count distinct sims
    # - apply privacy filter

    def unique_subscribers(self, time_filter, frequency):

      result = self.df.where(time_filter)\
        .groupby(frequency, 'region')\
        .agg(F.countDistinct('msisdn').alias('count'))\
        .where(F.col('count') > self.privacy_filter)

      return result



    #### Indicator 3

    # result:
    # - apply sample period filter
    # - groupby frequency
    # - then count distinct sims
    # - apply privacy filter

    def unique_subscribers_country(self, time_filter, frequency):

      result = self.df.where(time_filter)\
        .groupby(frequency)\
        .agg(F.countDistinct('msisdn').alias('count'))\
        .where(F.col('count') > self.privacy_filter)

      return result



    #### Indicator 4

    # prep:
    # - apply sample period filter
    # - then count distinct sims over full sample period

    # result:
    # - compute distinct sims per day
    # - calculate percentage of distinct sim per day / distinct sim over full sample period

    def percent_of_all_subscribers_active(self, time_filter, frequency):

      prep = self.df.where(time_filter)\
        .select('msisdn')\
        .distinct()\
        .count()

      result = self.unique_subscribers_country(
        time_filter, frequency).withColumn('percent_active',
        F.col('count') / prep)

      return result



    #### Indicator 5

    # assert correct frequency

    # result (intra-day od):
    # - get intra-day od matrix using flowminder definition

    # prep (inter-day od):
    # - apply sample period filter
    # - create timestamp lag per user
    # - create day lag per user, with a max calue of 7 days
    # - filter for observations that involve a day change (cause we have intra-day already)
    # - also filter for region changes only, since we are computing od matrix
    # - groupby o(rigin) and (d)estination, and frequency
    # - count observations
    # - apply privacy filter

    # result (joining intra-day od (result) and inter-day od (prep)):
    # - join on o, d, and frequency
    # - fill columns for NA's that arose in merge, so that we have complete columns
    # - compute total od summing intra and inter od count

    def origin_destination_connection_matrix(self, time_filter, frequency):

      assert frequency == 'day', 'This indicator is only defined for daily frequency'

      result = self.spark.sql(self.sql_code['directed_regional_pair_connections_per_day'])

      prep = self.df.where(time_filter)\
        .withColumn('call_datetime_lag', F.lag('call_datetime').over(user_window))\
        .withColumn('day_lag',
          F.when((F.col('call_datetime').cast('long') - \
          F.col('call_datetime_lag').cast('long')) <= (self.cutoff_days * 24 * 60 * 60),
          F.lag('day').over(user_window))\
          .otherwise(F.col('day')))\
        .where((F.col('region_lag') != F.col('region')) & ((F.col('day') > F.col('day_lag'))))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.count(F.col('msisdn')).alias('od_count'))\
        .where(F.col('od_count') > self.privacy_filter)

      result = result.join(prep, (prep.region == result.region_to)\
                           & (prep.region_lag == result.region_from)\
                           & (prep.day == result.connection_date), 'full')\
        .withColumn('region_to', F.when(F.col('region_to').isNotNull(),
            F.col('region_to')).otherwise(F.col('region')))\
        .withColumn('region_from', F.when(F.col('region_from').isNotNull(),
            F.col('region_from')).otherwise(F.col('region_lag')))\
        .withColumn('connection_date', F.when(F.col('connection_date').isNotNull(),
            F.col('connection_date')).otherwise(F.col('day')))\
        .na.fill({'od_count' : 0, 'subscriber_count' : 0})\
        .withColumn('total_count', F.col('subscriber_count') + F.col('od_count'))\
        .drop('region').drop('region_lag').drop('day')

      return result



    #### Indicator 6 helper method to find home locations for a given frequency

    # define user-day and user-frequency windows

    # result:
    # - apply sample period filter
    # - Get last timestamp of the day for each user
    # - Dummy when an observation is the last of the day
    # - Count how many times a region is the last of the day
    # - Dummy for the region with highest count
    # - Keep only the region with higest count
    # - Keep only one observation of that region

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
        .agg(F.sum('last_region').alias('last_region_count'))\
        .withColumn('modal_region',
            F.when(F.first('last_region_count').over(user_frequency) == \
            F.col('last_region_count'),1).otherwise(0))\
        .where(F.col('modal_region') == 1)\
        .groupby('msisdn', frequency)\
        .agg(F.last('region').alias('home_region'))

      return result



    #### Indicator 6 + 11

    # result:
    # - find home locations using helper method
    # - group by frequency and home region
    # - count observations
    # - apply privacy_filter

    def unique_subscriber_home_locations(self, time_filter, frequency):

      result = self.assign_home_locations(time_filter, frequency)\
        .groupby(frequency, 'home_region')\
        .count()\
        .where(F.col('count') > self.privacy_filter)

      return result



    #### Indicator 7 + 8

    # prep:
    # - apply sample period filter
    # - create time and location lags

    # result:
    # - join prep with distances matrix
    # - group by user, frequency and home region
    # - sum distances
    # - group by frequency and home region
    # - get mean and standard deviation of distance

    def mean_distance(self, time_filter, frequency):

      prep = self.df.where(time_filter)\
        .withColumn('location_id_lag', F.lag('location_id').over(user_window))\
        .withColumn('call_datetime_lag', F.lag('call_datetime').over(user_window))\
        .withColumn('location_id_lag',
          F.when((F.col('call_datetime').cast('long') - \
          F.col('call_datetime_lag').cast('long')) <= (self.cutoff_days * 24 * 60 * 60),
          F.lag('location_id').over(user_window))\
          .otherwise(None))

      result = prep.join(self.distances_df,
             (prep.location_id==self.distances_df.destination) &\
             (prep.location_id_lag==self.distances_df.origin),
             'left')\
        .groupby('msisdn', 'home_region', frequency)\
        .agg(F.sum('distance').alias('distance'))\
        .groupby('home_region', frequency)\
        .agg(F.mean('distance').alias('mean_distance'),
             F.stddev_pop('distance').alias('stdev_distance'))

      return result



    #### Indicator 9

    # get home_locations

    # prep (day locations):
    # - create timestamp lead with max value set to end of sample period
    # - calculate duration using lead timestamp
    # - constrain max duration to cutoff_days
    # - group by user, region and frequency (keeping home_location_frequency)
    # - get total duration
    # - group by user and frequency (keeping home_location_frequency)
    # - get the region with the longest duration
    # - rename vars to avoid duplicates in merge

    # result:
    # - merge home with day locations per user and home_location_frequency
    # - fill NAs that arose in merge
    # - group by home_region, region and frequency
    # - caclulate mean, standard deviation of duration and count sims
    # - apply privacy filter

    def home_vs_day_location(self, time_filter, frequency, home_location_frequency = 'week', **kwargs):

      home_locations = self.assign_home_locations(time_filter, home_location_frequency)

      prep = self.df.where(time_filter)\
        .withColumn('call_datetime_lead',
            F.when(F.col('call_datetime_lead').isNull(),
            self.dates['end_date'] + dt.timedelta(1)).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration', (F.col('call_datetime_lead').cast('long') - \
            F.col('call_datetime').cast('long')))\
        .withColumn('duration', F.when(F.col('duration') <= \
            (self.cutoff_days * 24 * 60 * 60), F.col('duration')).otherwise(0))\
        .groupby('msisdn', 'region', frequency, home_location_frequency)\
        .agg(F.sum('duration').alias('total_duration'))\
        .orderBy('msisdn', frequency, 'total_duration')\
        .groupby('msisdn', frequency, home_location_frequency)\
        .agg(F.last('region').alias('region'), F.last('total_duration').alias('duration'))\
        .withColumnRenamed('msisdn', 'msisdn2')\
        .withColumnRenamed(home_location_frequency, home_location_frequency + '2')

      result = prep.join(home_locations,
            (prep.msisdn2 == home_locations.msisdn) & \
            (prep[home_location_frequency + '2'] == \
            home_locations[home_location_frequency]), 'left')\
        .na.fill({'home_region' : self.missing_value_code})\
        .groupby(frequency, 'region', 'home_region')\
        .agg(F.mean('duration').alias('mean_duration'),
            F.stddev_pop('duration').alias('stdev_duration'),
            F.count('msisdn').alias('count'))\
        .where(F.col('count') > self.privacy_filter)

      return result



    #### Indicator 10

    # result:
    # - apply sample period filter
    # - drop all observations that don't imply a region change from lag or lead
    # - create timestamp lead, recplaing missing values with end of sample period
    # - calculate duration
    # - constrain duration to max seven days
    # - get the lead duration
    # - sum durations for stops without lead switch
    # - set max duration to 21 days
    # - get lag duration
    # - drop all observations with lead rather than lag switch
    # - group by frequency and origin (lag) and destination (lead)
    # - calculate avg, std, sums and counts of o and d durations

    def origin_destination_matrix_time(self, time_filter, frequency):

      user_frequency_window = Window.partitionBy('msisdn').orderBy('call_datetime')

      result = self.df.where(time_filter)\
        .where((F.col('region_lag') != F.col('region')) | \
            (F.col('region_lead') != F.col('region')) | \
            (F.col('call_datetime_lead').isNull()))\
        .withColumn('call_datetime_lead',
            F.when(F.col('call_datetime_lead').isNull(),
            self.dates['end_date'] + dt.timedelta(1)).otherwise(F.col('call_datetime_lead')))\
        .withColumn('duration', (F.col('call_datetime_lead').cast('long') - \
            F.col('call_datetime').cast('long')))\
        .withColumn('duration', F.when(F.col('duration') <= \
            (self.cutoff_days * 24 * 60 * 60), F.col('duration')).otherwise(0))\
        .withColumn('duration_next', F.lead('duration').over(user_frequency_window))\
        .withColumn('duration_change_only', F.when(F.col('region') == \
            F.col('region_lead'), F.col('duration_next') + \
            F.col('duration')).otherwise(F.col('duration')))\
        .withColumn('duration_change_only',
            F.when(F.col('duration_change_only') > \
            (self.max_duration * 24 * 60 * 60),
            (self.max_duration * 24 * 60 * 60)).otherwise(F.col('duration_change_only')))\
        .withColumn('duration_change_only_lag',
            F.lag('duration_change_only').over(user_frequency_window))\
        .where(F.col('region_lag') != F.col('region'))\
        .groupby(frequency, 'region', 'region_lag')\
        .agg(F.sum('duration_change_only').alias('total_duration_destination'),
           F.avg('duration_change_only').alias('avg_duration_destination'),
           F.count('duration_change_only').alias('count_destination'),
           F.stddev_pop('duration_change_only').alias('stddev_duration_destination'),
           F.sum('duration_change_only_lag').alias('total_duration_origin'),
           F.avg('duration_change_only_lag').alias('avg_duration_origin'),
           F.count('duration_change_only_lag').alias('count_origin'),
           F.stddev_pop('duration_change_only_lag').alias('stddev_duration_origin'))\
        .where((F.col('count_origin') > self.privacy_filter) & (F.col('count_destination') > self.privacy_filter))

      return result
