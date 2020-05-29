# Databricks notebook source
# Class to help counting outliers
class outlier_counter:
    """Class to count outliers

    Attributes
    ----------
    calls : a dataframe. which data to process
    spark : an initialised spark connection.
    thresholds : a dictionary with outlier thresholds to be used.

    Methods
    -------
    count()
        count outliers and print results

    print_results(df)
        print results of outlier counts
    """

    def __init__(self,
                 calls,
                 spark = spark,
                 thresholds = {'min_transactions' : 3,
                               'max_avg_transactions' : 100,
                               'max_transactions_in_single_day' : 200}):
        """
        Parameters
        ----------

        """
        self.calls = calls
        self.spark = spark
        self.counts = {}
        self.dfs = {}
        self.thresholds = thresholds


    def count(self):
      # count all records
      self.counts['all_records'] = self.calls.count()

      # count of days in dataframe
      self.counts['number_of_days'] = self.calls.select('call_date').distinct().count()

      # Count number of distinct users
      self.counts['distinct_ids'] = self.calls.select('msisdn').distinct().count()

      # Get # of records per user
      self.dfs['records_per_user'] = self.calls.groupby('msisdn').count()

      # Get # of records per user per day
      self.dfs['records_per_user_per_day'] = self.calls.groupby('msisdn', 'call_date').count()

      # Identify daily usage outlier msidsdn
      self.dfs['too_few_transactions'] = self.dfs['records_per_user']\
        .where(F.col('count') < self.thresholds['min_transactions'])\
        .select('msisdn').distinct()
      self.dfs['too_many_avg_transactions'] = self.dfs['records_per_user']\
        .where(F.col('count') > (self.counts['number_of_days'] * \
        self.thresholds['max_avg_transactions']))\
        .select('msisdn').distinct() # more than __ calls and texts per day on average
      self.dfs['too_many_transactions_in_single_day'] = \
        self.dfs['records_per_user_per_day']\
        .where(F.col('count') > self.thresholds['max_transactions_in_single_day'])\
        .select('msisdn').distinct() # more than __ calls and texts in a single day

      # Count the outlier accounts
      self.counts['too_few_transactions'] = \
        self.dfs['too_few_transactions'].count()
      self.counts['too_many_avg_transactions'] = \
        self.dfs['too_many_avg_transactions'].count()
      self.counts['too_many_transactions_in_single_day'] = \
        self.dfs['too_many_transactions_in_single_day'].count()

      # Caclulate the outlier account fraction
      self.counts['too_few_transactions_fraction'] = \
        self.counts['too_few_transactions'] / self.counts['distinct_ids']
      self.counts['too_many_avg_transactions_fraction'] = \
        self.counts['too_many_avg_transactions'] / self.counts['distinct_ids']
      self.counts['too_many_transactions_in_single_day_fraction'] = \
        self.counts['too_many_transactions_in_single_day'] / self.counts['distinct_ids']

      # Keep only ids that aren't among the outlier accounts
      self.filtered_transactions = self.calls.join(self.dfs['too_few_transactions'],
                                   self.calls['msisdn'] == \
                                   self.dfs['too_few_transactions']['msisdn'],
                                   how ='leftanti').select(self.calls.columns[0:])
      self.filtered_transactions = self.filtered_transactions\
                                    .join(self.dfs['too_many_avg_transactions'],
                                    self.filtered_transactions['msisdn'] == \
                                    self.dfs['too_many_avg_transactions']['msisdn'],
                                    how ='leftanti')\
                                    .select(self.filtered_transactions.columns[0:])
      self.filtered_transactions = self.filtered_transactions\
                                    .join(self.dfs['too_many_transactions_in_single_day'],
                                   self.filtered_transactions['msisdn'] == \
                                   self.dfs['too_many_transactions_in_single_day']['msisdn'],
                                   how ='leftanti')\
                                   .select(self.filtered_transactions.columns[0:])

      # count how many we kept and dropped
      self.counts['filtered_transactions'] = self.filtered_transactions.count()
      self.counts['dropped_calls'] = \
        self.counts['all_records'] - self.counts['filtered_transactions']
      self.print_results()


    def print_results(self):
      print('Total number of unique SIMs: {:,}'.format(self.counts['distinct_ids']))
      print('Number of SIMs with less than {} transactions: {:,}'\
        .format(self.thresholds['min_transactions'],
        self.counts['too_few_transactions']))
      print('Number of SIMs with more than {} transactions per day on average: {:,}'\
        .format(self.thresholds['max_avg_transactions'],
        self.counts['too_many_avg_transactions'] ))
      print('Number of SIMs with more than {} transactions in a single day: {:,}'\
        .format(self.thresholds['max_transactions_in_single_day'],
        self.counts['too_many_transactions_in_single_day']))
      print('SIMs with less than {} transactions as a fraction of all accounts: {:.8f}'\
        .format(self.thresholds['min_transactions'],
        self.counts['too_few_transactions_fraction']))
      print('SIMs with more than {} transactions per day on average as a fraction of all accounts: {:.8f}'\
        .format(self.thresholds['max_avg_transactions'],
        self.counts['too_many_avg_transactions_fraction']))
      print('SIMs with more than {} transactions on a single day as a fraction of all accounts: {:.8f}'\
        .format(self.thresholds['max_transactions_in_single_day'],
        self.counts['too_many_transactions_in_single_day_fraction']))
      print('Number of transactions that would be kept: {:,}'\
        .format(self.counts['filtered_transactions']))
      print('Number of transactions that would be deleted: {:,}'\
        .format(self.counts['dropped_calls']))
      print('Fraction of transactions that would be deleted: {:.8f}'\
        .format(self.counts['dropped_calls'] / self.counts['all_records']))