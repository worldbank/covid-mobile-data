import os
if os.environ['HOME'] != '/root':
    from modules.DataSource import *
    from modules.sql_code_aggregates import *
    databricks = False
else:
    databricks = True

# Databricks notebook source
class aggregator:
    """Class to handle sql aggregations of flowminder code.
    For the original sql code from flowminder see https://github.com/Flowminder/COVID-19

    Attributes
    ----------
    result_stub : a string. File path where to save results
    datasource : an instance of DataSource class. Holds all dataframes and paths required
    regions : a pyspark dataframe. Admin level this aggregator will be used for
    intermediate_tables : a list. Names of tables that we don't want written to csv
    calls : a pyspark dataframe. pyspcdr data
    cells : a pyspark dataframe. admin region to tower mapping
    spark : an initialised spark connection. spark connection this aggregator should use
    dates : a dictionary. dates the aggregator should run over
    sql_code : a string. the flowminder sql code to be used


    Methods
    -------
    create_sql_dates()
        Convert the dates to strings to be used in the flowminder sql queries

    create_view(df, table_name)
        Creates a view of a dataframe

    save(table_name)
      Repartitions a dataframe into a single partition and writes it to a csv file

    save_and_report(table_name)
        Checks whether csv file exists before saving table_name to csv

    run_and_save_sql(table_name)
        - programmatically runs an sql query and produces a dataframe
        - saves the result to a csv
        - creates a view

    run_and_save_all_sql(table_name)
        runs run_and_save_sql on the list of all flowminder queries at once

    run_save_and_rename_all_sql()
        runs run_and_save_all_sql and then renames the csv files created and moves them to their parent folder

    rename_csv(table_name)
        - rename a specific csv
        - move a csv to parent folder, rename it, then delete its remaining folder

    rename_all_csvs(table_name)
        renames all csvs at once

    rename_if_not_existing(table_name)
        rename only if the file doesn't exist as csv yet, handles errors

    check_if_file_exists(table_name)
        checks whether a csv exists before we re-create

    attempt_aggregation(indicators_to_produce = 'all', no_of_attempts = 4)
        - attempts aggregation of all flowminder indicators
        - tries mutiple times (this is relevant for databricks env, but should be dropped going forward and replaced by a more solid handling of databricks timeouts)


    """

    def __init__(self,
                 result_stub,
                 datasource,
                 regions,
                 intermediate_tables = ['home_locations']):
        """
        Parameters
        ----------
        result_stub : where to save results
        datasource : holds all dataframes and paths required
        regions : admin level this aggregator will be used for
        intermediate_tables : tables that we don't want written to csv
        """
        self.datasource = datasource
        self.result_path = datasource.results_path + result_stub
        self.calls = datasource.parquet_df
        self.calls.createOrReplaceTempView('calls')
        self.cells = getattr(datasource, regions)
        self.cells.createOrReplaceTempView("cells")
        self.spark = datasource.spark
        self.dates = datasource.dates
        self.create_sql_dates()
        self.sql_code = write_sql_code(calls = self.calls,
                                       start_date = self.dates_sql['start_date'],
                                       end_date = self.dates_sql['end_date'],
                                       start_date_weeks = self.dates_sql['start_date_weeks'],
                                       end_date_weeks = self.dates_sql['end_date_weeks'])
        self.table_names = self.sql_code.keys()
        self.intermediate_tables = intermediate_tables

    def create_sql_dates(self):
        self.dates_sql = {'start_date' : "\'" + self.dates['start_date'].isoformat('-')[:10] +  "\'",
                          'end_date' :  "\'" + self.dates['end_date'].isoformat('-')[:10] +  "\'",
                          'start_date_weeks' :  "\'" + self.dates['start_date_weeks'].isoformat('-')[:10] +  "\'",
                          'end_date_weeks' : "\'" + self.dates['end_date_weeks'].isoformat('-')[:10] +  "\'"}

    def create_view(self, df, table_name):
      df.createOrReplaceTempView(table_name)

    def save(self, df, table_name):
      df.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
        .save(os.path.join(self.result_path, table_name), header = 'true')

    def save_and_report(self, df, table_name):
      if table_name not in self.intermediate_tables:
        if self.check_if_file_exists(table_name):
            print('Skipped: ' + table_name)
        else:
            print('--> File does not exist. Saving: ' + table_name)
            self.save(df, table_name)
      else:
        print('Caching: home_locations')
        df.createOrReplaceTempView("home_locations")
        self.spark.sql('CACHE TABLE home_locations').collect()
      self.create_view(df, table_name)
      return table_name

    def run_and_save_all(self):
      for table_name in self.table_names:
          df = self.spark.sql(self.sql_code[table_name])
          self.save_and_report(df, table_name)

    def run_save_and_rename_all(self):
      self.run_and_save_all()
      self.rename_all_csvs()

    def rename_csv(self, table_name):
      # move one folder up and rename to human-legible .csv name
      if databricks:
          dbutils.fs.mv(dbutils.fs.ls(self.result_path + '/' + table_name)[-1].path,
                  self.result_path + '/' + table_name + '.csv')
          # remove the old folder
          dbutils.fs.rm(self.result_path + '/' + table_name + '/', recurse = True)
      else:
          os.rename(glob.glob(os.path.join(self.result_path, table_name + '/*.csv'))[0],
                            os.path.join(self.result_path, table_name + '.csv'))
          shutil.rmtree(os.path.join(self.result_path, table_name))

    def save_and_rename_one(self, table_name):
      self.rename_csv(self.save_and_report(table_name))

    def rename_all_csvs(self):
      for table_name in self.table_names:
        if table_name in self.intermediate_tables:
          pass
        else:
            self.rename_if_not_existing(table_name)

    def rename_if_not_existing(self, table_name):
            if databricks:
              try:
                # does the csv already exist
                dbutils.fs.ls(self.result_path + '/' + table_name + '.csv')
              except Exception as e:
                # the csv doesn't exist yet, move the file and delete the folder
                if 'java.io.FileNotFoundException' in str(e):
                  print('--> Renaming: ' + table_name)
                  self.rename_csv(table_name)
                else:
                  raise
            else:
              if os.path.exists(self.result_path + '/' + table_name + '.csv'):
                  pass
              else:
                  print('--> Renaming: ' + table_name)
                  self.rename_csv(table_name)

    def check_if_file_exists(self, table_name):
        if databricks:
          try:
            # does the folder exist?
            dbutils.fs.ls(self.result_path + '/' + table_name)
            return True
          except Exception as e:
            # the folder does not exist
            if 'java.io.FileNotFoundException' in str(e):
              try:
                # does the csv exist?
                dbutils.fs.ls(self.result_path + '/' + table_name + '.csv')
                return True
              except Exception as e:
                # the csv does not exist
                if 'java.io.FileNotFoundException' in str(e):
                  return False
                else:
                   raise
            else:
              raise
        else:
            return os.path.exists(self.result_path + '/' + table_name) | \
                   os.path.exists(self.result_path + '/' + table_name + '.csv')

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
                  table_name = indicators_to_produce[table]
                  print('--> Producing: ' + table_name)
                  self.run_save_and_rename(table_name + '_per_' + indicators_to_produce[table_name])
              print('Indicators saved.')
              break

          except Exception as e:
            attempts += 1
            print(e)
            print('Try number {} failed. Trying again.'.format(attempts))
            if attempts == 4:
              print('Tried creating and saving indicators 4 times, but failed.')
