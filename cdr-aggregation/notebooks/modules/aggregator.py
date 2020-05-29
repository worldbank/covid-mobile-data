import os
if os.environ['HOME'] != '/root':
    from modules.DataSource import *
    from modules.sql_code_aggregates import *
    databricks = False
else:
    databricks = True

# Databricks notebook source
class aggregator:
    """Class to handle aggregations.


    Attributes
    ----------
    result_stub : a string. File path where to save results
    datasource : an instance of DataSource class. Holds all dataframes and paths required
    regions : a pyspark dataframe. Admin level this aggregator will be used for
    calls : a pyspark dataframe. cdr data
    cells : a pyspark dataframe. admin region to tower mapping
    spark : an initialised spark connection. spark connection this aggregator should use
    dates : a dictionary. dates the aggregator should run over
    intermediate_tables : tables that we don't want written to csv


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

    rename_csv(table_name)
        - rename a specific csv
        - move a csv to parent folder, rename it, then delete its remaining folder

    rename_all_csvs(table_name)
        renames all csvs at once

    rename_if_not_existing(table_name)
        rename only if the file doesn't exist as csv yet, handles errors

    check_if_file_exists(table_name)
        checks whether a csv exists before we re-create



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

    def save_and_rename_one(self, df, table_name):
      self.rename_if_not_existing(self.save_and_report(df, table_name))

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
