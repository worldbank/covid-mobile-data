# Databricks notebook source
############# Utility functions used throughout
import os
if os.environ['HOME'] != '/root':
    from modules.import_packages import *
    databricks = False
else:
    databricks = True

def save_and_load_parquet(df, filename):
    # write parquet
    df.write.mode('overwrite').parquet(filename)
    #load parquet
    df = spark.read.format("parquet").load(filename)
    return df

def save_csv(matrix, path, filename):
    # write to csv
    matrix.repartition(1).write.mode('overwrite').format('com.databricks.spark.csv') \
        .save(os.path.join(path, filename), header = 'true')
    # move one folder up and rename to human-legible .csv name
    if databricks:
        dbutils.fs.mv(dbutils.fs.ls(path + '/' + filename)[-1].path,
                  path + '/' + filename + '.csv')
        # remove the old folder
        dbutils.fs.rm(path + '/' + filename + '/', recurse = True)

    else:
        os.rename(glob.glob(os.path.join(path, filename + '/*.csv'))[0],
                  os.path.join(path, filename + '.csv'))
        shutil.rmtree(os.path.join(path, filename))

def attempt_aggregation(aggregation_instance, indicators_to_produce = 'all', aggregator_type = 'custom', no_of_attempts = 4):
    attempts = 0
    while attempts < no_of_attempts:
      try:
        ## Flowminder aggregation
        if aggregator_type == 'flowminder':
          # all indicators
          if indicators_to_produce == 'all':
            aggregation_instance.run_save_and_rename_all_sql()
          # single indicator
          else:
            for table in indicators_to_produce.keys():
              table_name = indicators_to_produce[table]
              print('--> Producing: ' + table_name)
              aggregation_instance.run_save_and_rename_sql(table_name + '_per_' + indicators_to_produce[table_name])
          print('Flowminder indicators saved.')

        ## Custom aggregation
        else:
          # all indicators
          if indicators_to_produce == 'all':
            aggregation_instance.run_save_and_rename_all()

          # single indicator
          else:
            for table in indicators_to_produce.keys():
              table_name = indicators_to_produce[table][0]
              frequency = indicators_to_produce[table][1]
              weeks_filter = getattr(aggregation_instance, 'weeks_filter')
              period_filter = getattr(aggregation_instance, 'period_filter')

              # more than the standard arguments
              if isinstance(frequency, list):
                other_args = frequency[1]
                frequency = frequency[0]
                if frequency == 'week':
                  filter_var = weeks_filter
                else:
                  filter_var = period_filter

                result = getattr(aggregation_instance, table_name)(filter_var, frequency, **other_args)
                try:
                  table_name = other_args['home_location_frequency'] + '_' + table_name
                except:
                  pass

              # only the standard arguments
              else:
                if frequency == 'week':
                  filter_var = weeks_filter
                else:
                  filter_var = period_filter
                result = getattr(aggregation_instance, table_name)(filter_var, frequency)

              # save and rename
              table_name = table_name  + '_per_' + frequency
              table_name = aggregation_instance.save_and_report(result, table_name)
              if databricks:
                  try:
                    dbutils.fs.ls(aggregation_instance.result_path + '/' + table_name + '.csv')
                  except Exception as e:
                    # the csv doesn't exist yet, move the file and delete the folder
                    if 'java.io.FileNotFoundException' in str(e):
                      aggregation_instance.rename_csv(table_name)
                    else:
                      raise
              else:
                  if os.path.exists(aggregation_instance.result_path + '/' + table_name + '.csv'):
                      pass
                  else:
                      aggregation_instance.rename_csv(table_name)

        print('Custom indicators saved.')
        break
      except Exception as e:
        attempts += 1
        print(e)
        print('Try number {} failed. Trying again.'.format(attempts))
        if attempts == 4:
          print('Tried creating and saving indicators 4 times, but failed.')

############# Windows for window functions

# window by cardnumber
user_window = Window\
    .partitionBy('msisdn').orderBy('call_datetime')

# window by cardnumber starting with last transaction
user_window_rev = Window\
    .partitionBy('msisdn').orderBy(F.desc('call_datetime'))

# user date window
user_date_window = Window\
    .partitionBy('msisdn', 'call_date').orderBy('call_datetime')

# user date window starting from last date
user_date_window_rev = Window\
    .partitionBy('msisdn', 'call_date').orderBy(F.desc('call_datetime'))


############# Plotting

def zero_to_nan(values):
    """Replace every 0 with 'nan' and return a copy."""
    values[ values==0 ] = np.nan
    return values

def fill_zero_dates(pd_df):
    pd_df = pd_df[~pd_df.index.isnull()].sort_index()
    msisdnx = pd.date_range(pd_df.index[0], pd_df.index[-1])
    pd_df = pd_df.reindex(msisdnx, fill_value= 0)
    return pd_df
