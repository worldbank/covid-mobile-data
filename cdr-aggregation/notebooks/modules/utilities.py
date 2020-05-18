
############# Utility functions used throughout
import os
if os.environ['HOME'] != '/root':
    from modules.import_packages import *
    from modules.DataSource import *
    databricks = False
else:
    databricks = True

def save_and_load_parquet(df, filename, ds):
    # write parquet
    df.write.mode('overwrite').parquet(filename)
    #load parquet
    df = ds.spark.read.format("parquet").load(filename)
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
