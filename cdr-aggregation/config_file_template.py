from pyspark.sql.types import *
schema = StructType([
  StructField("msisdn", IntegerType(), True),
  StructField("call_datetime", StringType(), True), #load as string, will be turned into datetime in standardize_csv_files()
  StructField("location_id", StringType(), True)
])

datasource_configs = {
  "base_path": "/home/jovyan/work/data", #folder path used in this docker env
  "country_code": "",
  "telecom_alias": "",
  "schema" : schema,
  "data_paths" : ["*.csv"],
  "filestub": "",
  "geofiles": {},
  "shapefiles": ['admin2','admin3', 'voronoi'],
  "dates": {'start_date' : dt.datetime(2020,2,1),
            'end_date' : dt.datetime(2020,3,31)}
}
