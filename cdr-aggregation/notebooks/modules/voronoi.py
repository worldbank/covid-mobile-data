# Databricks notebook source
from geovoronoi import voronoi_regions_from_coords

import os
if os.environ['HOME'] != '/root':
    from modules.tower_clustering import *

## Class to handle spark and df in session
class voronoi_maker:
    """Class to handle all voronoi transformations and files for a specific df


    Attributes
    ----------
    datasource :  an instance of DataSource class.
    shape : a geopandas dataframe. Shapefile to use for clustering
    region_var : a string. Name of the region variable in the shapefile.
    sites :  a string. Name of the attribute of datasource that holds the tower coordinates.
    spark_df : a pyspark dataframe. Holds the cdr data
    result_path : a string. Where to save results.
    clusterer : an instance of tower_clusterer.
    sites_df :  a pyspark dataframe. Holds clustered sites.
    distances_pd_long : a pyspark dataframe. Holds distances between sites.
    sites : a pyspark dataframe. Clustered sites without NAs.

    Methods
    -------
    make_voronoi()
        orchestrates all methods

    filter_towers_for_voronoi()
        we can't run on duplicates (location duplicates), so we have to filter them out first

    make_shape(towers_for_voronoi)
        makes a buffer around towers to create bubble shapes

    create_voronoi(towers_for_voronoi, shape)
        creats voronoi cells from tower list

    save_voronoi(poly_shapes)
        saves voronoi shape file and voronoi-tower mapping

    assign_to_spark_df()
        adds voronoi id to cdr records (not used currently)
    """

    def __init__(self,
                datasource,
                shape,
                region_var,
                sites = 'tower_sites'):
        """
        Parameters
        ----------
        """
        self.spark = datasource.spark
        self.datasource = datasource
        self.spark_df = datasource.parquet_df
        self.result_path = datasource.results_path
        self.clusterer = tower_clusterer(datasource, shape, region_var, sites)
        self.clusterer.cluster_towers()
        self.sites_df = self.clusterer.sites_with_clusters.loc[:,['cell_id', 'centroid_LAT', 'centroid_LNG']].rename(columns={'centroid_LAT' : 'LAT', 'centroid_LNG': 'LNG'})
        self.distances_pd_long = self.clusterer.distances_pd_long
        if (self.sites_df.columns == ['cell_id', 'LAT', 'LNG']).all():
          self.sites = self.sites_df[self.sites_df.LAT.notna()]
        else:
          raise 'The sites dataframe does not have the correct columns / column order. Should be cell_id, LAT, LNG'

    def make_voronoi(self):

        towers_for_voronoi = self.filter_towers_for_voronoi()
        shape, towers_for_voronoi = self.make_shape(towers_for_voronoi = towers_for_voronoi)
        poly_shapes = self.create_voronoi(shape = shape, towers_for_voronoi = towers_for_voronoi)
        self.save_voronoi(poly_shapes = poly_shapes)
        return self.voronoi_dict

    def filter_towers_for_voronoi(self):

        # get unique towers in data
        distinct_towers = self.spark_df.select('location_id').distinct().toPandas()

        # filter list of towers for unique towers
        self.sites = self.sites[self.sites.cell_id.isin(list(distinct_towers.location_id))]

        # Assign gpd
        self.towers = gpd.GeoDataFrame(
        self.sites, geometry = gpd.points_from_xy(self.sites.LNG, self.sites.LAT), crs = 'epsg:4326')

        # Find towers that are in same location
        self.towers.LAT = self.towers.LAT.apply(lambda x: round(x,4))
        self.towers.LNG = self.towers.LNG.apply(lambda x: round(x,4))
        towers_for_voronoi = self.towers[~self.towers.duplicated(subset = ['LAT', 'LNG'])]

        return towers_for_voronoi

    def make_shape(self, towers_for_voronoi):

        # Make border shape
        radians =   35 / 40000  * 360
        self.shape = towers_for_voronoi.buffer(radians).unary_union

        return self.shape, towers_for_voronoi

    def create_voronoi(self, towers_for_voronoi, shape):

        # Create np array of vertices
        points = towers_for_voronoi.loc[:,['LNG','LAT']].to_numpy()

        # Create voronoi shapes
        self.poly_shapes, pts, poly_to_pt_assignments = voronoi_regions_from_coords(points, shape)

        return self.poly_shapes

    def save_voronoi(self, poly_shapes):

        # Save voronoi
        self.voronoi_pd = pd.DataFrame(poly_shapes)
        self.voronoi_pd.columns =['geometry']
        self.voronoi_gpd = deepcopy(self.voronoi_pd)
        self.voronoi_gpd = gpd.GeoDataFrame(self.voronoi_gpd, geometry = 'geometry', crs = 'epsg:4326')
        self.voronoi_pd['geometry'] = self.voronoi_pd.geometry.astype(str)
        self.voronoi_pd = self.voronoi_pd.reset_index()
        self.voronoi_pd.columns = ['region', 'geometry']
        self.voronoi_pd  = self.spark.createDataFrame(self.voronoi_pd)
        save_csv(self.voronoi_pd, self.result_path, self.datasource.country_code + '_voronoi_shapefile')

        # Match towers to voronoi so that all towers are assigned to a cell
        voronoi_towers = gpd.sjoin(self.voronoi_gpd, self.towers, op="intersects")
        self.voronoi_dict = voronoi_towers.drop(['geometry', 'LAT', 'LNG', 'index_right'], axis = 'columns')
        self.voronoi_dict = self.voronoi_dict.reset_index()
        self.voronoi_dict.columns = ['region', 'cell_id']
        self.voronoi_dict  = self.spark.createDataFrame(self.voronoi_dict)
        save_csv(self.voronoi_dict, self.result_path, self.datasource.country_code + '_voronoi_tower_map')

    def assign_to_spark_df(self):

        self.new_spark_df = self.spark_df.join(self.voronoi_dict, self.spark_df['location_id'] == self.voronoi_dict['cell_id'], how = 'left')
        return self.new_spark_df
