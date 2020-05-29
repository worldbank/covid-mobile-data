# Databricks notebook source
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Polygon, LineString
from sklearn.neighbors import DistanceMetric
from scipy.spatial.distance import squareform
from scipy.cluster.hierarchy import linkage
from scipy.cluster.hierarchy import fcluster
from copy import deepcopy
import os
if os.environ['HOME'] != '/root':
    from modules.utilities import *
    databricks = False
else:
    databricks = True


## Class to handle spark and df in session
class tower_clusterer:
    """Class to cluster towers together.


    Attributes
    ----------
    datasource :  an instance of DataSource class.
    shape : a geopandas dataframe. Shapefile to use for clustering
    region_var : a string. Name of the region variable in the shapefile.
    sites :  a string. Name of the attribute of datasource that holds the tower coordinates.
    shape_df : a pyspark dataframe. Shapefile to use for clustering, in pyspark df.
    spark : an initialised spark connection
    spark_df : a pyspark dataframe. Holds the cdr data
    result_path : a string. Where to save results.
    filename : a string. Name for result file.
    dist : a string. Metric to use to calculate distances.
    sites :  a pyspark dataframe. Code, Lat, Lng for all tower_sites
    sites_with_clusters : a pyspark dataframe. Clustered sites (once methods have run)



    Methods
    -------
    cluster_towers()
        runs clustering algorithm

    get_centroids()
        computes centroids of clusters

    map_to_regions()
        maps cluster centroids to admin regions

    save_results()
        saves the results to csv

    """

    def __init__(self,
                datasource,
                shape,
                region_var,
                sites = 'tower_sites'):
        """
        Parameters
        ----------
        datasource :  an instance of DataSource class.
        shape : a geopandas dataframe. Shapefile to use for clustering
        region_var : a string. Name of the region variable in the shapefile.
        sites :  a string. Name of the attribute of datasource that holds the tower coordinates.
        """
        self.datasource = datasource
        self.spark = datasource.spark
        self.shape = getattr(datasource, shape + '_gpd')
        self.shape_df = getattr(datasource, shape)
        self.result_path = datasource.results_path
        self.filename = shape
        self.region_var = region_var
        self.dist = DistanceMetric.get_metric('haversine')
        sites_df = getattr(datasource, sites + '_pd')
        if (sites_df.columns == ['cell_id', 'LAT', 'LNG']).all():
          self.sites = sites_df[sites_df.LAT.notna()]
          self.sites_with_clusters = self.sites
        else:
          raise 'The sites dataframe does not have the correct columns / \
            column order. Should be cell_id, LAT, LNG'

    def cluster_towers(self):
        ## deepcopy sites since we will need it later on
        self.radians = deepcopy(self.sites)
        # convert degrees to radians
        self.radians['LAT'] = np.radians(self.sites['LAT'])
        self.radians['LNG'] = np.radians(self.sites['LNG'])
        # run clustering algorithm
        self.clusters = fcluster(
            linkage(
            squareform(
            self.dist.pairwise(self.radians[['LAT','LNG']]\
            .to_numpy())*6373), method='ward'), t = 1, criterion = 'distance')
        self.sites_with_clusters = self.radians
        self.sites_with_clusters['cluster'] = self.clusters
        # compute centroids of clusters
        self.get_centroids()
        self.sites_with_clusters['LAT'] = np.rad2deg(self.sites_with_clusters['LAT'])
        self.sites_with_clusters['LNG'] = np.rad2deg(self.sites_with_clusters['LNG'])
        self.sites_with_clusters['centroid_LAT'] = \
            np.rad2deg(self.sites_with_clusters['centroid_LAT'])
        self.sites_with_clusters['centroid_LNG'] = \
            np.rad2deg(self.sites_with_clusters['centroid_LNG'])
        # put clusters in geodataframe
        self.sites_gpd = gpd.GeoDataFrame(self.sites_with_clusters,
                                          geometry=gpd.points_from_xy(
                                          self.sites_with_clusters.centroid_LNG,
                                          self.sites_with_clusters.centroid_LAT),
                                          crs = 'epsg:4326')
        # compute distances between cluters
        self.distances_pd = pd.DataFrame(
            self.dist.pairwise(
            np.radians(
            self.sites_with_clusters[['centroid_LAT','centroid_LNG']])\
                .to_numpy())*6373, columns=self.sites_with_clusters.cell_id.unique(),
                                    index=self.sites_with_clusters.cell_id.unique())
        # create long form of distance matrix
        distances = []
        origin = []
        destination = []
        for a in self.distances_pd.index:
          for b in self.distances_pd.index:
            distances.append(self.distances_pd.loc[a,b])
            origin.append(a)
            destination.append(b)
        self.distances_pd_long = pd.DataFrame(list(zip(distances, origin, destination)),
            columns =['distance', 'origin', 'destination'])
        # map clusters to regions
        self.map_to_regions()
        return self.save_results()

    def get_centroids(self):
      # loop through clusters to compute centroids
      for cluster_num in self.sites_with_clusters.cluster.unique():
        subset = self.sites_with_clusters[self.sites_with_clusters.cluster == cluster_num]
        # use line method if we have only two towers in cluster
        if len(subset) == 2:
            line = LineString(subset.loc[:,['LNG', 'LAT']].to_numpy())
            self.sites_with_clusters.loc[self.sites_with_clusters.cluster == \
                cluster_num, 'centroid_LNG'] = line.interpolate(0.5, normalized = True).x
            self.sites_with_clusters.loc[self.sites_with_clusters.cluster == \
                cluster_num, 'centroid_LAT'] = line.interpolate(0.5, normalized = True).y
        # use polygon method if we have more than two towers in cluster
        if len(subset) > 2:
            self.sites_with_clusters.loc[self.sites_with_clusters.cluster == \
                cluster_num, 'centroid_LNG'] = \
                Polygon(subset.loc[:,['LNG', 'LAT']].to_numpy()).convex_hull.centroid.x
            self.sites_with_clusters.loc[self.sites_with_clusters.cluster == \
                cluster_num, 'centroid_LAT'] = \
                Polygon(subset.loc[:,['LNG', 'LAT']].to_numpy()).convex_hull.centroid.y
      # replace NAs
      self.sites_with_clusters.loc[self.sites_with_clusters.centroid_LAT.isna(),
        'centroid_LNG'] = \
        self.sites_with_clusters.loc[self.sites_with_clusters.centroid_LAT.isna(), 'LNG']
      self.sites_with_clusters.loc[self.sites_with_clusters.centroid_LAT.isna(),
        'centroid_LAT'] = \
        self.sites_with_clusters.loc[self.sites_with_clusters.centroid_LAT.isna(), 'LAT']

    def map_to_regions(self):
      # spatial join clusteres with shapefile
      self.joined = gpd.sjoin(self.sites_gpd, self.shape, op="intersects")

    def save_results(self):
      # save results of mapping of clusters to regions
      self.joined = self.joined.rename(columns={self.region_var:'region'})
      self.towers_regions_clusters_all_vars = \
        self.joined.loc[:,['cell_id', 'LAT', 'LNG', 'centroid_LAT',
                           'centroid_LNG', 'region', 'cluster']]
      self.towers_regions_clusters_all_vars  = \
        self.spark.createDataFrame(self.towers_regions_clusters_all_vars)
      save_csv(self.towers_regions_clusters_all_vars,
        self.result_path,
        self.datasource.country_code + '_' + self.filename + '_tower_map_all_vars')
      # save results with only essential variables, for use in data processing
      self.towers_regions_clusters = \
        self.joined.loc[:,['cell_id', 'region']]
      self.towers_regions_clusters  = \
        self.spark.createDataFrame(self.towers_regions_clusters)
      save_csv(self.towers_regions_clusters,
        self.result_path,
        self.datasource.country_code + '_' + self.filename + '_tower_map')
      # save distance matrix in long form
      self.distances_df_long  = \
        self.spark.createDataFrame(self.distances_pd_long)
      save_csv(self.distances_df_long,
        self.result_path, self.datasource.country_code + '_distances_pd_long')
      # save shapefile used, for dashboarding
      save_csv(self.shape_df, self.result_path,
        self.datasource.country_code + '_' + self.filename  + '_shapefile')
      return self.towers_regions_clusters, self.distances_df_long
