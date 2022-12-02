from .config import *
from fynesse import access

import mlai
import mlai.plot as plot
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler
import geopandas as gpd
import osmnx as ox

import math
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes sure they are correctly labeled. How is the data indexed. Create visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


def verify_database(conn):
    """Check the status of the database connected by
        querying information about all tables and views within it
    :param conn: a connection object to the database
    :return: None
    """
    df = pd.read_sql("SHOW TABLES;", con=conn)
    print(df)


def verify_table_index(conn, table_name):
    """Check the indices of the table specified by table_name in the database
    :param conn: a connection object to the database
    :param table_name: table name
    :return: None
    """
    df = pd.read_sql(f"SHOW INDEX FROM `{table_name}`;", con=conn)
    print(df)


def verify_table_content(conn, table_name):
    """Check the contents of the table specified by table_name in the database
    :param conn: a connection object to the database
    :param table_name: table name
    :return: a dataframe containing the first 5 rows in the table
    """
    df = pd.read_sql(f"SELECT * FROM `{table_name}` LIMIT 5;", con=conn)
    return df


def get_average_housing_price_per_year(
    conn,
    per_property_type=False,
    pp_data="pp_data",
    date="date_of_transfer",
    type="property_type",
):
    """Query the average housing price per year
    :param conn: a connection to database
    :param per_property_type: a boolean indicating whether we group by property_type
    :param pp_data: db table name
    :param date: column name for date
    :param type: column name for property_type
    :return: a dataframe containing the average housing price
    """
    column_names = f" avg(price) as mean_price, EXTRACT(year FROM `{date}`) as year"
    if per_property_type:
        column_names += f", {type} as {type}"
    sql_query = (
        "SELECT"
        + column_names
        + f" FROM `{pp_data}` GROUP BY EXTRACT(year FROM `{date}`)"
    )
    if per_property_type:
        sql_query += f" , `{type}`"
    df = pd.read_sql(sql_query, con=conn)
    return df


def get_average_distance_to_POI(latitude, longitude, pois, threshold):
    """Calculate the average distance from the point (latitude, longitude) to POIs.
    Valid distances should be less than thresdhold.
    :param latitude: latitude
    :param longitude: longitude
    :param pois: a geodataframe representing Points of Interest
    :param threshold: the upper limit of distance to be considered when calculating the mean
    :return: a float value representing average distance to POIs
    """
    total_dis = 0
    for row in pois.iterrows():
        polygon = row[1]["geometry"]
        dis = math.sqrt(
            (latitude - polygon.centroid.y) ** 2 + (longitude - polygon.centroid.x) ** 2
        )
        if dis > threshold:
            continue
        total_dis += dis
    return total_dis / len(pois) * 111


def get_cnt_of_POI(latitude, longitude, pois, threshold):
    """Count the number of POIs within the distance of `threshold` from the point (latitude, longitude)
    :param latitude: latitude
    :param longitude: longitude
    :param pois: a geodataframe representing Points of Interest
    :param threshold: the upper limit of distance to be considered when calculating the count
    :return: an integer value representing the number of nearby POIs
    """
    cnt = 0
    for row in pois.iterrows():
        polygon = row[1]["geometry"]
        dis = math.sqrt(
            (latitude - polygon.centroid.y) ** 2 + (longitude - polygon.centroid.x) ** 2
        )
        if dis > threshold:
            continue
        cnt += 1
    return cnt


def get_shortest_distance_to_POI(latitude, longitude, pois, threshold):
    """Calculate the shortest distance from the point (latitude, longitude) to any POI in pois.
    Valid distances should be less than thresdhold.
    :param latitude: latitude
    :param longitude: longitude
    :param pois: a geodataframe representing Points of Interest
    :param threshold: the upper limit of distance to be considered when calculating the minimum
    :return: a float value representing shortest distance to that POI
    """
    shortest_dist = threshold
    for row in pois.iterrows():
        polygon = row[1]["geometry"]
        dis = math.sqrt(
            (latitude - polygon.centroid.y) ** 2 + (longitude - polygon.centroid.x) ** 2
        )
        shortest_dist = min(shortest_dist, dis)
    return shortest_dist * 111


def download_POI_around_coordinate(
    latitude,
    longitude,
    box_width=0.02,
    box_height=0.02,
    tags={},
    columns=["name", "addr:city", "addr:postcode", "addr:street", "geometry"],
):
    """Download POI as specified by tags around a coordinate
    :param latitude: latitude
    :param longitude: longitude
    :param box_width: width of bbox
    :param box_height: height of bbox
    :param tags: a dict of POI tags
    :param columns: a list of interested column names
    :return: a geopandas dataframe containing points of interest
    """
    north, south, west, east = get_bbox(latitude, longitude, box_width, box_height)
    pois = ox.geometries_from_bbox(north, south, east, west, tags)

    for key in tags.keys():
        if key not in columns:
            columns.append(key)
    present_columns = [key for key in columns if key in pois.columns]

    return pois[present_columns]


def download_POI_for_feature_list(
    latitude, longitude, feature_box_width, feature_box_height, features
):
    """Download POIs from OpenStreetMap as specified by the tags attributes in `features`
    :param latitude: latitude
    :param longitude: longitude
    :param feature_box_width: width of feature bbox
    :param feature_box_height: height of feature bbox
    :param features: a JSON encoding of features
    :return: a mapping from feature name to POIs
    """
    pois_map = {}
    for name, prop in features.items():
        tag = prop["tags"]
        pois = download_POI_around_coordinate(
            latitude,
            longitude,
            box_width=feature_box_width,
            box_height=feature_box_height,
            tags=tag,
        )
        pois_map[name] = pois
        print(f"POIs for feature: {name} downloaded")
    return pois_map


def create_gdf_from_df(
    df, latitude_col_name="lattitude", longitude_col_name="longitude"
):
    """Create a geodataframe from a dataframe, using the specified columns as
    latitude and longitude
    :param df: a pandas dataframe containing latitude and longitude as columns
    :param latitude_col_name: name of the latitude column in df
    :param longitude_col_name: name of the longitude column in df
    :return: a geopandas dataframe
    """
    assert latitude_col_name in df.columns.values
    assert longitude_col_name in df.columns.values

    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[latitude_col_name], df[longitude_col_name])
    )


def get_bbox(latitude, longitude, box_width, box_height):
    """Create a bounding box based on the coordinate
    :param north: north
    :param south: south
    :param west: west
    :param esat: east
    :return: a tuple (north, south, west, east) representing the bounding box
    """
    north = latitude + box_height / 2
    south = latitude - box_height / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2
    return (north, south, west, east)


def sample_locations_from_bbox(
    latitude, longitude, box_height, box_width, n_sample=100
):
    """Get random samples of coordinates within the bounding box of the given location
    :param latitude: latitude
    :param longitude: longitude
    :param box_width: width of bbox
    :param box_height: height of bbox
    :param n_sample: the number of required samples
    :return: a dataframe of coordinates
    """
    latitudes = []
    longitudes = []
    for _ in range(n_sample):
        i = random.uniform(latitude - box_height / 2, latitude + box_height / 2)
        j = random.uniform(longitude - box_width / 2, longitude + box_width / 2)
        latitudes.append(i)
        longitudes.append(j)
    return pd.DataFrame({"lattitude": latitudes, "longitude": longitudes})


def calculate_single_feature(
    df, feature_fn, dist_threshold, method_name, poi, feature_name
):
    """Calculate single feature specified by `feature_fn` and add it as a new
    column with name `feature_name` to the input dataframe `df`
    :param df: a dataframe of transactions and features
    :param feature_fn: a function used to compute feature
    :param dist_threshold: distance threshold
    :param method_name: a string
    :param poi: the POIs used to compute the feature
    :feature_name: the type name of the POIs
    :return: a dataframe containing the new feature
    """
    column_name = feature_name + "_" + method_name
    df[column_name] = df.apply(
        lambda row: feature_fn(
            row.lattitude.item(), row.longitude.item(), poi, dist_threshold
        ),
        axis=1,
    )
    return df


def calculate_features(df, features, dist_threshold, pois_map):
    """Apply 'calculate_single_feature` to a collection of features
    :param df: a dataframe of transactions
    :param features: a JSON encoding of features
    :param dist_threshold: distance threshold
    :param pois_map: a mapping from feature_name to POIs
    :return: a dataframe containing all computed features
    """
    for feature_name, prop in features.items():
        for method_name in prop["methods"]:
            if method_name == "cnt":
                feature_fn = get_cnt_of_POI
            elif method_name == "avg_dist":
                feature_fn = get_average_distance_to_POI
            elif method_name == "shortest_dist":
                feature_fn = get_shortest_distance_to_POI
            else:
                raise NotImplementedError
            df = calculate_single_feature(
                df,
                feature_fn,
                dist_threshold,
                method_name,
                pois_map[feature_name],
                feature_name,
            )
    return df


def plot_geo_transactions(df):
    """Plot house locations as specified in `df` on UK map
    :param df: a dataframe representing house locations
    :return: None
    """
    fig, ax = plt.subplots(figsize=(12, 12))
    area = ox.geocode_to_gdf("United Kingdom")
    area.plot(ax=ax, facecolor="lightgray")

    ax.scatter(
        df.longitude,
        df.lattitude,
        c=np.log(df.price.values),
        alpha=0.005,
    )
    plt.show()


def plot_POI(
    latitude,
    longitude,
    place_name,
    box_height,
    box_width,
    pois,
    graph_name=None,
    plot_coordinate=False,
):
    """Geo-plot POIs on the map for the given place specified by place_name
    :param latitude: latitude
    :param longitude: longitude
    :param place_name: a geo-decodable string representing place name
    :param box_width: width of bbox
    :param box_height: height of bbox
    :param pois: a list of POIs
    :param graph_name: the name of the plot
    :param plot_coordinate: boolean value, plot the coordinate (lat, long) if true
    :return: None
    """
    north, south, west, east = get_bbox(latitude, longitude, box_width, box_height)

    graph = ox.graph_from_bbox(north, south, east, west)
    _, edges = ox.graph_to_gdfs(graph)
    area = ox.geocode_to_gdf(place_name)

    fig, ax = plt.subplots(figsize=plot.big_figsize)
    area.plot(ax=ax, facecolor="white")
    edges.plot(ax=ax, linewidth=1, edgecolor="dimgray")

    ax.set_xlim([west, east])
    ax.set_ylim([south, north])
    ax.set_xlabel("longitude")
    ax.set_ylabel("latitude")

    # Plot all POIs
    if plot_coordinate:
        plt.plot(longitude, latitude, "bo", markersize=10, alpha=0.6)
    for poi in pois:
        poi.plot(ax=ax, alpha=0.8, markersize=10)
    plt.tight_layout()
    if graph_name is not None:
        ax.set_title(f"{graph_name}")
    plt.show()


def plot_scatter_matrix_for_features(
    features, latitude, longitude, box_width, box_height, threshold, to_plot=True
):
    """Download POIs for feature list, calculate features for the randomly sampled locations,
    and then plot the scatter matrix of features if to_plot is set to True
    :param features: a JSON encoding of features
    :param latitude: latitude
    :param longitude: longitude
    :param box_width: width of bbox
    :param box_height: height of bbox
    :param threshold: distance threshold
    :param to_plot: a boolean incidating whether scatter matrix should be plot
    :return: a dataframe containing all computed features
    """
    pois_map = download_POI_for_feature_list(
        latitude, longitude, box_width, box_height, features
    )
    df = sample_locations_from_bbox(
        latitude, longitude, box_height, box_width, n_sample=30
    )
    df_with_features = calculate_features(df, features, threshold, pois_map)
    pure_features = df_with_features.drop(["lattitude", "longitude"], axis=1)

    if to_plot:
        axes = pd.plotting.scatter_matrix(pure_features, alpha=0.2, figsize=(13, 13))
        for ax in axes.flatten():
            ax.xaxis.label.set_rotation(45)
            ax.xaxis.label.set_ha("right")
            ax.yaxis.label.set_rotation(45)
            ax.yaxis.label.set_ha("right")
        plt.show()
    return df


def pca_analysis(pure_features, num_features):
    """Perform PCA analysis on features specified by `pure_features`
    :param pure_features: a collection of features in a dataframe
    :param num_featuress: total number of features
    :return: None
    """
    scaler = StandardScaler()
    scaler.fit(pure_features)
    pure_features = scaler.transform(pure_features)

    pca = PCA(n_components=num_features)
    features_new = pca.fit_transform(pure_features)
    exp_var_pca = pca.explained_variance_ratio_
    cum_sum_eigenvalues = np.cumsum(exp_var_pca)

    fig, ax = plt.subplots(figsize=plot.big_figsize)
    plt.bar(
        range(0, len(exp_var_pca)),
        exp_var_pca,
        alpha=0.5,
        align="center",
        label="Individual explained variance",
    )
    plt.step(
        range(0, len(cum_sum_eigenvalues)),
        cum_sum_eigenvalues,
        where="mid",
        label="Cumulative explained variance",
    )
    plt.ylabel("Explained variance ratio")
    plt.xlabel("Principal component index")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.show()
