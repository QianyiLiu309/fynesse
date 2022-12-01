from .config import *

from fynesse import access
import pandas as pd
import math
import geopandas as gpd

import matplotlib.pyplot as plt

import osmnx as ox
import math
import mlai
import mlai.plot as plot

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


# def data():
#     """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
#     df = access.data()
#     raise NotImplementedError


# def query(data):
#     """Request user input for some aspect of the data."""
#     raise NotImplementedError


# def view(data):
#     """Provide a view of the data that allows the user to verify some aspect of its quality."""
#     raise NotImplementedError


# def labelled(data):
#     """Provide a labelled set of data ready for supervised learning."""
#     raise NotImplementedError


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


def get_average_distance_to_POI(latitude, longitude, pois, threshold):
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
    shortest_dist = threshold
    for row in pois.iterrows():
        polygon = row[1]["geometry"]
        dis = math.sqrt(
            (latitude - polygon.centroid.y) ** 2 + (longitude - polygon.centroid.x) ** 2
        )
        shortest_dist = min(shortest_dist, dis)
    return shortest_dist * 111


def create_gdf_from_df(
    df, latitude_col_name="lattitude", longitude_col_name="longitude"
):
    assert latitude_col_name in df.columns.values
    assert longitude_col_name in df.columns.values

    return gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df[latitude_col_name], df[longitude_col_name])
    )


def get_bbox(latitude, longitude, box_width, box_height):
    north = latitude + box_height / 2
    south = latitude - box_height / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2
    return (north, south, west, east)


def download_POI_around_coordinate(
    latitude,
    longitude,
    box_width=0.02,
    box_height=0.02,
    tags={},
    columns=["name", "addr:city", "addr:postcode", "addr:street", "geometry"],
):
    north, south, west, east = get_bbox(latitude, longitude, box_width, box_height)
    pois = ox.geometries_from_bbox(north, south, east, west, tags)

    for key in tags.keys():
        if key not in columns:
            columns.append(key)
    present_columns = [key for key in columns if key in pois.columns]

    return pois[present_columns]


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
