# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

from .config import *

import pymysql
import pandas as pd
from pymysql.constants import CLIENT
import urllib.request
import zipfile
import requests
import osmnx as ox


def create_connection(user, password, host, database, port=3306):
    """Create a database connection to the MariaDB database
        specified by the host url and database name.
    :param user: username
    :param password: password
    :param host: host url
    :param database: database
    :param port: port number
    :return: Connection object or None
    """
    conn = None
    try:
        conn = pymysql.connect(
            user=user,
            passwd=password,
            host=host,
            port=port,
            local_infile=1,
            db=database,
            client_flag=CLIENT.MULTI_STATEMENTS,
        )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


def initialize_database(conn, db_name):
    """Initialize database specified by db_name, set SQL_MODE and timezone
    :param db_name: database name
    :return: None
    """
    cur = conn.cursor()
    cur.execute(
        f"""
        SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
        SET time_zone = "+00:00";
        CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;
    """
    )
    conn.commit()
    print(f"Database {db_name} initialized")


class DatabaseTable:
    """A base class for database tables in MariaDB, with member functions to add primary key and create index
    :param conn: a connection to the database
    :param table_name: table_name
    """

    def __init__(self, conn, table_name):
        self.conn = conn
        self.table_name = table_name

    def add_primary_key(self, column_name):
        cur = self.conn.cursor()
        cur.execute(
            f"""
            ALTER TABLE `{self.table_name}`
            ADD PRIMARY KEY (`{column_name}`);
            ALTER TABLE `{self.table_name}`
            MODIFY `{column_name}` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
        """
        )
        self.conn.commit()
        print(f"Added primary key {column_name} for table {self.table_name}")

    def create_index(self, index_name, column_name):
        cur = self.conn.cursor()
        cur.execute(
            f"""
            CREATE INDEX `{index_name}` USING HASH
            ON `{self.table_name}`
            ({column_name});
        """
        )
        self.conn.commit()
        print(
            f"Index {index_name} created on column {column_name} in table {self.table_name}"
        )


class PricePaidDataTable(DatabaseTable):
    """The pp_data table containing all the UK Price Paid data
    :param conn: a connection to the database
    :param table_name: table_name
    """

    def __init__(self, conn, table_name):
        super().__init__(conn, table_name)

    def initialize_pp_data_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            f"""
            DROP TABLE IF EXISTS `{self.table_name}`;
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            `transaction_unique_identifier` tinytext COLLATE utf8_bin NOT NULL,
            `price` int(10) unsigned NOT NULL,
            `date_of_transfer` date NOT NULL,
            `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
            `property_type` varchar(1) COLLATE utf8_bin NOT NULL,
            `new_build_flag` varchar(1) COLLATE utf8_bin NOT NULL,
            `tenure_type` varchar(1) COLLATE utf8_bin NOT NULL,
            `primary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
            `secondary_addressable_object_name` tinytext COLLATE utf8_bin NOT NULL,
            `street` tinytext COLLATE utf8_bin NOT NULL,
            `locality` tinytext COLLATE utf8_bin NOT NULL,
            `town_city` tinytext COLLATE utf8_bin NOT NULL,
            `district` tinytext COLLATE utf8_bin NOT NULL,
            `county` tinytext COLLATE utf8_bin NOT NULL,
            `ppd_category_type` varchar(2) COLLATE utf8_bin NOT NULL,
            `record_status` varchar(2) COLLATE utf8_bin NOT NULL,
            `db_id` bigint(20) unsigned NOT NULL
            ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin AUTO_INCREMENT=1 ;
        """
        )
        self.conn.commit()
        print(f"Schema for table {self.table_name} initialized")

    def download_pp_data(self, start_year=1995, end_year=2022):
        base_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/"
        for year in range(start_year, end_year + 1):
            if year == 2022:
                file_name = "pp-2022.csv"
                urllib.request.urlretrieve(base_url + file_name, file_name)
            else:
                for part in range(1, 3):
                    file_name = "pp-" + str(year) + "-" + "part" + str(part) + ".csv"
                    urllib.request.urlretrieve(base_url + file_name, file_name)
            print(f"pp_data for {year} downloaded")

    def load_pp_data_single_year(self, year):
        cur = self.conn.cursor()
        if year == 2022:
            cur.execute(
                f"""
                LOAD DATA LOCAL INFILE 'pp-2022.csv' INTO TABLE `{self.table_name}`
                FIELDS TERMINATED BY ',' 
                ENCLOSED BY '"'
                LINES STARTING BY '' TERMINATED BY '\n';
            """
            )
        else:
            for part in range(1, 3):
                file_name = "pp-" + str(year) + "-" + "part" + str(part) + ".csv"
                cur.execute(
                    f"""
                    LOAD DATA LOCAL INFILE '{file_name}' INTO TABLE `{self.table_name}`
                    FIELDS TERMINATED BY ',' 
                    ENCLOSED BY '"'
                    LINES STARTING BY '' TERMINATED BY '\n';
                """
                )
        self.conn.commit()
        print(f"pp_data for {year} loaded")

    def load_pp_data(self, start_year=1995, end_year=2022):
        start_year = max(start_year, 1995)
        end_year = min(end_year, 2022)
        # assert start_year <= end_year
        for year in range(start_year, end_year + 1):
            self.load_pp_data_single_year(year)


class PostcodeData(DatabaseTable):
    """The postcode_data table containing the ONS Postcode information
    :param conn: a connection to the database
    :param table_name: table_name
    """

    def __init__(self, conn, table_name):
        super().__init__(conn, table_name)

    def initialize_property_prices_schema(self):
        cur = self.conn.cursor()
        cur.execute(
            f"""
            DROP TABLE IF EXISTS `{self.table_name}`;
            CREATE TABLE IF NOT EXISTS `{self.table_name}` (
            `postcode` varchar(8) COLLATE utf8_bin NOT NULL,
            `status` enum('live','terminated') NOT NULL,
            `usertype` enum('small', 'large') NOT NULL,
            `easting` int unsigned,
            `northing` int unsigned,
            `positional_quality_indicator` int NOT NULL,
            `country` enum('England', 'Wales', 'Scotland', 'Northern Ireland', 'Channel Islands', 'Isle of Man') NOT NULL,
            `lattitude` decimal(11,8) NOT NULL,
            `longitude` decimal(10,8) NOT NULL,
            `postcode_no_space` tinytext COLLATE utf8_bin NOT NULL,
            `postcode_fixed_width_seven` varchar(7) COLLATE utf8_bin NOT NULL,
            `postcode_fixed_width_eight` varchar(8) COLLATE utf8_bin NOT NULL,
            `postcode_area` varchar(2) COLLATE utf8_bin NOT NULL,
            `postcode_district` varchar(4) COLLATE utf8_bin NOT NULL,
            `postcode_sector` varchar(6) COLLATE utf8_bin NOT NULL,
            `outcode` varchar(4) COLLATE utf8_bin NOT NULL,
            `incode` varchar(3)  COLLATE utf8_bin NOT NULL,
            `db_id` bigint(20) unsigned NOT NULL
            ) DEFAULT CHARSET=utf8 COLLATE=utf8_bin;
        """
        )
        self.conn.commit()
        print(f"Schema for table {self.table_name} initialized")

    def download_postcode_data(self):
        postcode_data_url = (
            "https://www.getthedata.com/downloads/open_postcode_geo.csv.zip"
        )
        r = requests.get(postcode_data_url)
        with open("open_postcode_geo.csv.zip", "wb") as outfile:
            outfile.write(r.content)

        with zipfile.ZipFile("open_postcode_geo.csv.zip", "r") as zip_ref:
            zip_ref.extractall(".")
        print("postcode data downloaded")

    def load_postcode_data(self):
        cur = self.conn.cursor()
        cur.execute(
            f"""
            LOAD DATA LOCAL INFILE 'open_postcode_geo.csv' INTO TABLE `{self.table_name}`
            FIELDS TERMINATED BY ',' 
            LINES STARTING BY '' TERMINATED BY '\n';
        """
        )
        self.conn.commit()
        print(f"postcode_data loaded")


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
    north = latitude + box_height / 2
    south = latitude - box_height / 2
    west = longitude - box_width / 2
    east = longitude + box_width / 2

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


def get_joined_transactions(
    conn,
    start_date,
    end_date,
    latitude=None,
    longitude=None,
    box_width=None,
    box_height=None,
    property_type=None,
    limit=None,
    pp_data="pp_data",
    postcode_data="postcode_data",
):
    """Perform an inner join on table `pp_data` and `postcode_data`, joining rows that have the same postcode.
    :param conn: a connection to databse
    :param start_date: start date
    :param end_date: end date
    :param latitude: latitude
    :param longitude: longitude
    :param box_width: width of bbox
    :param box_height: height of bbox
    :param property_type: property_type of the housing
    :param limit: the maximum number of retrieved entries
    :param pp_data: table name for pp_data
    :param postcode_data: table name for postcode_dat
    :return: a dataframe containing results of the inner-join query
    """
    if latitude is not None and box_height is None:
        raise RuntimeError("box_height needs to be defined when passing in a latitude")
    if longitude is not None and box_width is None:
        raise RuntimeError("box_width needs to be defined when passing in a longitude")

    inner_query = f"""
    SELECT postcode, lattitude, longitude 
    FROM {postcode_data} 
    WHERE true"""

    if latitude is not None:
        inner_query = (
            inner_query
            + f" and lattitude <= {latitude + box_height / 2} and lattitude >= {latitude - box_height / 2}"
        )
    if longitude is not None:
        inner_query = (
            inner_query
            + f" and longitude <= {longitude + box_width / 2} and longitude >= {longitude - box_width / 2}"
        )

    sql_query_prefix = f"SELECT * FROM `{pp_data}` AS p INNER JOIN ( "
    sql_query_suffix = f""" 
    ) AS c 
    ON p.postcode = c.postcode 
    WHERE date_of_transfer >= date({start_date.strftime("%Y%m%d")}) and date_of_transfer <= date({end_date.strftime("%Y%m%d")})"""
    if property_type is not None:
        sql_query_suffix = sql_query_suffix + f" and property_type = '{property_type}'"
    if limit is not None:
        sql_query_suffix = sql_query_suffix + f" LIMIT {limit}"

    sql_query = sql_query_prefix + inner_query + sql_query_suffix
    df = pd.read_sql(sql_query, con=conn)
    return df
