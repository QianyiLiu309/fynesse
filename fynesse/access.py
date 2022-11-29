# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

from .config import *

import pymysql
import pandas as pd
from pymysql.constants import CLIENT
import urllib.request
import zipfile
import requests


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
            `postcode` varchar(8)P COLLATE utf8_bin NOT NULL,
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
            self.load_pp_data_single_year(self.conn, self.table_name, year)


class PostcodeData(DatabaseTable):
    """The postcode_data table containing the ONS Postcode information
    :param conn: a connection to the database
    :param table_name: table_name
    """

    def __init__(self, conn, table_name):
        super().__init__(conn, table_name)

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