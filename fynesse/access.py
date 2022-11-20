from .config import *

"""These are the types of import we might expect in this file
import httplib2
import oauth2
import tables
import mongodb
import sqlite"""

import pymysql
from pymysql.constants import CLIENT
import urllib.request

# This file accesses the data

"""Place commands in this file to access the data electronically. Don't remove any missing values, or deal with outliers. Make sure you have legalities correct, both intellectual property and personal data privacy rights. Beyond the legal side also think about the ethical issues around this data. """

def data():
    """Read the data from the web or local file, returning structured format such as a data frame"""
    raise NotImplementedError

def create_connection(user, password, host, database, port=3306):
    """ Create a database connection to the MariaDB database
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
        conn = pymysql.connect(user=user,
                               passwd=password,
                               host=host,
                               port=port,
                               local_infile=1,
                               db=database,
                               client_flag=CLIENT.MULTI_STATEMENTS
                               )
    except Exception as e:
        print(f"Error connecting to the MariaDB Server: {e}")
    return conn


def initialize_database(conn, db_name):
  cur = conn.cursor()
  cur.execute(f"""
    SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
    SET time_zone = "+00:00";
    CREATE DATABASE IF NOT EXISTS `{db_name}` DEFAULT CHARACTER SET utf8 COLLATE utf8_bin;
  """)
  conn.commit()
  print(f"Database {db_name} initialized")


def initialize_pp_data_schema(conn, table_name='pp_data'):
  cur = conn.cursor()
  cur.execute(f"""
    DROP TABLE IF EXISTS `{table_name}`;
    CREATE TABLE IF NOT EXISTS `{table_name}` (
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
  """)
  conn.commit()
  print(f"Schema for table {table_name} initialized")


def add_primary_key(conn, table_name, column_name):
  cur = conn.cursor()
  cur.execute(f"""
    ALTER TABLE `{table_name}`
    ADD PRIMARY KEY (`{column_name}`);
    ALTER TABLE `{table_name}`
    MODIFY `{column_name}` bigint(20) unsigned NOT NULL AUTO_INCREMENT,AUTO_INCREMENT=1;
  """)
  conn.commit()
  print(f"Added primary key {column_name} for table {table_name}")


def download_pp_data(start_year=1995, end_year=2022):
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


def load_pp_data_single_year(conn, table_name, year):
  cur = conn.cursor()
  if year == 2022:
      cur.execute(f"""
        LOAD DATA LOCAL INFILE 'pp-2022.csv' INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES STARTING BY '' TERMINATED BY '\n';
      """)
  else:
    for part in range(1, 3):
      file_name = "pp-" + str(year) + "-" + "part" + str(part) + ".csv"
      cur.execute(f"""
        LOAD DATA LOCAL INFILE '{file_name}' INTO TABLE `{table_name}`
        FIELDS TERMINATED BY ',' 
        ENCLOSED BY '"'
        LINES STARTING BY '' TERMINATED BY '\n';
      """)
  conn.commit()
  print(f"pp_data for {year} loaded")


def load_pp_data(conn, table_name, start_year=1995, end_year=2022):
  start_year = max(start_year, 1995)
  end_year = min(end_year, 2022)
  assert start_year <= end_year
  for year in range(start_year, end_year + 1):
    load_pp_data_single_year(conn, table_name, year)