from .config import *

from fynesse import access
import pandas as pd

"""These are the types of import we might expect in this file
import pandas
import bokeh
import seaborn
import matplotlib.pyplot as plt
import sklearn.decomposition as decomposition
import sklearn.feature_extraction"""

"""Place commands in this file to assess the data you have downloaded. How are missing values encoded, how are outliers encoded? What do columns represent, makes rure they are correctly labeled. How is the data indexed. Crete visualisation routines to assess the data (e.g. in bokeh). Ensure that date formats are correct and correctly timezoned."""


def data():
    """Load the data from access and ensure missing values are correctly encoded as well as indices correct, column names informative, date and times correctly formatted. Return a structured data structure such as a data frame."""
    df = access.data()
    raise NotImplementedError


def query(data):
    """Request user input for some aspect of the data."""
    raise NotImplementedError


def view(data):
    """Provide a view of the data that allows the user to verify some aspect of its quality."""
    raise NotImplementedError


def labelled(data):
    """Provide a labelled set of data ready for supervised learning."""
    raise NotImplementedError


def verify_database(conn):
    """Check the status of the database connected by
        querying information about all tables and views within it
    :param conn: a connection object to the database
    :return: None
    """
    df = pd.read_sql("SHOW TABLES;", con=conn)
    print(df)


def verify_index(conn, table_name):
    """Check the indices of the table specified by table_name in the database
    :param conn: a connection object to the database
    :param table_name: table name
    :return: None
    """
    df = pd.read_sql(f"SHOW INDEX FROM `{table_name}`;", con=conn)
    print(df)
