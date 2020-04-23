"""
Script to extract rentals data from Microsoft SQL server.

To configure database parameters and extraction settings, please refer to documentation in script settings.py

pyodbc (https://github.com/mkleehammer/pyodbc) module is used to enable connection to Microsoft SQL server.
"""

import pandas as pd

from pandas.io.sql import DatabaseError

import pyodbc

import settings


def get_rentals():
    """Establish connection to Microsoft SQL server, extract rentals data from table and returns a dataframe

    Parameters
    ----------

    Returns
    -------
    dataframe
        a dataframe containing rentals data if successful, prints error message and return None if not
    """

    try:

        # Establish connection to Microsoft SQL server using parameters in settings.py
        conn_mssql = pyodbc.connect(settings.DB_CONN_STR)

        # Query and extract rentals data from table, returns a dataframe
        return pd.read_sql_query(settings.SQL_RENTAL, conn_mssql)

    # Catch and handle errors thrown by pyodbc
    except pyodbc.Error as err_pyodbc:
        err_msg = err_pyodbc.args[1]

    # Catch  and handle errors thrown by pandas
    except DatabaseError as err_db:
        err_msg = err_db.args[0]

    # Prints error message if any
    print(err_msg)

    # Returns None to indicate an error in the data extraction
    return None


# TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #####
'''
df_rentals = get_rentals()

if df_rentals is not None:
    df_rentals.info()
'''
