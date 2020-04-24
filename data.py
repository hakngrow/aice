"""
Script to extract rentals data from Microsoft SQL server.

To configure database parameters and extraction settings, please refer to documentation in script settings.py

pyodbc (https://github.com/mkleehammer/pyodbc) module is used to enable connection to Microsoft SQL server.
"""

import pandas as pd

from pandas.io.sql import DatabaseError

import pyodbc

import settings

# Column labels of rentals data dataframe
COL_DATE_STR = 'date_str'
COL_DATETIME = 'datetime'


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


def clean_data(df_rentals):

    # Clean date column ################################################################################################

    # Rename date column to date_str to indicate string data type
    df_rentals.rename(columns={settings.COL_DATE: COL_DATE_STR}, inplace=True)

    # Create datetime column by concatenating the date and hr columns
    df_rentals[COL_DATETIME] = df_rentals.apply(lambda row: row[COL_DATE_STR] + ' ' + str(row[settings.COL_HOUR]),
                                                axis=1) + ':00'

    # Convert datetime column from string to datetime data type
    df_rentals.datetime = pd.to_datetime(df_rentals.datetime)

    # Clean weather column #############################################################################################

    # Standardized weather column to lower case characters
    df_rentals.weather = df_rentals.weather.str.lower()

    # Replace incorrect values 'lear' and 'clar' with 'clear'
    df_rentals.weather.replace(['lear', 'clar'], 'clear', inplace=True)

    # Replace incorrect values 'cludy' and 'loudy' with 'cloudy'
    df_rentals.weather.replace(['cludy', 'loudy'], 'cloudy', inplace=True)

    # Replace incorrect value 'liht snow/rain' with 'light snow/rain'
    df_rentals.weather.replace('liht snow/rain', 'light snow/rain', inplace=True)

    # Clean temperature and feels_like_temperature columns #############################################################

    return df_rentals


# TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #####
'''
df_rentals = get_rentals()

if df_rentals is not None:
    df_rentals.info()
'''
