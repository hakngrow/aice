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
    """Cleans rental data in a dataframe and prepares it for EDA and modelling purposes

    Parameters
    ----------
    df_rentals
        a dataframe containing rentals data which needs cleaning to be ready for EDA and modelling

    Returns
    -------
    dataframe
        a dataframe containing rentals data ready for EDA and modelling
    """

    # Clean date column ################################################################################################

    # Rename date column to date_str to indicate string data type
    df_rentals.rename(columns={settings.COL_DATE: COL_DATE_STR}, inplace=True)

    # Create datetime column by concatenating the date and hr columns
    df_rentals[COL_DATETIME] = df_rentals.apply(lambda row: row[COL_DATE_STR] + ' ' + str(row[settings.COL_HOUR]),
                                                axis=1) + ':00'

    # Convert datetime column from string to datetime data type
    df_rentals[COL_DATETIME] = pd.to_datetime(df_rentals[COL_DATETIME])

    # Clean weather column #############################################################################################

    # Standardized weather column to lower case characters
    df_rentals[settings.COL_WEATHER] = df_rentals[settings.COL_WEATHER].str.lower()

    dict_weather = {

        # Replace incorrect values 'lear' and 'clar' with 'clear'
        'lear': 'clear',
        'clar': 'clear',

        # Replace incorrect values 'cludy' and 'loudy' with 'cloudy'
        'cludy': 'cloudy',
        'loudy': 'cloudy',

        # Replace incorrect value 'liht snow/rain' with 'light snow/rain'
        'liht snow/rain': 'light snow/rain'
    }

    # Replace incorrect values in weather column
    df_rentals.replace({settings.COL_WEATHER: dict_weather}, inplace=True)

    # Clean relative_humidity column ###################################################################################

    # Drop observations with relative humidity value of 0
    df_rentals.drop(df_rentals[df_rentals[settings.COL_REL_HUMIDITY] == 0].index, inplace=True)

    # Clean guest_scooter, registered_scooter columns ##################################################################

    # Set all negative values in the guest_scooter column to 0
    df_rentals.loc[df_rentals[settings.COL_GUEST_SCOOTER] < 0, settings.COL_GUEST_SCOOTER] = 0

    # Set all negative values in the registered_scooter column to 0
    df_rentals.loc[df_rentals[settings.COL_REG_SCOOTER] < 0, settings.COL_REG_SCOOTER] = 0

    # Remove duplicate columns #########################################################################################

    # Drop duplicate observations
    df_rentals.drop_duplicates(inplace=True)

    return df_rentals


# TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #####

df = get_rentals()

if df is not None:

    # Before cleaning
    print(df.shape)

    df_cleaned = clean_data(df)

    # After cleaning
    print(df.shape)

