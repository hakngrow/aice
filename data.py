"""
Script to extract rentals data from MS SQL.
To configure database settings and extraction parameters, refer to settings.py
"""

import pandas as pd

import pyodbc

import settings


def get_rentals():

    try:

        conn_mssql = pyodbc.connect(settings.DB_CONN)

        return pd.read_sql_query(settings.SQL_RENTAL, conn_mssql)

    except (pyodbc.Error, ) as err:
        err_msg = err.args[1]

    except pd.io.sql.DatabaseError as err:
        err_msg = err.args[0]

    print(err_msg)

    return None


df_rentals = get_rentals()

if df_rentals is not None:
    df_rentals.info()