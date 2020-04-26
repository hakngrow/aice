"""
Script to extract rentals data from Microsoft SQL server.

To configure database parameters and extraction settings, please refer to documentation in script settings.py

pyodbc (https://github.com/mkleehammer/pyodbc) module is used to enable connection to Microsoft SQL server.
"""

import pandas as pd

from pandas.io.sql import DatabaseError

from sklearn.preprocessing import StandardScaler

import pyodbc

import settings

# Labels of extra columns/features created for EDA and modelling
COL_DATE_STR = 'date_str'
COL_HOUR_STR = 'hr_str'
COL_DATETIME = 'datetime'
COL_ACT_SCOOTER = 'active_scooter'
COL_DAY_OF_WEEK = 'day_of_wk'

# Categorical values of the weather feature
WEA_CLEAR = 'clear'
WEA_CLOUDY = 'cloudy'
WEA_HEAVY_SNOW_RAIN = 'heavy snow/rain'
WEA_LIGHT_SNOW_RAIN = 'light snow/rain'

# Categorical values of the day_of_wk feature
DOW_MON = 'Monday'
DOW_TUE = 'Tuesday'
DOW_WED = 'Wednesday'
DOW_THU = 'Thursday'
DOW_FRI = 'Friday'
DOW_SAT = 'Saturday'
DOW_SUN = 'Sunday'

# Categorical values of the hr feature
HR_0 = '0'
HR_1 = '1'
HR_2 = '2'
HR_3 = '3'
HR_4 = '4'
HR_5 = '5'
HR_6 = '6'
HR_7 = '7'
HR_8 = '8'
HR_9 = '9'
HR_10 = '10'
HR_11 = '11'
HR_12 = '12'
HR_13 = '13'
HR_14 = '14'
HR_15 = '15'
HR_16 = '16'
HR_17 = '17'
HR_18 = '18'
HR_19 = '19'
HR_20 = '20'
HR_21 = '21'
HR_22 = '22'
HR_23 = '23'

# Column labels of all numerical independent variables
cols_numerical = [settings.COL_GUEST_SCOOTER, settings.COL_REG_SCOOTER, settings.COL_TEMP, settings.COL_FEELS_LIKE_TEMP,
                  settings.COL_REL_HUMIDITY, settings.COL_WINDSPEED, settings.COL_PSI]

# Column labels of weather one-hot encoded variables

COL_WEATHER_CLEAR = settings.COL_WEATHER + '_' + WEA_CLEAR
COL_WEATHER_CLOUDY = settings.COL_WEATHER + '_' + WEA_CLOUDY
COL_WEATHER_LIGHT_SNOW_RAIN = settings.COL_WEATHER + '_' + WEA_LIGHT_SNOW_RAIN
COL_WEATHER_HEAVY_SNOW_RAIN = settings.COL_WEATHER + '_' + WEA_HEAVY_SNOW_RAIN

cols_weather = [COL_WEATHER_CLEAR, COL_WEATHER_CLOUDY, COL_WEATHER_LIGHT_SNOW_RAIN, COL_WEATHER_HEAVY_SNOW_RAIN]

# Column labels of day of week one-hot encoded variables

COL_DAY_OF_WEEK_MON = COL_DAY_OF_WEEK + '_' + DOW_MON
COL_DAY_OF_WEEK_TUE = COL_DAY_OF_WEEK + '_' + DOW_TUE
COL_DAY_OF_WEEK_WED = COL_DAY_OF_WEEK + '_' + DOW_WED
COL_DAY_OF_WEEK_THU = COL_DAY_OF_WEEK + '_' + DOW_THU
COL_DAY_OF_WEEK_FRI = COL_DAY_OF_WEEK + '_' + DOW_FRI
COL_DAY_OF_WEEK_SAT = COL_DAY_OF_WEEK + '_' + DOW_SAT
COL_DAY_OF_WEEK_SUN = COL_DAY_OF_WEEK + '_' + DOW_SUN

cols_day_of_wk = [COL_DAY_OF_WEEK_MON, COL_DAY_OF_WEEK_TUE, COL_DAY_OF_WEEK_WED, COL_DAY_OF_WEEK_THU,
                  COL_DAY_OF_WEEK_FRI, COL_DAY_OF_WEEK_SAT, COL_DAY_OF_WEEK_SUN]

# Column labels of hour one-hot encoded variables

COL_HOUR_0 = settings.COL_HOUR + '_' + HR_0
COL_HOUR_1 = settings.COL_HOUR + '_' + HR_1
COL_HOUR_2 = settings.COL_HOUR + '_' + HR_2
COL_HOUR_3 = settings.COL_HOUR + '_' + HR_3
COL_HOUR_4 = settings.COL_HOUR + '_' + HR_4
COL_HOUR_5 = settings.COL_HOUR + '_' + HR_5
COL_HOUR_6 = settings.COL_HOUR + '_' + HR_6
COL_HOUR_7 = settings.COL_HOUR + '_' + HR_7
COL_HOUR_8 = settings.COL_HOUR + '_' + HR_8
COL_HOUR_9 = settings.COL_HOUR + '_' + HR_9
COL_HOUR_10 = settings.COL_HOUR + '_' + HR_10
COL_HOUR_11 = settings.COL_HOUR + '_' + HR_11
COL_HOUR_12 = settings.COL_HOUR + '_' + HR_12
COL_HOUR_13 = settings.COL_HOUR + '_' + HR_13
COL_HOUR_14 = settings.COL_HOUR + '_' + HR_14
COL_HOUR_15 = settings.COL_HOUR + '_' + HR_15
COL_HOUR_16 = settings.COL_HOUR + '_' + HR_16
COL_HOUR_17 = settings.COL_HOUR + '_' + HR_17
COL_HOUR_18 = settings.COL_HOUR + '_' + HR_18
COL_HOUR_19 = settings.COL_HOUR + '_' + HR_19
COL_HOUR_20 = settings.COL_HOUR + '_' + HR_20
COL_HOUR_21 = settings.COL_HOUR + '_' + HR_21
COL_HOUR_22 = settings.COL_HOUR + '_' + HR_22
COL_HOUR_23 = settings.COL_HOUR + '_' + HR_23

cols_hr = [COL_HOUR_0, COL_HOUR_1, COL_HOUR_2, COL_HOUR_3, COL_HOUR_4, COL_HOUR_5, COL_HOUR_6, COL_HOUR_7, COL_HOUR_8,
           COL_HOUR_9, COL_HOUR_10, COL_HOUR_11, COL_HOUR_12, COL_HOUR_13, COL_HOUR_14, COL_HOUR_15, COL_HOUR_16,
           COL_HOUR_17, COL_HOUR_18, COL_HOUR_19, COL_HOUR_20, COL_HOUR_21, COL_HOUR_22, COL_HOUR_23]

# Column labels of all one-hot encoded variables
cols_categorical = []

cols_categorical.extend(cols_weather)
cols_categorical.extend(cols_day_of_wk)
cols_categorical.extend(cols_hr)

# Construct list of column labels of numerical and one-hot encoded variables
cols_all = []

cols_all.extend(cols_numerical)
cols_all.extend(cols_categorical)

# Selected features for machine learning pipeline
ML_FEATURES = [settings.COL_REG_SCOOTER,
               settings.COL_GUEST_SCOOTER,
               settings.COL_TEMP,
               settings.COL_REL_HUMIDITY,
               COL_WEATHER_CLEAR,
               COL_WEATHER_LIGHT_SNOW_RAIN,
               COL_DAY_OF_WEEK_SUN,
               COL_HOUR_0, COL_HOUR_1, COL_HOUR_2, COL_HOUR_3, COL_HOUR_4, COL_HOUR_5, COL_HOUR_8,
               COL_HOUR_16, COL_HOUR_17, COL_HOUR_18, COL_HOUR_19]

ML_TARGET = COL_ACT_SCOOTER


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

    # Convert date column from string to datetime data type
    df_rentals[settings.COL_DATE] = pd.to_datetime(df_rentals[COL_DATE_STR])

    # Create datetime column by concatenating the date and hr columns
    df_rentals[COL_DATETIME] = df_rentals.apply(lambda row: row[COL_DATE_STR] + ' ' + str(row[settings.COL_HOUR]),
                                                axis=1) + ':00'

    # Convert datetime column from string to datetime data type
    df_rentals[COL_DATETIME] = pd.to_datetime(df_rentals[COL_DATETIME])

    # Clean hr column ##################################################################################################

    # Rename hr column to hr_str to indicate string data type
    df_rentals.rename(columns={settings.COL_HOUR: COL_HOUR_STR}, inplace=True)

    # Convert hr column from int to string data type
    df_rentals[COL_HOUR_STR] = df_rentals[COL_HOUR_STR].apply(str)

    # Convert the hr column from string to categorical data type
    df_rentals[settings.COL_HOUR] = df_rentals[COL_HOUR_STR].astype('category')

    # Clean weather column #############################################################################################

    # Standardized weather column to lower case characters
    df_rentals[settings.COL_WEATHER] = df_rentals[settings.COL_WEATHER].str.lower()

    dict_weather = {

        # Replace incorrect values 'lear' and 'clar' with 'clear'[]
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

    # Convert the weather column from string to categorical data type
    df_rentals[settings.COL_WEATHER] = df_rentals[settings.COL_WEATHER].astype('category')

    # Clean relative_humidity column ###################################################################################

    # Drop observations with relative humidity value of 0
    df_rentals.drop(df_rentals[df_rentals[settings.COL_REL_HUMIDITY] == 0].index, inplace=True)

    # Clean guest_scooter, registered_scooter columns ##################################################################

    # Set all negative values in the guest_scooter column to 0
    df_rentals.loc[df_rentals[settings.COL_GUEST_SCOOTER] < 0, settings.COL_GUEST_SCOOTER] = 0

    # Set all negative values in the registered_scooter column to 0
    df_rentals.loc[df_rentals[settings.COL_REG_SCOOTER] < 0, settings.COL_REG_SCOOTER] = 0

    # Remove duplicate observations ####################################################################################

    # Drop duplicate observations
    df_rentals.drop_duplicates(inplace=True)

    return df_rentals


def create_target_variable(df_rentals):

    # Create active_scooter column as target variable
    df_rentals[COL_ACT_SCOOTER] = df_rentals[settings.COL_GUEST_SCOOTER] + df_rentals[settings.COL_REG_SCOOTER]

    return df_rentals


def engineer_features(df_rentals):

    # Create day_of_wk column as independent variable
    df_rentals[COL_DAY_OF_WEEK] = df_rentals.apply(lambda row: row[COL_DATETIME].strftime('%A'), axis=1)

    # Convert the day_of_wk column from string to categorical data type
    df_rentals[COL_DAY_OF_WEEK] = df_rentals[COL_DAY_OF_WEEK].astype('category')

    # One-hot encode the hr column
    df_rentals_1hot = pd.get_dummies(df_rentals, columns=[settings.COL_HOUR], prefix=[settings.COL_HOUR])

    # Create binary values for weather category values
    df_rentals_1hot = pd.get_dummies(df_rentals_1hot, columns=[settings.COL_WEATHER], prefix=[settings.COL_WEATHER])

    # One-hot encode the day_of_wk column
    df_rentals_1hot = pd.get_dummies(df_rentals_1hot, columns=[COL_DAY_OF_WEEK], prefix=[COL_DAY_OF_WEEK])

    return df_rentals_1hot


def remove_outliers(df_rentals):

    # Remove outliers from registered_scooter, guest_scooter and windspeed base on their maximum values in the box plots
    df_rentals = df_rentals[df_rentals.registered_scooter <= 3491]
    df_rentals = df_rentals[df_rentals.guest_scooter <= 346]
    df_rentals = df_rentals[df_rentals.windspeed <= 32]

    return df_rentals


def scale_features(df_rentals_1hot):

    cols_X = cols_all.copy()

    X = df_rentals_1hot[cols_X]

    std_scaler = StandardScaler()

    # Standard scale the independent variables
    X_scaled = std_scaler.fit_transform(X)
    X_scaled = pd.DataFrame(X_scaled, columns=X.columns)

    return X_scaled


# TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #####
'''
df = get_rentals()

if df is not None:

    # Before cleaning
    print(df.shape)

    df_cleaned = clean_data(df)

    # After cleaning
    print(df.shape)
'''
