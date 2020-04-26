"""
Default script settings for data extraction from rentals data table

Configure parameters used to connect to the database that contains the rentals data table
Configure the SQL statement used to extract data from the rentals table
"""

# Microsoft SQL Server connection string parameters
# Database connection string requires database driver, host, name, user id and password
# For detail documentation on the pyodbc module, please refer to https://github.com/mkleehammer/pyodbc/wiki

DB_DRIVER = 'ODBC Driver 17 for SQL Server'
DB_HOST = 'aice.database.windows.net'
DB_NAME = 'aice'
DB_USERID= 'aice_candidate'
DB_PASSWORD = '@ic3_a3s0c1at3'

# Assemble the database connection string
DB_CONN_STR = f'Driver={DB_DRIVER}; Server={DB_HOST}; Database={DB_NAME}; UID={DB_USERID}; PWD={DB_PASSWORD};'


# Name of rentals table
TBL_RENTAL = 'rental_data'

# Column names of rentals data table
COL_DATE = 'date'
COL_HOUR = 'hr'
COL_WEATHER = 'weather'
COL_TEMP = 'temperature'
COL_FEELS_LIKE_TEMP = 'feels_like_temperature'
COL_REL_HUMIDITY = 'relative_humidity'
COL_WINDSPEED = 'windspeed'
COL_PSI = 'psi'
COL_GUEST_SCOOTER = 'guest_scooter'
COL_REG_SCOOTER = 'registered_scooter'
COL_GUEST_BIKE = 'guest_bike'
COL_REG_BIKE = 'registered_bike'

# List of table columns to be selected in SQL SELECT clause
SQL_SELECT_COLS = f'{COL_DATE}, {COL_HOUR}, {COL_WEATHER}, {COL_TEMP}, {COL_FEELS_LIKE_TEMP}, {COL_REL_HUMIDITY}, ' \
                  f'{COL_WINDSPEED}, {COL_PSI}, {COL_GUEST_SCOOTER}, {COL_REG_SCOOTER}'

# Start and end date values in the SQL WHERE clause
SQL_WHERE_DATE_START = '2011-01-01'
SQL_WHERE_DATE_END = '2012-12-31'

# Condition in the SQL WHERE clause
SQL_WHERE = f'{COL_DATE} BETWEEN \'{SQL_WHERE_DATE_START}\' AND \'{SQL_WHERE_DATE_END}\''

# Assemble SQL statement
SQL_RENTAL = f'SELECT {SQL_SELECT_COLS} FROM {TBL_RENTAL} WHERE {SQL_WHERE}'


# TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #### TESTING #####

# print(SQL_RENTAL)
# print(DB_CONN)
