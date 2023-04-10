'''
This script uses a SQL statement to perform the summary calculations
and store the output in the analysis_weather_summary table in the database

'''
# Import libraries
import sys
import logging
import sqlite3
from datetime import datetime
import pandas as pd
from data_model import WeatherSummary
from functions import create_db_connection, update_database_table, database_log

sys.path.append('./src') #locate python modules

def data_analysis():
    ''' Performing summary statistics'''

    # Set the start time for log file
    logging.info('Ingesting data in dim_weather_station')
    start_time = datetime.now()

    # Create database connection to execute SQL
    conn = sqlite3.connect('./database/weather.db')
    query = pd.read_sql('''
    SELECT
        d.year AS year,
        s.station_id as station_id,
        s.state as state,
        PRINTF("%.3f", AVG(max_temp)) AS avg_max_temp,
        PRINTF("%.3f", AVG(min_temp)) AS avg_min_temp,
        PRINTF("%.3f", SUM(precipitation)) AS total_precip
    FROM
        fact_weather_data
    INNER JOIN dim_weather_station as s ON fact_weather_data.station_key = s.station_key
    INNER JOIN dim_date as d ON fact_weather_data.date_id= d.date_id
    GROUP BY
        s.station_key,
        d.year,
        s.state;
    ''', conn)

    # Create a db connection
    session = create_db_connection('sqlite:///database/weather.db')

    # Get a list of existing years in the summary table
    year = [r.year for r in session.query(WeatherSummary.year)]

    # Get a list of existing station ids in the summary table in database
    station_id = [r.station_id for r in session.query(WeatherSummary.station_id)]

    # Store records not already in the summary table based on year and station ids
    new_summary = query[~query['station_id'].isin(station_id) & ~query['year'].isin(year)]

    # Push to summary table in database
    update_database_table(new_summary, WeatherSummary, session)

    # Log completion
    database_log(new_summary, start_time, "analysis_weather_summary")
