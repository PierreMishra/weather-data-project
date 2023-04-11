''' 
This script is divided into 3 main sections:
1. Create or update a dimension table to store information about all/new the weather stations
2. Create or update a dimension table to store all/new dates in different formats and date components
3. Create or update a fact table to store all/new weather records
'''

# Import libraries
import sys
import os
import glob #for loading files in a folder
import sqlite3 #to directly connect to database (used for creating reference stations list table)
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy.exc import SQLAlchemyError #to catch db errors
from data_model import WeatherStation, RecordDate, WeatherData
from functions import create_db_connection, create_reference_station_table, update_database_table, database_log

sys.path.append('./src') #locate python modules

def data_ingestion():
    ''' This is the workflow for ingesting data to our database tables'''

    # Configure connection to SQLite weather database
    session = create_db_connection('sqlite:///database/weather.db')

    # Define the directory where the text files are located
    DATA_FOLDER = './wx_data'

    # Get all the file names 
    file_names = os.listdir(DATA_FOLDER)

    # ----------------------------------------------------
    # POPULATE/UPDATE DIMENSION TABLE: dim_weather_station
    # ----------------------------------------------------

    ## Set the start time for log file
    logging.info('Ingesting data in dim_weather_station')
    start_time = datetime.now()

    ## Retrieve station id from all files ending with .txt
    stations_txt = [os.path.splitext(f)[0] for f in file_names if f.endswith('.txt')]
    stations_txt = list(set(stations_txt)) #remove duplicates if more than 1 file for same station

    ## Retrieve station id already stored in the dim_weather_station table in db
    stations_db = [r.station_id for r in session.query(WeatherStation.station_id)]

    ## Create a reference table to store various station attributes using NASA GISS weather station list
    ## Reverse geocode lat long to get states
    ## Create table if it does not exist in the database (1-2 minutes processing)
    create_reference_station_table()

    ## Get stations not in the database from the list of US stations that are in the .txt files 
    reference_stations = pd.read_sql('SELECT station_id, station_name, state FROM reference_nasa_table', 
                                    sqlite3.connect('./database/weather.db'))
    new_stations = (reference_stations[~reference_stations['station_id']
                                    .isin(stations_db) & reference_stations['station_id'].isin(stations_txt)])

    ## Add the new stations to dim_weather_station if nullable condition for station_id is satisfied
    if not new_stations['station_id'].isnull().values.any():
        update_database_table(new_stations, WeatherStation, session)
    else:
        print("Station ID can not be null") #change to log later

    ## Log completion
    database_log(new_stations, start_time, "dim_weather_station")

    # -----------------------------------------
    # POPULATE/UPDATE DIMENSION TABLE: dim_date
    # -----------------------------------------

    ## Set the start time for log file
    logging.info('Ingesting data in dim_date')
    start_time = datetime.now()

    ## Get a list of text files to read
    files = glob.glob(f'{DATA_FOLDER}/*.txt')

    ## Read all the files and store in a dataframe
    dates = []
    for file in files:
        df = pd.read_csv(file, sep='\t', header=None, usecols=[0]) #read first column only
        df = df.rename(columns={0: "date_id"})
        dates.append(df)
    dates_df = pd.concat(dates).drop_duplicates()

    ## Create other date columns
    dates_df['date_alternate'] = pd.to_datetime(dates_df['date_id'], format='%Y%m%d')
    dates_df['day'] = dates_df['date_alternate'].dt.day
    dates_df['month'] = dates_df['date_alternate'].dt.month
    dates_df['year'] = dates_df['date_alternate'].dt.year

    ## Retrieve date id already stored in the dim_dates table in db
    dates_db = [r.date_id for r in session.query(RecordDate.date_id)]

    ## Get new dates not in date dimension table
    new_dates = dates_df[~dates_df['date_id'].isin(dates_db)]

    ## Add the new dates to dim_weather_station ONLY if date_id is not null
    if not new_dates['date_id'].isnull().values.any():
        update_database_table(new_dates, RecordDate, session)
    else:
        print("Date ID can not be null") #change to log later

    ## Log completion
    database_log(new_dates, start_time, "dim_date")

    # --------------------------------------------------
    # POPULATE/UPDATE DIMENSION TABLE: fact_weather_data
    # --------------------------------------------------

    ## Set the start time for log file
    logging.info('Ingesting data in fact_weather_data')
    start_time = datetime.now()

    weather_data_list = []

    conn = sqlite3.connect('./database/weather.db')
    cursor = conn.cursor()
    for file_name in os.listdir(DATA_FOLDER):
        if file_name.endswith('.txt'):
            ## Extract the weather station id from the file name
            station_id = file_name.split('.')[0]
            
            ## Open the file and read its contents
            with open(os.path.join(DATA_FOLDER, file_name), 'r', encoding='utf-8') as file:
                
                for line in file:
                    
                    ## Split the line into its four components
                    date_txt, max_temp_txt, min_temp_txt, precip_txt = line.strip().split('\t')

                    ## Join on dimension tables to populate the fact table
                    query = cursor.execute(
                        """
                        SELECT *
                        FROM fact_weather_data
                        INNER JOIN dim_weather_station ON fact_weather_data.station_key = dim_weather_station.station_key
                        WHERE dim_weather_station.station_id = ? AND fact_weather_data.date_id = ?
                        LIMIT 1;
                        """,
                        (station_id, int(date_txt))
                    )

                    ## Fetch the first row from the result set
                    existing_record = cursor.fetchone()

                    ## If the record doesn't exist, add it to the fact table
                    if existing_record is None:
                        date_id = int(date_txt)
                        query = cursor.execute(f"SELECT station_key FROM dim_weather_station WHERE station_id = '{station_id}'")
                        station_key = cursor.fetchone()[0] #extract the first part of tuple
                        max_temp = None if max_temp_txt == '-9999' else float(max_temp_txt) / 10 #assign NULL to -9999 values
                        min_temp = None if min_temp_txt == '-9999' else float(min_temp_txt) / 10 #assign NULL to -9999 values
                        precipitation = None if precip_txt == '-9999' else float(precip_txt) / 10 #assign NULL to -9999 values
                        weather_data = WeatherData(date_id=date_id, station_key=station_key, max_temp=max_temp, min_temp=min_temp, precipitation=precipitation)
                        weather_data_list.append(weather_data)

    ## Push to the fact table
    session.bulk_save_objects(weather_data_list)
    try:
        session.commit()
    except SQLAlchemyError as err:
        session.rollback()

    ## Log completion
    database_log(weather_data_list, start_time, "fact_weather_data")
