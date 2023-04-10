'''This script contains several functions that are used in other python modules'''

import sys
import logging
import io #to parse string response
from datetime import datetime
import sqlite3
import re #regular expression to parse station data from NASA
import requests
import reverse_geocoder as rg #reverse geocode to find state
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError #to catch db errors
from data_model import Base

sys.path.append('./src')

def create_db_connection(database_location):
    '''Function to connect to database and create tables if they do not exist'''

    # Create an engine that connects to the SQLite database (if it does not exist)
    engine = create_engine(database_location)
    # Create a Session maker class to create sessions for interacting with db
    Session = sessionmaker(bind=engine)
    session = Session()
    # Create all the tables inheriting from base class if they do not exist
    # Use the Base class to create all the tables defined in data_model if they do not exist
    Base.metadata.create_all(engine)
    return session

def parse_url_response(url):
    '''Function to parse station information from GET response retrieved from NASA's url'''
    # Send http get request to the URL
    response = requests.get(url, timeout=10)
    # Decode the response object
    data = response.content.decode('utf-8') 
    # Store in a dataframe, unparsed
    station_list = pd.read_csv(io.StringIO(data))
    # Split the string by whitespace and create new columns
    station_list = station_list.rename(columns={station_list.columns[0]: 'column'}) #remove unparsed column name
    def parse_row(row):
        split_pattern = r'\s{2,}' #double whitespace
        first_space_idx = row.find(' ') #find first single space
        station_id = row[:first_space_idx]
        station_info = re.split(split_pattern, row[first_space_idx:].strip())
        return [station_id] + station_info
    station_list['column'] = station_list['column'].apply(lambda x: parse_row(x))
    # Split column into 5 separate columns
    station_list = pd.DataFrame(station_list['column'].tolist(), columns=['station_id', 'station_name', 'lat', 'lon', 'bi'])
    # Remove trailing whitespace from column names
    station_list.columns = station_list.columns.str.strip()
    return station_list #dataframe

def find_state(pd_series_latitude, pd_series_longitude):
    '''Function to find state given a series of latitude and longitude values'''
    # Create a list of tuples for the lat-lon pairs
    coordinates = list(zip(pd_series_latitude, pd_series_longitude))
    # Perform reverse geocoding
    results = rg.search(coordinates)
    # Extract state names from the results
    states = [r['admin1'] for r in results]
    return states

def create_reference_station_table():
    '''Function to store reference weather station table retrieved from NASA GISS url'''
    # Direct connection to db
    conn = sqlite3.connect('./database/weather.db')
    # check if reference station table exists
    table_exists = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='reference_nasa_table'", conn).shape[0] > 0
    if not table_exists:
        # Retrieve the list of all weather stations and parse response into a dataframe
        url = 'https://data.giss.nasa.gov/gistemp/station_data_v4/station_list.txt'
        # Get all station list
        all_stations = parse_url_response(url)
        # Keep US stations
        us_stations = all_stations[all_stations['station_id'].str.startswith('USC')].copy()
        # Get state for each station
        us_stations['state'] = find_state(us_stations['lat'], us_stations['lon'])
        # Create if reference table does not exists
        us_stations.reset_index(drop=True).to_sql('reference_nasa_table', conn)
    conn.close()

def update_database_table(df, db_class, session):
    '''Function to create or update database tables using a dataframe'''

    # Convert rows to dictionary
    record_dictionary = df.to_dict(orient='records')
    # Create a list of objects
    objects = [db_class(**row) for row in record_dictionary]
    # Insert the WeatherStation objects
    session.add_all(objects)
    # Commit the session to insert the new stations into the database
    try:
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()

def database_log(df, start_time, table_name):
    ''' Log data ingestions to db.log'''       
    # Create log message
    record_len = len(df)
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f'{record_len} records ingested in {table_name} in {duration:.2f}s')