import sys
sys.path.append('./src')                   #to load from other scripts in the same folder
from sqlalchemy import create_engine #to create database engine to interact with db
from sqlalchemy.orm import sessionmaker    #to create db sessions
from data_model import Base, WeatherData   #import Base and WeatherData classes from data_model.py
import requests #to send HTTP requests for station id information 
import re #regular expression to parse station data from NASA
import pandas as pd
import io #to parse string response
#from geopy.geocoders import Nominatim 
import reverse_geocoder as rg #reverse geocode to find state
import sqlite3 #to directly connect to database (used for creating reference stations list table)
from sqlalchemy.exc import SQLAlchemyError #to catch db errors
from datetime import datetime
import logging

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

    response = requests.get(url, timeout=10) #send http get request to the URL
    data = response.content.decode('utf-8') #decode the response object
    station_list = pd.read_csv(io.StringIO(data)) #store in a dataframe, unparsed
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

    conn = sqlite3.connect('./database/weather.db') #direct connection to db
    table_exists = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table' AND name='reference_nasa_table'", conn).shape[0] > 0 #check if reference station table exists  
    if not table_exists:  
        url = 'https://data.giss.nasa.gov/gistemp/station_data_v4/station_list.txt'           #retrieve the list of all weather stations and parse response into a dataframe
        all_stations = parse_url_response(url)                                                #get all station list
        us_stations = all_stations[all_stations['station_id'].str.startswith('USC')].copy()   #keep US stations
        us_stations['state'] = find_state(us_stations['lat'], us_stations['lon'])             #get state for each station
        us_stations.reset_index(drop=True).to_sql('reference_nasa_table', conn)               #create if reference table does not exists
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
        print(str(e)) # change to log later
        session.rollback()
    
def database_log(df, start_time, table_name):        
    # Create log message
    record_len = len(df)
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logging.info(f'{record_len} records ingested in {table_name} in {duration:.2f}s')

#add a unit test to check data type for each function