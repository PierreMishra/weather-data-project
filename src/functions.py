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

# Function to connect to database and create tables if they do not exist
def create_db_connection(x):

    # Create an engine that connects to the SQLite database (if it does not exist)
    engine = create_engine(x)

    # Create a Session maker class to create sessions for interacting with db
    Session = sessionmaker(bind=engine)
    session = Session()

    # Create all the tables inheriting from base class if they do not exist
    # Use the Base class to create all the tables defined in data_model if they do not exist
    Base.metadata.create_all(engine)

    return session

# Function to parse station information retrieved from NASA's url
def parse_url_response(url):
    response = requests.get(url) #send http get request to the URL
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

# Function to find state given latitude and longitude
# def get_state(latitude, longitude):
#     geolocator = Nominatim(user_agent="weather_stations")
#     location = geolocator.reverse(f"{latitude}, {longitude}", exactly_one=True, timeout=15)
#     address = location.raw['address'] #from dictiionary get various address components
#     state = address.get('state', '')
#     return state

def find_state(pd_series_latitude, pd_series_longitude):

    # Create a list of tuples for the lat-lon pairs
    coordinates = list(zip(pd_series_latitude, pd_series_longitude))

    # Perform reverse geocoding
    results = rg.search(coordinates)

    # Extract state names from the results
    states = [r['admin1'] for r in results]

    return states





#add a unit test to check data type for each function