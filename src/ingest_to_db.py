# Import libraries
import sys
sys.path.append('./src')                   #to load from other scripts in the same folder
import os                                  #to interact with local files
from datetime import datetime              #to work with datetime type
import pandas as pd
import glob #may need to remove because i can do same stuff in os library
from data_model import WeatherStation, Date, WeatherData
from functions import create_db_connection, create_reference_station_table, update_dimensions
import sqlite3 #to directly connect to database (used for creating reference stations list table)
from sqlalchemy.exc import SQLAlchemyError #to catch db errors

# Configure connection to SQLite weather database
session = create_db_connection('sqlite:///database/weather.db')

# Define the directory where the text files are located
data_folder = './wx_data'

# Get all the file names 
file_names = os.listdir(data_folder)

# --- Populate/Update dimension table: dim_weather_station

# Retrieve station id from all files ending with .txt
stations_txt = [os.path.splitext(f)[0] for f in file_names if f.endswith('.txt')]
stations_txt = list(set(stations_txt)) #remove duplicates if more than 1 file for same station

# Retrieve station id already stored in the dim_weather_station table in db
stations_db = [r.station_id for r in session.query(WeatherStation.station_id)]

# Create a reference table to store various station attributes using NASA GISS weather station list
# Reverse geocode lat long to get states
# Create table if it does not exist in the database (1-2 minutes processing)
create_reference_station_table()

# Get stations not in the database from the list of US stations that are in the .txt files 
reference_stations = pd.read_sql('SELECT station_id, station_name, state FROM reference_nasa_table', 
                                 sqlite3.connect('./database/weather.db'))
new_stations = reference_stations[~reference_stations['station_id'].isin(stations_db) & reference_stations['station_id'].isin(stations_txt)]

# Add the new stations to dim_weather_station if nullable condition for station_id is satisfied
if not new_stations['station_id'].isnull().values.any():
    update_dimensions(new_stations, WeatherStation, session)
else:
    print("Station ID can not be null") #change to log later

# --- Populate/Update dimension table: dim_date

# Get a list of text files to read
files = glob.glob(f'{data_folder}/*.txt')

# Read all the files and store in a dataframe
dates = []
for file in files:
    df = pd.read_csv(file, sep='\t', header=None, usecols=[0]) #read first column only
    df = df.rename(columns={0: "date_id"})
    dates.append(df)
dates_df = pd.concat(dates).drop_duplicates()

# Create other date columns
dates_df['date_alternate'] = pd.to_datetime(dates_df['date_id'], format='%Y%m%d')
dates_df['day'] = dates_df['date_alternate'].dt.day
dates_df['month'] = dates_df['date_alternate'].dt.month
dates_df['year'] = dates_df['date_alternate'].dt.year

# Retrieve date id already stored in the dim_dates table in db
dates_db = [r.date_id for r in session.query(Date.date_id)]

# Get new dates not in date dimension table
new_dates = dates_df[~dates_df['date_id'].isin(dates_db)]

# Add the new stations to dim_weather_station if nullable condition for date_id is satisfied
if not new_dates['date_id'].isnull().values.any():
    update_dimensions(new_dates, Date, session)
else:
    print("Date ID can not be null") #change to log later


    

# Push new stations to the station dimension table

# Create a cursor object to execute SQL statements
conn = sqlite3.connect('./database/weather.db')
cur = conn.cursor()
# Execute the SQL statement to drop the table
cur.execute("DELETE from dim_date")
cur.execute('DROP TABLE fact_weather_data')

# Commit the changes to the database
conn.commit()

# Close the cursor and the database connection
cur.close()
#conn.close()


# Define a SQL query to select the top 10 rows from the table
query = "SELECT station_id from dim_weather_station"
# Use the read_sql() method to execute the query and return the results as a DataFrame
results = pd.read_sql(query, conn)
# Print the results
print(type(results))








# Retrieve all the station names
stations = []
for file in os.listdir(data_folder):
    station = 

# Read all the files and store in a dataframe
combined = []
for file in files:
    df = pd.read_csv(file, sep='\t')
    #df['station_id'] = file
    combined.append(df)
dfs = pd.concat(combined, axis=0)

# If station does not exist, add to the station dimension table


    #---retrieve which state does the new station(s) lie(s) in
    #define it in functions


# If a date does not exist, add to the date dimention table

# If a particular date record for a station does not exist, add to the fact table
from sqlalchemy import inspect

inspector = inspect(engine)
print(inspector.get_table_names())





# Define the directory where the text files are located
data_folder = './wx_data'

# Loop over each text file in the directory
for file in os.listdir(data_folder):

    # Get the station name from the filename
    station = os.path.splitext(file)[0]

    # Open the text file
    with open(os.path.join(data_folder, file), 'r') as f:

        # Loop over each line in the text file
        for line in f:

            # Parse the data from the line
            fields = line.strip().split('\t')
            date = datetime.strptime(fields[0], '%Y%m%d').date()
            max_temp = float(fields[1])
            min_temp = float(fields[2])
            precipitation = float(fields[3])

            # Check if a record with the same station name and date already exists in the database
            existing_data = session.query(WeatherData).filter_by(station=station, date=date).first()

            # If the record does not exist, create a new WeatherData object and add it to the session
            if not existing_data:
                data = WeatherData(date=date, station=station, max_temp=max_temp,
                                   min_temp=min_temp, precipitation=precipitation)
                session.add(data)

        # Commit the changes to the database
        session.commit()

        # Close the session
        session.close()


# Replace missing values -9999 to NULL