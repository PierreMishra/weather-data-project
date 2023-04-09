''' 
3 main steps:

Talk about state stuff
'''

# Import libraries
import sys
sys.path.append('./src')                   #to load from other scripts in the same folder
import os                                  #to interact with local files
from datetime import datetime              #to work with datetime type
import pandas as pd
import glob #may need to remove because i can do same stuff in os library
from data_model import WeatherStation, RecordDate, WeatherData
from functions import create_db_connection, create_reference_station_table, update_database_table
import sqlite3 #to directly connect to database (used for creating reference stations list table)
from sqlalchemy.exc import SQLAlchemyError #to catch db errors
from sqlalchemy import select, join #to run sqlalchemy core

if __name__ == '__main__':

    # Configure connection to SQLite weather database
    session = create_db_connection('sqlite:///database/weather.db')

    # Define the directory where the text files are located
    DATA_FOLDER = './wx_data'

    # Get all the file names 
    file_names = os.listdir(DATA_FOLDER)

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
    new_stations = (reference_stations[~reference_stations['station_id']
                                    .isin(stations_db) & reference_stations['station_id'].isin(stations_txt)])

    # Add the new stations to dim_weather_station if nullable condition for station_id is satisfied
    if not new_stations['station_id'].isnull().values.any():
        update_database_table(new_stations, WeatherStation, session)
    else:
        print("Station ID can not be null") #change to log later

    # --- Populate/Update dimension table: dim_date

    # Get a list of text files to read
    files = glob.glob(f'{DATA_FOLDER}/*.txt')

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
    dates_db = [r.date_id for r in session.query(RecordDate.date_id)]

    # Get new dates not in date dimension table
    new_dates = dates_df[~dates_df['date_id'].isin(dates_db)]

    # Add the new dates to dim_weather_station ONLY if date_id is not null
    if not new_dates['date_id'].isnull().values.any():
        update_database_table(new_dates, RecordDate, session)
    else:
        print("Date ID can not be null") #change to log later

    # --- Populate/update fact table - fact_weather_data

    weather_data_list = []

    conn = sqlite3.connect('./database/weather.db')
    cursor = conn.cursor()
    for file_name in os.listdir(DATA_FOLDER):
        if file_name.endswith('.txt'):
            # Extract the weather station id from the file name
            station_id = file_name.split('.')[0]
            
            # Open the file and read its contents
            with open(os.path.join(DATA_FOLDER, file_name), 'r', encoding='utf-8') as file:
                
                for line in file:
                    
                    # Split the line into its four components
                    date_txt, max_temp_txt, min_temp_txt, precip_txt = line.strip().split('\t')

                    # Join on dimension tables to populate the fact table
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

                    # Fetch the first row from the result set
                    existing_record = cursor.fetchone()

                    # If the record doesn't exist, add it to the fact table
                    if existing_record is None:
                        date_id = int(date_txt)
                        query = cursor.execute(f"SELECT station_key FROM dim_weather_station WHERE station_id = '{station_id}'")
                        station_key = cursor.fetchone()[0] #extract the first part of tuple
                        max_temp = None if max_temp_txt == '-9999' else float(max_temp_txt) / 10 #assign NULL to -9999 values
                        min_temp = None if min_temp_txt == '-9999' else float(min_temp_txt) / 10 #assign NULL to -9999 values
                        precipitation = None if precip_txt == '-9999' else float(precip_txt) / 10 #assign NULL to -9999 values
                        weather_data = WeatherData(date_id=date_id, station_key=station_key, max_temp=max_temp, min_temp=min_temp, precipitation=precipitation)
                        weather_data_list.append(weather_data)

    # Push to the fact table
    session.bulk_save_objects(weather_data_list)
    try:
        session.commit()
    except SQLAlchemyError as e:
        print(str(e)) # change to log later
        session.rollback()







# dates = []
# for file in files:
#     df = pd.read_csv(file, sep='\t', header=None, usecols=[0]) #read first column only
#     df = df.rename(columns={0: "date_id"})
#     dates.append(df)
# dates_df = pd.concat(dates).drop_duplicates()

# import time
# conn = sqlite3.connect('./database/weather.db')
# cursor = conn.cursor()
# t0 = time.time()
# for file_name in os.listdir(data_folder)[0:10]:
#     if file_name.endswith('.txt'):
#         # Extract the weather station id from the file name
#         station_id = file_name.split('.')[0]
        
#         # Open the file and read its contents
#         with open(os.path.join(data_folder, file_name), 'r') as file:
#             for line in file:
#                 # Split the line into its four components
#                 date_str, max_temp_str, min_temp_str, precip_str = line.strip().split('\t')
                
#                 # Convert the date string to a datetime object
#                 #date = datetime.strptime(date_str, '%Y%m%d')
                
#                 # Check if the record already exists in the fact table
#                 # existing_record = session.query(WeatherData).\
#                 #     join(WeatherData.station).\
#                 #     filter(WeatherStation.station_id == station_id).\
#                 #     filter(WeatherData.date_id == date_str).\
#                 #     first()
                
#                 # existing_record_query = (select(WeatherData)
#                 #     .select_from(join(WeatherData, WeatherStation, WeatherData.station_key == WeatherStation.station_key))
#                 #     .where((WeatherStation.station_id == station_id) & (WeatherData.date_id == date_str))
#                 # )
#                 # existing_record = session.execute(existing_record_query).scalar()

#                 query = cursor.execute(
#                     """
#                     SELECT *
#                     FROM fact_weather_data
#                     INNER JOIN dim_weather_station ON fact_weather_data.station_key = dim_weather_station.station_key
#                     WHERE dim_weather_station.station_id = ? AND fact_weather_data.date_id = ?
#                     LIMIT 1;
#                     """,
#                     (station_id, date_str)
#                 )

#                 # Fetch the first row from the result set
#                 existing_record = cursor.fetchone()

#                 # If the record doesn't exist, add it to the fact table
#                 if existing_record is None:
#                     date_id = date_str
#                     #station_query = select(WeatherStation.station_key).where(WeatherStation.station_id == station_id)
#                     #station_key = session.execute(station_query).scalar()
#                     query = cursor.execute(f"SELECT station_key FROM dim_weather_station WHERE station_id = '{station_id}'")
#                     station_key = cursor.fetchone()[0] #extract the first part of tuple
#                     max_temp = int(max_temp_str) / 10
#                     min_temp = int(min_temp_str) / 10
#                     precipitation = int(precip_str) / 10
#                     weather_data = WeatherData(date_id=date_id, station_key=station_key, max_temp=max_temp, min_temp=min_temp, precipitation=precipitation)
#                     weather_data_list.append(weather_data)
# print (str(time.time() - t0) + " secs")

# #---------

# conn = sqlite3.connect('./database/weather.db')
# cur = conn.cursor()
# t0 = time.time()
# for file_name in os.listdir(data_folder)[0:10]:
#     if file_name.endswith('.txt'):
#         # Extract the weather station id from the file name
#         station_id = file_name.split('.')[0]
        
#         # Open the file and read its contents
#         with open(os.path.join(data_folder, file_name), 'r') as file:
#             for line in file:
#                 # Split the line into its four components
#                 date_str, max_temp_str, min_temp_str, precip_str = line.strip().split('\t')
                
#                 # Convert the date string to a datetime object
#                 #date = datetime.strptime(date_str, '%Y%m%d')
                
#                 # Check if the record already exists in the fact table
#                 lala = pd.read_sql(f'''
#                 SELECT * FROM WeatherData
#                 INNER JOIN WeatherStation ON WeatherData.station_key = WeatherStation.station_key
#                 WHERE WeatherStation.station_id = {station_id}
#                 AND WeatherData.date_id = {date_str}
#                 ''', conn)
# print (str(time.time() - t0) + " secs")














    

# # # Push new stations to the station dimension table

# # Create a cursor object to execute SQL statements
# conn = sqlite3.connect('./database/weather.db')
# cur = conn.cursor()
# # Execute the SQL statement to drop the table
# cur.execute("DROP TABLE dim_date")
# cur.execute('DROP TABLE dim_weather_station')
# cur.execute('DROP TABLE fact_weather_data')
# cur.execute

# # Commit the changes to the database
# conn.commit()

# # Close the cursor and the database connection
# cur.close()
# #conn.close()


# # Define a SQL query to select the top 10 rows from the table
# query = "SELECT * from dim_date"
# # Use the read_sql() method to execute the query and return the results as a DataFrame
# results = pd.read_sql(query, conn)
# # Print the results
# print((results))








# # Retrieve all the station names
# stations = []
# for file in os.listdir(data_folder):
#     station = 

# # Read all the files and store in a dataframe
# combined = []
# for file in files:
#     df = pd.read_csv(file, sep='\t')
#     #df['station_id'] = file
#     combined.append(df)
# dfs = pd.concat(combined, axis=0)

# # If station does not exist, add to the station dimension table


#     #---retrieve which state does the new station(s) lie(s) in
#     #define it in functions


# # If a date does not exist, add to the date dimention table

# # If a particular date record for a station does not exist, add to the fact table
# from sqlalchemy import inspect

# inspector = inspect(engine)
# print(inspector.get_table_names())





# # Define the directory where the text files are located
# data_folder = './wx_data'

# # Loop over each text file in the directory
# for file in os.listdir(data_folder):

#     # Get the station name from the filename
#     station = os.path.splitext(file)[0]

#     # Open the text file
#     with open(os.path.join(data_folder, file), 'r') as f:

#         # Loop over each line in the text file
#         for line in f:

#             # Parse the data from the line
#             fields = line.strip().split('\t')
#             date = datetime.strptime(fields[0], '%Y%m%d').date()
#             max_temp = float(fields[1])
#             min_temp = float(fields[2])
#             precipitation = float(fields[3])

#             # Check if a record with the same station name and date already exists in the database
#             existing_data = session.query(WeatherData).filter_by(station=station, date=date).first()

#             # If the record does not exist, create a new WeatherData object and add it to the session
#             if not existing_data:
#                 data = WeatherData(date=date, station=station, max_temp=max_temp,
#                                    min_temp=min_temp, precipitation=precipitation)
#                 session.add(data)

#         # Commit the changes to the database
#         session.commit()

#         # Close the session
#         session.close()


# # Replace missing values -9999 to NULL