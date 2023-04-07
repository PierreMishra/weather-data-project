# Import libraries
import sys
sys.path.append('./src')                   #to load from other scripts in the same folder
import os                                  #to interact with local files
from datetime import datetime              #to work with datetime type
from functions import create_db_connection
import pandas as pd

# Configure connection to our SQLite weather database
session = create_db_connection('sqlite:///database/weather.db')

# Define the directory where the text files are located
data_folder = './wx_data'

# Loop over each text file in the directory
for file in os.listdir(data_folder):

    # Get the station name from the filename
    station = os.path.splitext(file)[0]

    # Open the text file
    with open(os.path.join(data_folder, file), 'r') as f:

        # Create a session for interacting with the database
        session = Session()

        # Loop over each line in the text file
        for line in f:

            # Parse the data from the line
            fields = line.strip().split('\t')
            date = datetime.strptime(fields[0], '%Y%m%d').date()
            max_temp = float(fields[1])
            min_temp = float(fields[2])
            precipitation = float(fields[3])

            # Create a WeatherData object for the row of data
            data = WeatherData(date=date, station=station, max_temp=max_temp,
                               min_temp=min_temp, precipitation=precipitation)

            # Add the WeatherData object to the session
            session.add(data)

        # Commit the changes to the database
        session.commit()

        # Close the session
        session.close()