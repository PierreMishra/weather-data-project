# Import libraries
import sys
sys.path.append('./src')                   #to load from other scripts in the same folder
import os                                  #to interact with local files
from datetime import datetime              #to work with datetime type
import pandas as pd
import glob #may need to remove because i can do same stuff in os library
from data_model import WeatherStation, Date, WeatherData
from functions import create_db_connection, parse_url_response, find_state

# Configure connection to SQLite weather database
session = create_db_connection('sqlite:///database/weather.db')

# Define the directory where the text files are located
data_folder = './wx_data'

# Get all the file names 
file_names = os.listdir(data_folder)

# Retrieve station id from all files ending with .txt
stations_txt = [os.path.splitext(f)[0] for f in file_names if f.endswith('.txt')]

# Check if all stations exist in the database weather station dimension table
# if not, retrive station attributes using GET request to NASA website
stations_db = session.query(WeatherStation.station_id).distinct().all()

# Determine if we need to run HTTP request for any new station
for s in stations_txt:
    if s not in stations_db:
        nasa_request = True

# Get station attributes of new stations (!!! Takes 1-2 minutes !!!)
if nasa_request == True:
    
    # Obtain a list of all weather stations
    url = 'https://data.giss.nasa.gov/gistemp/station_data_v4/station_list.txt'
    all_stations = parse_url_response(url) #get all station list
    us_stations = all_stations[all_stations['station_id'].str.startswith('USC')] #keep US stations

    # Get those stations from the list of US stations that are in the .txt files but not in the database
    new_stations = us_stations[~us_stations['station_id'].isin(stations_db) & us_stations['station_id'].isin(stations_txt)]

    # Get state for each station
    new_stations.loc[:, 'state'] = find_state(new_stations['lat'], new_stations['lon'])

# Push new stations to the station dimension table









# Split the string by whitespace and create new columns
station_list = station_list.rename(columns={station_list.columns[0]: 'column'}) #remove unparsed column name

def parse_row(row):
    split_pattern = r'\s{2,}'
    first_space_idx = row.find(' ')
    station_id = row[:first_space_idx]
    station_info = re.split(split_pattern, row[first_space_idx:].strip())
    return [station_id] + station_info

station_list['column'] = station_list['column'].apply(lambda x: parse_row(x))

# Split column into 5 separate columns
station_list = pd.DataFrame(station_list['column'].tolist(), columns=['station_id', 'station_name', 'lat', 'lon', 'bi'])

# Remove trailing whitespace from column names
station_list.columns = station_list.columns.str.strip()






# Define regular expression pattern to split the string
pattern = r'\s{2,}' #check for 2 spaces
split_pattern = r'\s{2,}'

def parse_row(row):
    first_space_idx = row.find(' ')
    station_id = row[:first_space_idx]
    station_info = re.split(split_pattern, row[first_space_idx:].strip())
    return [station_id] + station_info

# Split the string into columns

columns = re.split(pattern, example_string.strip())



# replace "\n" with "," and create a StringIO object
data_io = io.StringIO(data.replace('\n', ','))

# read the data from the StringIO object
df = pd.read_csv(data_io, sep='\s+', header=None)

rows = []
for line in data.strip().split('\n'):
    row = line.strip().split()
    rows.append(row)

df = pd.DataFrame(rows, columns=['ID', 'Station', 'Name', 'Lat', 'Lon', 'Elevation'])




station_list = pd.read_csv(
    io.StringIO(data), # assuming the delimiter is whitespace
    skiprows=39, # skip the first 39 rows
    header=None, # no header row in the data
    names=['station_id', 'name', 'lat', 'lon', 'alt', 'unknown'], # column names
    usecols=['station_id', 'name', 'lat', 'lon', 'alt'] # select only required columns
)



# Get a list of text files to read
files = glob.glob(f'{data_folder}/*.txt')

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