# Import libraries
import sys
sys.path.append('./src')
import sqlite3
import pandas as pd
from data_model import WeatherSummary
from functions import create_db_connection, update_database_table
import logging
from datetime import datetime

# Configure the logging file
logging.basicConfig(filename='db.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

# Set the start time for log file
logging.info('Ingesting data in analysis_weather_summary')
start_time = datetime.now()

# Create database connection to execute SQL
conn = sqlite3.connect('./database/weather.db')
cursor = conn.cursor()
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

# Get a list of existing station ids in the summary table
station_id = [r.station_id for r in session.query(WeatherSummary.station_id)]

# Store records not in the summary table based on year and station ids
new_summary = query[~query['station_id'].isin(station_id) & ~query['year'].isin(year)]

# Push to summary table
update_database_table(new_summary, WeatherSummary, session)

# Log completion
num_records = len(new_summary)
end_time = datetime.now()
duration = (end_time - start_time).total_seconds()
logging.info(f'{num_records} records ingested in analysis_weather_summary in {duration:.2f}s')







# Create a cursor object to execute SQL statements
conn = sqlite3.connect('./database/weather.db')
cur = conn.cursor()
# Execute the SQL statement to drop the table
#cur.execute('DROP TABLE analysis_weather_summary')
cur.execute("SELECT * from dim_weather_station where station_id='USC00338830'")
cur.fetchall()
# Commit the changes to the database
conn.commit()

cur.execute("SELECT * from reference_nasa_table where station_id='USC00338830'")
cur.fetchall()
# # Close the cursor and the database connection
# cur.close()

# query.total_precip.sum() == check.precipitation.sum()


# check = pd.read_sql('''
# SELECT
#     * from fact_weather_data
# ''', conn)


# check = pd.read_sql('''
# SELECT
#     * from fact_weather_data
# WHERE min_temp is NULL
# ''', conn)

