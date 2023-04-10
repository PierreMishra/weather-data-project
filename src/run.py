
''' This script runs our data pipeline. Logs can be seen in database.log file in the root folder'''   
import sys
from data_ingestion import data_ingestion

sys.path.append('./src') #locate python modules

def main():

    # Ingest data to fact and dimension tables in the database
    data_ingestion()

    # Perform summary analysis on weather records
    data_analysis()

    # Open swagger documentation if user chooses to
    # Check if command line argument was passed
    if len(sys.argv) > 1:
        # Access the command line argument
        arg = sys.argv[1]
        # Do something based on the command line argument
        if arg == '--verbose':
            print('Verbose mode enabled')
    else:
        # No command line argument passed
        print('No command line argument passed')