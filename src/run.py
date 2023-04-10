
''' This script runs our data pipeline. Logs can be seen in database.log file in the root folder'''   

import sys
import logging
from data_ingestion import data_ingestion
from data_analysis import data_analysis

sys.path.append('./src') #locate python modules

def main():

    # Ingest data to fact and dimension tables in the database
    data_ingestion()

    # Perform summary analysis on weather records
    data_analysis()

if __name__ == '__main__':
    
    # Configure the logging file
    logging.basicConfig(filename='db.log', level=logging.INFO,
                        format='%(asctime)s:%(levelname)s:%(message)s')
    
    # Run the program
    main()