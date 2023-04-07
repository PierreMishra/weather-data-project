import sys
sys.path.append('./src')                   #to load from other scripts in the same folder
from sqlalchemy import create_engine #to create database engine to interact with db
from sqlalchemy.orm import sessionmaker    #to create db sessions
from data_model import Base, WeatherData   #import Base and WeatherData classes from data_model.py

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

