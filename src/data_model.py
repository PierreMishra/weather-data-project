'''
Insert info about this script
'''

# Import libraries
from sqlalchemy import Column, Integer, Float, String, Date, Numeric, ForeignKey #only import necessary classes
from sqlalchemy.orm import declarative_base, relationship   #to create Base class (catalog field mappings)

# Define Base class
Base = declarative_base()

class WeatherStation(Base):
    '''Dimension Table - Define table structure for weather stations'''

    # Assign table name in the database
    __tablename__ = 'dim_weather_station'

    # Define columns and data types
    station_key = Column(Integer, primary_key=True)
    station_id = Column(String(20), nullable=False)
    station_name = Column(String(50))
    state = Column(String(12))

    # Define relationship with the fact table
    weather_data = relationship('WeatherData', back_populates='station')

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.station_key} {self.station_id} {self.station_name} {self.state}"

class RecordDate(Base):
    '''Dimension Table - Define table structure for weather stations'''

    # Assign table name in the database
    __tablename__ = 'dim_date'

    # Define columns and data types
    date_key = Column(Integer, primary_key=True)
    date_id = Column(Integer, nullable=False)
    date_alternate = Column(Date, nullable=False)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)

    # Define relationship with the fact table
    weather_data = relationship('WeatherData', back_populates='date')

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.date_key} {self.date_id} {self.date_alternate} {self.day} {self.month} {self.year}"

class WeatherData(Base):
    '''Fact Table - Define table structure for weather data (fact table)'''
    
    # Assign table name in the database
    __tablename__ = 'fact_weather_data'

    # Define columns and data types
    date_id = Column(Integer, ForeignKey(RecordDate.date_id), primary_key=True) #composite key 1
    station_key = Column(Integer, ForeignKey(WeatherStation.station_key), primary_key=True) #composite key 2
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)

    # Define relationship with the dimension tables
    station = relationship('WeatherStation', back_populates='weather_data')
    date = relationship('RecordDate', back_populates='weather_data')

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.date_id} {self.station_key} {self.max_temp} {self.min_temp} {self.precipitation}"

class WeatherSummary(Base):
    '''Analysis - Define table structure to hold summarized data'''

    # Assign table name in the database
    __tablename__ = 'analysis_weather_summary'

    # Define columns and data types
    id = Column(Integer, primary_key=True)
    year = Column(Integer, nullable=False)
    station_id = Column(Integer, nullable=False)
    state = Column(String(12), nullable=False)
    avg_max_temp = Column(Float)
    avg_min_temp = Column(Float)
    total_precip = Column(Float)

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.id} {self.year} {self.station_id} {self.avg_max_temp} {self.avg_min_temp} {self.total_precip}"