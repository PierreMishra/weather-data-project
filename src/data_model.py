# Import libraries
from sqlalchemy import Column, Integer, Float, String, Date, ForeignKey, PrimaryKeyConstraint #only import necessary classes
from sqlalchemy.orm import declarative_base, relationship   #to create Base class (catalog field mappings)

# Define Base class
Base = declarative_base()

# Dimension Table - Define table structure for weather stations
class WeatherStation(Base):

    # Assign table name in the database
    __tablename__ = 'dim_weather_station'

    # Define columns and data types
    id = Column(Integer, primary_key=True, nullable=False)
    station_id = Column(String(20), nullable=False)
    station_name = Column(String(50))
    state = Column(String(12))

    # Define relationship with the fact table
    weather_data = relationship('WeatherData', back_populates='station')

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.id} {self.station_id} {self.station_name} {self.state})"


# Dimension Table - Define table structure for weather stations (dimension table)
class Date(Base):

    # Assign table name in the database
    __tablename__ = 'dim_date'

    # Define columns and data types
    id = Column(Integer, primary_key=True, nullable=False)
    date_id = Column(Integer, nullable=False)
    date_alternate = Column(Date, nullable=False)
    day = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    year = Column(Integer, nullable=False)

    # Define relationship with the fact table
    weather_data = relationship('WeatherData', back_populates='date')

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.id} {self.date_id} {self.date_alternate} {self.day} {self.month} {self.year})"

# Fact Table - Define table structure for weather data (fact table)
class WeatherData(Base):
    
    # Assign table name in the database
    __tablename__ = 'fact_weather_data'

    # Define columns and data types
    id = Column(Integer, primary_key=True)
    date_id = Column(Integer, ForeignKey(Date.date_id), primary_key=True) #composite key 1
    station_id = Column(Integer, ForeignKey(WeatherStation.id), primary_key=True) #composite key 2
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)

    # Define relationship with the dimension tables
    station = relationship('WeatherStation', back_populates='weather_data')
    date = relationship('Date', back_populates='weather_data')

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.id} {self.date} {self.station} {self.max_temp} {self.min_temp} {self.precipitation})"

