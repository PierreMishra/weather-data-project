# --- Import libraries
from sqlalchemy import Column, Integer, Float, String, Date #only import necessary classes
from sqlalchemy.orm import declarative_base #to create Base class (catalog field mappings)

# --- Define Base class
Base = declarative_base()

# --- Define table representation for weather data
class WeatherData(Base):
    
    # Assign table name in the database
    __tablename__ = 'weather_data'

    # Define columns and data types
    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    station = Column(String(50), nullable=False)
    max_temp = Column(Float)
    min_temp = Column(Float)
    precipitation = Column(Float)

    # Define representation for debugging and logging purposes
    def __repr__(self):
        return f"{self.id} {self.date} {self.station} {self.max_temp} {self.min_temp} {self.precipitation})"

