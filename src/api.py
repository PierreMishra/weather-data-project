import sys
sys.path.append('./src')                   

from flask import Flask, jsonify, request
from flask_restful import Api, Resource, reqparse
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from data_model import Base, WeatherStation, WeatherData, RecordDate, WeatherSummary

# Create a new Flask application
app = Flask(__name__)
api = Api(app)

# Connect to the database
engine = create_engine('sqlite:///database/weather.db', echo=True)
Session = sessionmaker(bind=engine)

# Parse the query parameters
parser = reqparse.RequestParser()
parser.add_argument('date', type=str, location='args') #location='args' for URL query string
parser.add_argument('station_id', type=str, location='args')
parser.add_argument('page', type=int, location='args', default=1)
parser.add_argument('limit', type=int, location='args', default=100)

# Create a resource class
class WeatherAPI(Resource):
    def get(self):
        
        # Parse arguments
        args = parser.parse_args()

        # Query the database
        with Session() as session:
            
            # Join database tables
            query = session.query(WeatherData, WeatherStation, RecordDate)\
                           .filter(WeatherData.station_key == WeatherStation.station_key)\
                           .filter(WeatherData.date_id == RecordDate.date_id)

            # Filter if date argument is provided in URL
            if args['date']:
                query = query.filter(RecordDate.date_id == args['date'])

            # Filter if station_id is provided in URL
            if args['station_id']:
                query = query.filter(WeatherStation.station_id == args['station_id'])

            # Pagination
            offset = (args['page'] - 1) * args['limit'] #skip records to navigate to other pg
            query = query.offset(offset).limit(args['limit']) 

            # Build the response - list of dictionaries for each record
            result = []
            for weather_data, weather_station, record_date in query.all():
                result.append({
                    'date': record_date.date_alternate.strftime('%Y-%m-%d'),
                    'station_id': weather_station.station_id,
                    'station_name': weather_station.station_name,
                    'state': weather_station.state,
                    'max_temp': weather_data.max_temp,
                    'min_temp': weather_data.min_temp,
                    'precipitation': weather_data.precipitation
                })
            return jsonify(result)

class WeatherStatsAPI(Resource):
    def get(self):

        # Parse arguments
        args = parser.parse_args()
        
        # Query the database
        with Session() as session:
            query = session.query(WeatherSummary)

            # Filter by date
            if args['date']:
                query = query.filter(WeatherSummary.year == int(args['date'][0:4]))

            # Filter by station_id
            if args['station_id']:
                query = query.filter(WeatherSummary.station_id == int(args['station_id']))

            # Build the response
            result = []
            for weather_summary in query.all():
                result.append({
                    'year': weather_summary.year,
                    'station_id': weather_summary.station_id,
                    'state': weather_summary.state,
                    'avg_max_temp': weather_summary.avg_max_temp,
                    'avg_min_temp': weather_summary.avg_min_temp,
                    'total_precip': weather_summary.total_precip
                })

            return jsonify(result)

# Register the resources with the API
api.add_resource(WeatherAPI, '/api/weather')
api.add_resource(WeatherStatsAPI, '/api/weather/stats')

if __name__ == '__main__':
    app.run(debug=True)