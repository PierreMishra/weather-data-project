''' 
This script contains a Flask app in the form of REST API to retrieve 
raw and summarized weather data from the database. It also allows an
user to filter the data using query parameters. 

A Swagger/OpenAPI type UI is also created for API documentation and testing.
'''

# Import libraries
import sys
import json
from flask import Flask, jsonify
from flask_restful import Api, Resource, reqparse
from flask_swagger_ui import get_swaggerui_blueprint
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from data_model import WeatherStation, WeatherData, RecordDate, WeatherSummary

sys.path.append('./src')  #locate python modules

# Create a new Flask application and set configuration
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database/weather.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
api = Api(app)

# Connect to the database
engine = create_engine('sqlite:///database/weather.db', echo=True)
Session = sessionmaker(bind=engine)

# Parse the query parameters
parser = reqparse.RequestParser()
parser.add_argument('date', type=int, location='args') #location='args' for URL query string
parser.add_argument('year', type=int, location='args')
parser.add_argument('station_id', type=str, location='args')
parser.add_argument('state', type=str, location='args')
parser.add_argument('page', type=int, location='args', default=1)
parser.add_argument('limit', type=int, location='args', default=100)

class WeatherAPI(Resource):
    ''' Create a resource class to retrieve raw weather data'''
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

            # Filter if state is provided in URL
            if args['state']:
                query = query.filter(WeatherStation.state == args['state'])

            # Implement pagination
            offset = (args['page'] - 1) * args['limit'] #skip records to navigate to other pg
            query = query.offset(offset).limit(args['limit']) 

            # Build the response - list of dictionaries for each record
            result = []
            for weather_data, weather_station, record_date in query.all():
                result.append({
                    'station_id': weather_station.station_id,
                    'station_name': weather_station.station_name,
                    'state': weather_station.state,
                    'date': record_date.date_alternate.strftime('%Y-%m-%d'),
                    'max_temp': weather_data.max_temp,
                    'min_temp': weather_data.min_temp,
                    'precipitation': weather_data.precipitation
                })
            return jsonify(result)

class WeatherStatsAPI(Resource):
    ''' Create a resource class to retrieve summarized weather data'''
    def get(self):

        # Parse arguments
        args = parser.parse_args()
        
        # Query the database
        with Session() as session:
            query = session.query(WeatherSummary)

            # Filter by date
            if args['year']:
                query = query.filter(WeatherSummary.year == args['year'])

            # Filter by station_id
            if args['station_id']:
                query = query.filter(WeatherSummary.station_id == args['station_id'])

            # Filter by state
            if args['state']:
                query = query.filter(WeatherSummary.state == args['state'])

            # Implement pagination
            offset = (args['page'] - 1) * args['limit'] #skip records to navigate to other pg
            query = query.offset(offset).limit(args['limit'])

            # Build the response
            result = []
            for weather_summary in query.all():
                result.append({
                    'station_id': weather_summary.station_id,
                    'state': weather_summary.state,
                    'year': weather_summary.year,
                    'avg_max_temp': weather_summary.avg_max_temp,
                    'avg_min_temp': weather_summary.avg_min_temp,
                    'total_precip': weather_summary.total_precip
                })

            return jsonify(result)

# Register the resources with the API endpoints
api.add_resource(WeatherAPI, '/weather')
api.add_resource(WeatherStatsAPI, '/weather/stats')

# Configure Swagger UI
SWAGGER_URL = '/swagger'
API_URL = '/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Weather Stations API"
    }
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

@app.route('/swagger.json')
def swagger():
    ''' JSON document to display the API documentation in Swagger UI'''
    with open('api_swagger.json', 'r', encoding='utf-8') as f:
        return jsonify(json.load(f))

if __name__ == '__main__':
    app.run(debug=True, port=5000)

