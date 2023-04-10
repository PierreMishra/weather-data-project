import sys
sys.path.append('./src')                   

from flask import Flask, jsonify, request
from flask_restful import Api, Resource, reqparse
from flask_swagger_ui import get_swaggerui_blueprint
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from data_model import Base, WeatherStation, WeatherData, RecordDate, WeatherSummary
import json


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

            # Pagination
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
                    'max_temp': f'{weather_data.max_temp} deg C',
                    'min_temp': f'{weather_data.min_temp} deg C',
                    'precipitation': f'{weather_data.precipitation} cm'
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

            # Build the response
            result = []
            for weather_summary in query.all():
                result.append({
                    'station_id': weather_summary.station_id,
                    'state': weather_summary.state,
                    'year': weather_summary.year,
                    'avg_max_temp': f'{weather_summary.avg_max_temp} deg C',
                    'avg_min_temp': f'{weather_summary.avg_min_temp} deg C',
                    'total_precip': f'{weather_summary.total_precip} cm'
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
    with open('swagger.json', 'r', encoding='utf-8') as f:
        return jsonify(json.load(f))

if __name__ == '__main__':
    app.run(debug=True)

