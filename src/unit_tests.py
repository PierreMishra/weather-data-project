''' This script contains unit tests to check the functionality of our weather data Flask REST API'''   
import json
import unittest
import sys
from api import app

sys.path.append('./src') #locate python modules

class TestWeatherAPI(unittest.TestCase):
    ''' Create a class to perform unit testing'''

    def setUp(self):
        ''' Create Flask app object'''
        self.app = app.test_client()
        self.app.testing = True #testing environment

    def test_weather_json(self):
        ''' Check if the weather api GET request returns a json with 200 success status code '''
        response = self.app.get('/weather')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, 'application/json')

    def test_weather_stats_json(self):
        ''' Check if the weather/stats api GET request returns a json with 200 success status code '''
        response = self.app.get('/weather/stats')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, 'application/json')

    def test_weather_query_params(self):
        ''' 
        Check if weather api with query parameters returns 200
        Check if query parameters are working as expected
        '''
        response = self.app.get('/weather?state=Nebraska&limit=5')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.get_data())
        self.assertLessEqual(len(data), 5)
        for record in data:
            self.assertEqual(record['state'], 'Nebraska')

    def test_weather_stats_query_params(self):
        ''' 
        Check if weather/stats api with query parameters returns 200
        Check if query parameters are working as expected
        '''
        response = self.app.get('/weather/stats?station_id=USC00338830&year=2000')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.get_data())
        for record in data:
            self.assertEqual(record['station_id'], 'USC00338830')
            self.assertEqual(record['year'], 2000)

if __name__ == '__main__':
    unittest.main()