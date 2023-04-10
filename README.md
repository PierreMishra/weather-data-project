<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a>
    <img src="images/icon.png" alt="Logo" width="80" height="80">
  </a>

  <h3 align="center">Weather Station API</h3>

  <p align="center">
    Building a Data Pipeline and Flask API using Python
  </p>
</div>

# Overview

<p align="center">
    <img src="images/api_swagger.png">
</p>

This project contains 4 main parts:
1. `data_model.py` - Designing a data model to represent the weather data records using SQLAlchemy ORM and SQLite3 database.
2. `data_ingestion.py` - Processing and ingesting the weather data stored in the wx_data folder into the database.
3. `data_analysis.py` - Performing calculations on ingested weather station data
4. `api.py` - Creating a Flask API and developing Swagger/OpenAPI documentation

I also defined some supporting functions and unit tests that are provided in the following modules:
`functions.py` `unit_tests.py`

All the python modules are located inside the `src` folder.

The SQLite3 database is stored in `database` folder. Due to the GitHub size limits, the `weather.db` file only contains empty data model.

Data ingestion logs are stored in `db.log` file in the root folder.

## Getting Started

Clone this GitHub repository to your local machine or download it as a ZIP file. Then run the following steps to execute the data pipeline locally and interact with the API.

## Prerequisites and Installations 

Using the terminal, set the working directory to the root folder where the procject was cloned or unzipped. Open up terminal (command prompt on Windows) and use the `cd` command to navigate to the project folder.
```sh
  cd /your/local/folder
```

Based on your OS and python version installed, use one of the following to setup a virtual environment

#### * Create virtual environment 
Mac/Linux:
```sh
python3 -m venv venv
```
Windows:
```sh
python -m venv venv
```
NOTE: Using `python` or `python3` in Window's command prompt would depend on your python system installation.

#### Activate virtual environment
Mac/Linux: 
```sh
source venv/bin/activate
```
Windows:
```sh
venv\Script\activate
```
NOTE: Alternatively, `venv\bin\activate` may be used depending on your python installation.

#### Installing requirements in the virtual environment
Before running requirements.txt, run:
pip3 install reverse_geocode==1.4.1

Then:
pip3 install -r requirements.txt

### Running data pipeline

To run the data pipeline (runtime depends on your device, progress can be tracked using db.log file):
On Mac/Linux:
python3 src/run.py

On Windows (either one depending on your version of python):
python3 src/run.py
python src/run.py

### Running Flask API

To run the api:
On Mac/Linux:
python3 src/api.py

On Windows (either one depending on your python configuration):
python3 src/api.py
python src/api.py

Once you see the server running in terminal, go to the following endpoint to see the Swagger/OpenAPI style documentation
127.0.0.1:5000/swagger

To get JSON response use the following endpoints
127.0.0.1:5000/weather
127.0.0.1:5000/weather/stats




Database, logs and api UI

### Components

Our data pipeline is executed using run.py

This executes the following modules:
data_model.py -> data_ingestion.py -> data_analysis.py

Describe the workflow and general architecture
Add workflow diagram

### Contact

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

Many thanks to Othneil Drew for creating this beautiful README template

* [README Template](https://github.com/othneildrew/Best-README-Template)

<p align="right">(<a href="#readme-top">back to top</a>)</p>