# About The Project

Provide brief overalll summary

### Getting Started

Prerequisites 

In the terminal, 
cd to the working directory where the procject was cloned to or unzipped to.

Based on your OS and python version installed, use one of the following to setup a virtual environment

On Mac/Linux:
Create virtual environment: python3 -m venv venv
Activate virtual environment: source env/bin/activate

On Windows:
python -m venv venv
venv\Script\activate

Before running requirements.txt, run:
pip3 install reverse_geocode==1.4.1

Then:
pip3 install -r requirements.txt

To run the data pipeline:
On Mac/Linux:
python3 src/run.py

On Windows (either one depending on your version of python):
python3 src/run.py
python src/run.py

To run the api:
On Mac/Linux:
python3 src/api.py

On Windows (either one depending on your version of python):
python3 src/api.py
python src/api.py



Database, logs and api UI

### Components

Our data pipeline is executed using run.py

This executes the following modules:
data_model.py -> data_ingestion.py -> data_analysis.py

Describe the workflow and general architecture
Add workflow diagram

### Contact