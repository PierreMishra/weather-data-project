# AWS Deployment

There could be multiple ways to deploy our project on to AWS.

1. We could migrate to AWS by containerizing the whole application as a Docker container image and pushing it to `Amazon ECR` registry. We can then use an `Amazon EC2` instance to orchestrate and manage our Docker container. 

2. We could use an `Amazon S3` bucket to store all the .txt files and process and ingest them into an `Amazon RDS` database. To process, ingest and sumarize the data we could migrate our python code into `AWS Lambda` functions and use `AWS Glue` to schedule and orchestrate our data pipeline. On the API side of things, we could use `Amazon API Gateway` to create and publish our API. Alternatively, we could just use an `Amazon EC2` instance to deploy our existing Flask code as well. `Amazon Lightsail` is also an alternative to deploy light-weight containerized Flask app.

3. For data processing/ingestion, alternatively to using AWS Glue, we could also setup `Apache Airflow` on an `Amazon EC2` instance and use it to orchestrate and monitor our pipeline consisting of multple `AWS Lambda` functions. 

# Data Modeling

This is done using SQLAlchemy ORM in Python. Please refer to the python script `data_model.py` and the *Database Design* section of the `README.md` in the root folder. Comments and docstrings are also provided in the python script.

# Ingestion

Please refer to the python scripts `data_ingestion.py`, `run.py`, and `functions.py`. Comments and docstrings are also provided in the python scripts.

# Data Analysis

Please refer to the python scripts `data_analysis.py`, `run.py`, and `functions.py`. For the data model containing the summarized records, please refer to the python script `data_model.py` and the *Database Design* section of the `README.md` in the root folder. Comments and docstrings are also provided in the python scripts.

# REST API

The API is created using Flask framework in Python. Please refer to the python scripts `api.py` and `unit_tests.py`. You can also refer to the README section of *Flask API* under *Running the Code*. The Swagger/OpenAPI documentation is configured using `api_swagger.json`. Comments and docstrings are also provided in the python scripts.

