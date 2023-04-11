# AWS Deployment:

There could be multiple ways to deploy our project on to AWS.

1. We could migrate to AWS by containerizing the whole application as a Docker container image and pushing it to `Amazon ECR` registry. We can then use an `Amazon EC2` instance to orchestrate and manage our Docker container. 

2. We could use an `Amazon S3` bucket to store all the .txt files and process and ingest them into an `Amazon RDS` database. To process, ingest and sumarize the data we could migrate our python code into `AWS Lambda` functions and use `AWS Glue` to schedule and orchestrate our data pipeline. On the API side of things, we could use `Amazon API Gateway` to create and publish our API. Alternatively, we could just use an `Amazon EC2` instance to deploy our existing Flask code as well. `Amazon Lightsail` is also an alternative to deploy light-weight containerized Flask app.

3. For data processing/ingestion, alternatively to using AWS Glue, we could also setup `Apache Airflow` on an `Amazon EC2` instance and use it to orchestrate and monitor our pipeline consisting of multple `AWS Lambda` functions. 
