# TrueLayer Assignment

### Prerequisites

In order to run this pipeline, you'll need the following:

- [docker](https://docs.docker.com/get-docker/)
- [docker-compose](https://docs.docker.com/compose/install/)

They both have easy installers and on most platforms the installation for the desktop client for `docker` includes `docker-compose`.


### Infrastructure

I have chosen Python as my programming language, Pandas as the main library for data transformation and Apache Airflow as the orchestrator.
By using DAGs I have ensured that the process is reproducible and automated.

Airflow is the choice for my ETL because it is easy to setup, comes with out of the box Connectors for most of the data sources, customizable as it is open-source and written in Python, and is easy to monitor. I have used Puckel's image for Airflow. 
Postgres can also be used as the backend DB for Airflow.

Do create a postgres.env file in the root directory with the values:
POSTGRES_PASSWORD=airflow
POSTGRES_USER=user
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=database

### DAG

I have created individual tasks for working with the given input files. 
Task1 is for Data Extraction where I've extracted the required fields from the gz file.
Task2 is for Data Transformation, joining the data and uploading it to Postgres DB.

Due to short of time, I have hardcoded few of the values in the callable functions, instead I could have passed them dynamically in the Dag.

## Testing
When doing data transformatin in pandas dataframe I checked for data consistencies each time I implement a join, filter, drop etc.
Dataframe provides us with a tabular representatioin of data which helps in finding any anomalies.

## Performance
Initially I found performance issues while parsing XML file but later figured out the parsing method for compressed XML file(gz file), and I managed to make it efficient by using Element Tree and filtered only required fields.

