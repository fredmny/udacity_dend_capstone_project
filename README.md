# [WIP] Capstone Project
This is the capstone project of the Data Engineering Nanodegree from Udacity. 

In this project we build the ETL pipeline of an analytical data model of flights delays and cancellations. As an example, this data could be used by companies or even users to identify patterns in these delays or cancellations and use this information to make better decisions when planning trips.

Since it's a data model for analytical (and not transactional) purposes, a daily update (daily run of the workflow) should be necessary.
## Data model
For the data model we have chosen the star-schema, making it easy to aggregate data on the `fct_flights` table and at the same time easy to join with dimension tables for filtering and specifying aggregation parameters. This is justified further, considering that this model is to be used for analytical purposes.

![flights data model](./images/data_model.png)

The model is composed of:
- `fct_flights` - fact table with each line being a flight as the granularity
- `dim_date` - dimension table opening the data info in year, month, day, quarter and semester to facilitate the clusterization of flights data in thes timespans
- `dim_airport` - dimension table with more information about airports
- `dim_airlines` - dimension table with more data about airlines

A data dictionary can be found [here](./DATA-DICTIONARY.md)
## Data pipeline
The steps performed for each transformation are:
1. Read data in `.csv` or `.json` format from a s3 bucket
2. Transform the data using spark functions and methods
3. Write the data to another s3 bucket in `.parquet` format

For development and POC we are running the etl process locally, but in production we would run it on a cloud provider, for example, submitting it to an EMR cluster on AWS.
## Project steps
The major steps taken in the project were:
1. Find a data for the project with following prerequisites:
    - at least 2 different sources
    - at least 2 different formats
    - at leas 1M rows
2. Explore the data
    - More info in first part of `data_exploration.ipynb` 
3. Develop the data model (fact and dimension tables)
    - More info second part of `data_explorations.ipynb`
4. Develop the ETL
## Data sources
- Flights:
    - [source](https://www.kaggle.com/datasets/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018)
    - file format: `csv`
    - lines: 63M
- Airlines:
    - [source](https://www.kaggle.com/datasets/open-flights/airline-database)
    - file format: `csv`
    - lines: 6k 
- Airports:
    - [source](https://datahub.io/core/airport-codes)
    - file format: `json`
    - lines: 57k
## Tools
Spark is being used to transform the data, writing it to `parquet` files, since it's one of the industry standards for big data. This means that it's easily scalable (e.g. using cloud solutions) and there are a lot of tools that use it (e.g. Databricks), which make it easier to create a more robust system to deploy the ETL.

We save the data to s3, since we save data in files (`parquet`) and are using AWS, the most used cloud provider. 
## How to run the code
1. Create IAM user with:
    1. Programatig access
    2. Attached Policies:
        - AdministratorAccess
        - AmazonS3FullAccess
2. Create two s3 buckets:
    - One for the source data
    - One for the transformed tables that will be in the `.parquet` format
<!-- 3. Run `docker compose up -d` and log in to Airflow on `localhost:8080`
    - user: `admin`
    - password: `admin`
4. Add connections with above created credentials and cluster inf to Airflow:
    - Amazon Web Services: name `aws_credentials`
    - Postgres: name `redshift`
5. Enable the dag -->
## Main files
The project consists of following files:
- `docker-compose.yml` - Creates the docker containers, volumes and network
- `plugins/operators/*.py` - Custom operators used by the dag
- `plugins/helpers/sql_queries.py` - SQL queries to transform the data. Used within the dag.
- `dags/sparkify_etl.py` - Airflow dag
- `pyproject.yml` and `poetry.lock` - Files for the virtual environment (used for the development)
## References
- The `docker-compose.yml` is based on Bitnami's instructions and file, which can be found [here](https://github.com/bitnami/bitnami-docker-airflow)
- For delay and cancellation codes: [BTS - Technical Directive: On-Time Reporting](https://www.bts.gov/topics/airlines-and-airports/number-23-technical-directive-time-reporting-effective-jan-1-2014)
- Specifications about the delay categories: [BTS - Understanding the Reporting of Causes of Flight Delays and Cancellations](https://www.bts.gov/topics/airlines-and-airports/understanding-reporting-causes-flight-delays-and-cancellations)
- Glossary for further terms: [BTS - Glossary](https://www.transtats.bts.gov/Glossary.asp)

## Dependencies
- Java (for running Spark)