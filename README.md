# [WIP] Capstone Project
This is the capstone project of the Data Engineering Nanodegree from Udacity. 

In this project we build the ETL pipeline of an analytical data model of flights delays and cancellations. As an example, this data could be used by companies or even users to identify patterns in these delays or cancellations and use this information to make better decisions when planning trips.

Since it's a data model for analytical (and not transactional) purposes, a daily update (daily run of the workflow) should be necessary.

## Data model
For the data model we have chosen the star-schema, since it's easy to query for making analyses.
![flights data model](./images/data_model.png)

The model is composed of:
- `fct_flights` - fact table with each line being a flight as the granularity
- `dim_date` - dimension table opening the data info in year, month, day, quarter and semester to facilitate the clusterization of flights data in thes timespans
- `dim_airport` - dimension table with more information about airports
- `dim_airlines` - dimension table with more data about airlines
## Project steps
The major steps taken in the project were:
1. Find a data for the project with following prerequisites:
    - at least 2 different sources
    - at least 2 different formats
    - at leas 1M rows
2. Explore the data
    - Done in first part of `data_exploration.ipynb` 
3. Develop the data model (fact and dimension tables)
    - Done in second part of `data_explorations.ipynb`
4. Develop the ETL

The data is originated from files within:
[TODO]

To do so, in this project, we use airflow to orchestrate our pipeline to transform and check the data. The transformed data is saved in following tables:
- `fct_` (fact table) -
- `dim_` (dimension table) - 

The final tables are in the star schema, making it easy to aggregate data on the songplays fact table and at the same time easy to join with dimension tables for filtering and specify aggregation parameters.

The airflow dag executes following steps:
[TODO]

## How to run the code
1. Create IAM user with:
    1. Programatig access
    2. Attached Policies:
        - AdministratorAccess
        - AmazonRedshiftFullAccess
        - AmazonS3FullAccess
2. Create Redshift Cluster and:
    1. Make it publicly accessible (Actions > Modify publicly accessile setting)
    2. Enable `Enhanced VPC Routing`
3. Run `docker compose up -d` and log in to Airflow on `localhost:8080`
    - user: `admin`
    - password: `admin`
4. Add connections with above created credentials and cluster inf to Airflow:
    - Amazon Web Services: name `aws_credentials`
    - Postgres: name `redshift`
5. Enable the dag
## Main files
The project consists of following files:
- `docker-compose.yml` - Creates the docker containers, volumes and network
- `plugins/operators/*.py` - Custom operators used by the dag
- `plugins/helpers/sql_queries.py` - SQL queries to transform the data. Used within the dag.
- `dags/sparkify_etl.py` - Airflow dag
- `pyproject.yml` and `poetry.lock` - Files for the virtual environment (used for the development)
## References
The project is based on the initial project files and used the guidance provided by the Udacity Data Engineering Nanodegree.

The `docker-compose.yml` is based on Bitnami's instructions and file, which can be found [here](https://github.com/bitnami/bitnami-docker-airflow)


## Dependencies
- Java (for running Spark)