# Data Pipeline with Airflow
Talk about the project and objective.

## Process
asdasdasdasd
asd
asdas
d
Here is visually:
![Data Pipeline with Airflow - By Israel Mendes.](./images/tables.png "Data Pipeline with Airflow - By Israel Mendes.")


## Files and Setup
asdasdasdasd
asd
- `create_tables_dag.py:` DAG to create the database schema of the application.
- `migrate_data_dag.py:` DAG that performs the ETL process of the application.
- `stage_redshift.py:` Operator to move S3 data to Redshift.
- `load_fact.py:` Operator to move data from staging tables to fact tables.
- `load_dimension.py:` Operator to move data to dimension tables.
- `data_quality.py:` Operator to verify all tables have data.
- `docker-compose.yml`: Docker-compose file to easily start Apache Airflow locally.

## Setup
asdasdasd
> pip3 install apache-airflow==1.10.2
> cd project_workspace # where your dags & plugins folder located
> EXPORT AIRFLOW_HOME=`pwd`
> airflow initdb
> airflow webserver
