# Data Pipeline with Airflow
##### [Israel Mendes](israelmendes.com.br)
This is a small project to work with Airflow, using Python to Extract-Transform-Load from a Data Lake in S3 into Redshift. The source uses CSV logs about user activity and JSON with metadata from the songs information.

## Process
The main steps of the workflow are simple:
1. Pull data from S3 (JSON or CSV);
2. DAG Airflow from ECS: importing, data quality checks and transforming;
3. Loading data on Redshift.

Here is visually the **DAG** with more details: <br />
![Data Pipeline with Airflow - By Israel Mendes.](https://raw.githubusercontent.com/israelmendez232/data-pipeline-airflow/master/images/dags.png "Data Pipeline with Airflow - By Israel Mendes.")
[]

## Files and Setup
Here are the main files and scripts used in this process:
- `create_tables_dag.py:` Create the database schema in Redshift;
- `migrate_data_dag.py:` Extract the data from S3;
- `data_quality.py:` Verify all tables have data;
- `load_fact.py:` Move and transform data into the fact table;
- `load_dimension.py:` Move and transform data into the dimension tables;
- `stage_redshift.py:` Connect and load the data into Redshift.
