from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'israelmendes',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
}

dag = DAG(
    'udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow.',
    schedule_interval='@hourly'
)

start_operator = DummyOperator(
    task_id='Begin_execution',  
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag
    aws_credentials='aws_credentials',
    redshift='redshift',
    s3_key='log_data/',
    s3_bucket='dend',
    table='staging_events'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag
    aws_credentials='aws_credentials',
    redshift='redshift',
    s3_key='song_data/',
    s3_bucket='dend',
    table='staging_songs',
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag
    redshift='redshift',
    table='songplays',
    sql_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag
    provide_context=True,
    table='users',
    aws_credentials='aws_credentials',
    redshift='redshift',
    sql_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag
    provide_context=True,
    table='songs',
    aws_credentials='aws_credentials',
    redshift='redshift',
    sql_query=SqlQueries.user_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag
    provide_context=True,
    table='artists',
    aws_credentials='aws_credentials',
    redshift='redshift',
    sql_query=SqlQueries.user_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag
    provide_context=True,
    table='time',
    aws_credentials='aws_credentials',
    redshift='redshift',
    sql_query=SqlQueries.user_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag
    provide_context=True,
    tables=["songplays", "users", "songs", "artirst", "time"],
    aws_credentials="aws_credentials",
    redshift='redshift'
)

end_operator = DummyOperator(
    task_id='Stop_execution',  
    dag=dag
)

start_operator >> [
    stage_events_to_redshift, 
    stage_songs_to_redshift
    ] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table, 
    load_song_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table
    ] >> run_quality_checks
run_quality_checks >> end_operator
