from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'end_date': datetime(2019, 1, 22),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'catchup_by_default': False
}

# Data quality tests
qc_checks=[
        {'sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL",
         'result': 0},
        {'sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL",
         'result': 0},
        {'sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL",
         'result': 0},
        {'sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL",
         'result': 0},
        {'sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL",
         'result': 0}
    ]

# DAG definition
dag = DAG('udac_example_dag_h',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          template_searchpath = ['/home/workspace/airflow'],
          max_active_runs=1
        )

# Task definitions
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables_task = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    sql=['create_tables.sql'],
    postgres_conn_id="redshift"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region = 'us-west-2',
    jsonpath="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A",
    region = 'us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift",    
    table="songplays",
    table_columns=["playid", "start_time", "userid", "level", "songid",
                   "artistid", "sessionid", "location", "user_agent"],
    sql_select=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    conn_id="redshift",    
    table="users",
    table_columns=["userid", "first_name", "last_name", "gender", "level"],
    sql_select=SqlQueries.user_table_insert,
    append_mode=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    conn_id="redshift",    
    table="songs",
    table_columns=["songid", "title", "artistid", "year", "duration"],
    sql_select=SqlQueries.song_table_insert,
    append_mode=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    conn_id="redshift",    
    table="artists",
    table_columns=["artistid", "name", "location", "lattitude", "longitude"],
    sql_select=SqlQueries.artist_table_insert,
    append_mode=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    conn_id="redshift",    
    table="time",
    table_columns=["start_time", "hour", "day", "week", "month", "year",
                   "weekday"],
    sql_select=SqlQueries.time_table_insert,
    append_mode=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    conn_id = "redshift",
    qc_checks = qc_checks    
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Setting task dependecies
start_operator >> create_tables_task

create_tables_task >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_song_dimension_table, load_user_dimension_table,
                         load_artist_dimension_table, load_time_dimension_table]

[load_song_dimension_table, load_user_dimension_table,
 load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator