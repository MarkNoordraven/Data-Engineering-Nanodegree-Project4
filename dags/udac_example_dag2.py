from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadFactOperator,
                               PostgresOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators import StageToRedshiftOperator
from helpers import SqlQueries


default_args = {'owner': 'udacity',
                'start_date': datetime(2019, 1, 12),
                'catchup':False,
                'depends_on_past':False,
                'retries':0,
                'retry_delay': timedelta(minutes=1)}

dag2 = DAG('create_table_dag',
          default_args=default_args,
          description='Creates tables to be loaded with another dag',
#           schedule_interval='0 * * * *'
          schedule_interval = None)

start_table_operator = DummyOperator(task_id='Begin_execution',  dag=dag2)

drop_artist_table = PostgresOperator(
    task_id="drop_artist_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""DROP TABLE IF EXISTS public.artists
        """)

drop_songplays_table = PostgresOperator(
    task_id="drop_songplays_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""DROP TABLE IF EXISTS public.songplays
        """)

drop_songs_table = PostgresOperator(
    task_id="drop_songs_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""DROP TABLE IF EXISTS public.songs
        """)

drop_staging_events_table = PostgresOperator(
    task_id="drop_staging_events_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""DROP TABLE IF EXISTS public.staging_events
        """)

drop_staging_songs_table = PostgresOperator(
    task_id="drop_staging_songs_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""DROP TABLE IF EXISTS public.staging_songs
        """)

drop_users_table = PostgresOperator(
    task_id="drop_users_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""DROP TABLE IF EXISTS public.users
        """)

create_artist_table = PostgresOperator(
    task_id="create_artist_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""CREATE TABLE public.artists (
           artistid varchar(256) NOT NULL,
           name varchar(256),
           location varchar(256),
           lattitude numeric(18,0),
           longitude numeric(18,0));
        """)

create_songplays_table = PostgresOperator(
    task_id="create_songplays_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""CREATE TABLE public.songplays (
           playid varchar(32) NOT NULL,
           start_time timestamp NOT NULL,
           userid int4 NOT NULL,
           "level" varchar(256),
           songid varchar(256),
           artistid varchar(256),
           sessionid int4,
           location varchar(256),
           user_agent varchar(256),
           CONSTRAINT songplays_pkey PRIMARY KEY (playid));
        """)

create_songs_table = PostgresOperator(
    task_id="create_songs_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""CREATE TABLE public.songs (
           songid varchar(256) NOT NULL,
           title varchar(256),
           artistid varchar(256),
           "year" int4,
           duration numeric(18,0),
           CONSTRAINT songs_pkey PRIMARY KEY (songid));
        """)

create_staging_events_table = PostgresOperator(
    task_id="create_staging_events_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""CREATE TABLE public.staging_events (
           artist varchar(256),
           auth varchar(256),
           firstname varchar(256),
           gender varchar(256),
           iteminsession varchar(256),
           lastname varchar(256),
           length numeric,
           "level" varchar(256),
           location varchar(256),
           "method" varchar(256),
           page varchar(256),
           registration numeric(18,0),
           sessionid int,
           song varchar(256),
           status int4,
           ts int8,
           useragent varchar(256),
           userid int4);
        """)

create_staging_songs_table = PostgresOperator(
    task_id="create_staging_songs_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""CREATE TABLE public.staging_songs (
           num_songs int4,
           artist_id varchar(256),
           artist_name varchar(256),
           artist_latitude numeric(18,0),
           artist_longitude numeric(18,0),
           artist_location varchar(256),
           song_id varchar(256),
           title varchar(256),
           duration numeric(18,0),
           "year" int4);
        """)

create_users_table = PostgresOperator(
    task_id="create_users_table",
    dag=dag2,
    postgres_conn_id="redshift",
    sql="""CREATE TABLE public.users (
           userid int4 NOT NULL,
           first_name varchar(256),
           last_name varchar(256),
           gender varchar(256),
           "level" varchar(256),
           CONSTRAINT users_pkey PRIMARY KEY (userid));
        """)

end_table_operator = DummyOperator(task_id='Stop_execution',  dag=dag2)

start_table_operator >> drop_artist_table
start_table_operator >> drop_songplays_table
start_table_operator >> drop_songs_table
start_table_operator >> drop_staging_events_table
start_table_operator >> drop_staging_songs_table
start_table_operator >> drop_users_table

drop_artist_table >> create_artist_table
drop_songplays_table >> create_songplays_table
drop_songs_table >> create_songs_table
drop_staging_events_table >> create_staging_events_table
drop_staging_songs_table >> create_staging_songs_table
drop_users_table >> create_users_table

create_artist_table >> end_table_operator
create_songplays_table >> end_table_operator
create_songs_table >> end_table_operator
create_users_table >> end_table_operator
create_staging_events_table >> end_table_operator
create_staging_songs_table >> end_table_operator

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
          schedule_interval = None)

start_etl_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    extra_params="format as json 's3://udacity-dend/log_json_path.json'",
    dag=dag)

stage_songs_to_redshift = StageToRedshiftOperator(
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data",
    task_id='Stage_songs',
    extra_params = "json 'auto'",
    dag=dag)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    sql_source=SqlQueries.songplay_table_insert,
    dag=dag)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    redshift_conn_id = "redshift",
    table = 'users',
    sql_source = SqlQueries.user_table_insert,
    dag = dag)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = "redshift",
    table = 'songs',
    sql_source = SqlQueries.song_table_insert,
    dag = dag)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = "redshift",
    table = 'artists',
    sql_source = SqlQueries.artist_table_insert,
    dag = dag)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = "redshift",
    table = 'time',
    sql_source = SqlQueries.time_table_insert,
    dag = dag)

run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id="redshift",
    table="time",
    dq_checks=[{'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result': 0},
               {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result': 0}],
    dag=dag)

end_etl_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_etl_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> [load_user_dimension_table,
                         load_song_dimension_table,
                         load_artist_dimension_table,
                         load_time_dimension_table]

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_etl_operator
