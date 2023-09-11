from datetime import datetime, timedelta
import pendulum
import os

from airflow.decorators import dag
from airflow.operators.dummy_operator import DummyOperator

from plugins.final_project_operators.stage_redshift import (
    StageToRedshiftOperator
)
from plugins.final_project_operators.load_fact import LoadFactOperator
from plugins.final_project_operators.load_dimension import (
    LoadDimensionOperator
)
from plugins.final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from helper import get_columns

redshift_conn_id = 'redshift'
aws_credentials_id = 'aws_credentials'

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'retries': 3,
    'retry_delay': 300,
    'catchup': False,
    'depends_on_past': False,
    'email_on_retry': False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table="staging_events",
        s3_bucket="yue-li-experiment",
        s3_key="log-data",
        json_format="s3://yue-li-experiment/log_json_path.json",
        init_table_fields=get_columns.get_create_table_def(
            get_columns.stage_events_fields
        )
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id=redshift_conn_id,
        aws_credentials_id=aws_credentials_id,
        table="staging_songs",
        s3_bucket="yue-li-experiment",
        s3_key="song-data",
        json_format="auto",
        init_table_fields=get_columns.get_create_table_def(
            get_columns.stage_songs_fields
        )
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id=redshift_conn_id,
        table="fact_songplay",
        insert_columns=get_columns.get_insertion_columns(
            get_columns.fact_songplay_fields
        ),
        query=final_project_sql_statements.SqlQueries.songplay_table_insert,
        init_table_fields=get_columns.get_create_table_def(
            get_columns.fact_songplay_fields
        )
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="dim_user",
        insert_columns=get_columns.get_insertion_columns(
            get_columns.dim_user_fields
        ),
        query=final_project_sql_statements.SqlQueries.user_table_insert,
        init_table_fields=get_columns.get_create_table_def(
            get_columns.dim_user_fields
        ),
        insertion_mode='append'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="dim_song",
        insert_columns=get_columns.get_insertion_columns(
            get_columns.dim_song_fields
        ),
        query=final_project_sql_statements.SqlQueries.song_table_insert,
        init_table_fields=get_columns.get_create_table_def(
            get_columns.dim_song_fields
        ),
        insertion_mode='append'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="dim_artist",
        insert_columns=get_columns.get_insertion_columns(
            get_columns.dim_artist_fields
        ),
        query=final_project_sql_statements.SqlQueries.artist_table_insert,
        init_table_fields=get_columns.get_create_table_def(
            get_columns.dim_artist_fields
        ),
        insertion_mode='append'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id=redshift_conn_id,
        table="dim_time",
        insert_columns=get_columns.get_insertion_columns(
            get_columns.dim_time_fields
        ),
        query=final_project_sql_statements.SqlQueries.time_table_insert,
        init_table_fields=get_columns.get_create_table_def(
            get_columns.dim_time_fields
        ),
        insertion_mode='append'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id=redshift_conn_id,
        tables={
            'fact_songplay': 'songplay_id',
            'dim_user': 'user_id',
            'dim_song': 'song_id',
            'dim_artist': 'artist_id',
            'dim_time': 'start_time'
        }
    )

    end_operator = DummyOperator(task_id='End_execution')

    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

final_project_dag = final_project()
