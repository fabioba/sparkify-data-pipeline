
"""
This mdoule contains the DAG responsible for running the ETL of Sparkify

Author: Fabio Barbazza
Date: Nov, 2022
"""
from datetime import datetime, timedelta
from airflow.decorators import task_group
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.task_group import TaskGroup


from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries

import logging 
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

logger = logging.getLogger(__name__)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_failure': True,
    # retry 3 times after failure
    'retries': 3,
    'email_on_retry': False,
    # retry after 5 minutes
    'retry_delay': timedelta(minutes=5)
}

with DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        ) as dag:

    start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

    # copy data from S3 to redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        dag=dag,
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket = 'udacity-dend',
        #s3_key = 'log_data/{execution.year}/{execution.month}',
        s3_key = 'log_json_path.json',
        #s3_json = 'auto',
        s3_json = 's3://udacity-dend/log_json_path.json',
        region= 'us-west-2'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        dag=dag,
        redshift_conn_id = "redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        s3_json = 'auto',
        region= 'us-west-2'
    )

    # create fact table from staging tables
    load_songplays_table = LoadFactOperator(
        task_id='load_songplays_fact_table',
        dag=dag,
        table = "songplays",
        sql = SqlQueries.songplay_table_insert,
        redshift_conn_id = "redshift"
    )


    with TaskGroup(group_id='load_dimension_tables') as load_dimension_tables:
        # load dimension tables
        load_user_dimension_table = LoadDimensionOperator(
            task_id='load_user_dim_table',
            table = "users",
            sql = SqlQueries.user_table_insert,
            redshift_conn_id = "redshift"
        )

        load_song_dimension_table = LoadDimensionOperator(
            task_id='load_song_dim_table',
            table = "songs",
            sql = SqlQueries.song_table_insert,
            redshift_conn_id = "redshift"
        )

        load_artist_dimension_table = LoadDimensionOperator(
            task_id='load_artist_dim_table',
            table = "artists",
            sql = SqlQueries.artist_table_insert,
            redshift_conn_id = "redshift"
        )

        load_time_dimension_table = LoadDimensionOperator(
            task_id='load_time_dim_table',
            table = "time",
            sql = SqlQueries.time_table_insert,
            redshift_conn_id = "redshift"
        )



    with TaskGroup(group_id='data_quality_check') as data_quality_check:
        # data quality checks
        artists_data_quality_checks = DataQualityOperator(
            task_id='artists_data_quality_checks',
            table = "artists",
            col_name = "artist_id",
            redshift_conn_id = "redshift"
        )

        songplays_data_quality_checks = DataQualityOperator(
            task_id='songplays_data_quality_checks',
            table = "songplays",
            col_name = "playid",
            redshift_conn_id = "redshift"
        )

        songs_data_quality_checks = DataQualityOperator(
            task_id='songs_data_quality_checks',
            table = "songs",
            col_name = "songid",
            redshift_conn_id = "redshift"
        )


        time_data_quality_checks = DataQualityOperator(
            task_id='time_data_quality_checks',
            table = "time",
            col_name = "start_time",
            redshift_conn_id = "redshift"
        )


        users_data_quality_checks = DataQualityOperator(
            task_id='users_data_quality_checks',
            table = "users",
            col_name = "userid",
            redshift_conn_id = "redshift")



    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table >> load_dimension_tables >> data_quality_check >> end_operator
