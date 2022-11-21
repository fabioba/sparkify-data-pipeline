"""
This mdoule contains the data quality SUBDAG

Author: Fabio Barbazza
Date: Nov, 2022
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

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

def get_data_quality_subdag(
        parent_dag_name,
        task_id,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )


    # data quality checks
    run_quality_checks_artist = DataQualityOperator(
        task_id='Run_data_quality_checks_artists',
        dag=dag,
        table = "artists",
        redshift_conn_id = "redshift"
    )

    run_quality_checks_songplays = DataQualityOperator(
        task_id='Run_data_quality_checks_songplays',
        dag=dag,
        table = "songplays",
        redshift_conn_id = "redshift"
    )

    run_quality_checks_songs = DataQualityOperator(
        task_id='Run_data_quality_checks_songs',
        dag=dag,
        table = "songs",
        redshift_conn_id = "redshift"
    )


    run_quality_checks_time = DataQualityOperator(
        task_id='Run_data_quality_checks_time',
        dag=dag,
        table = "time",
        redshift_conn_id = "redshift"
    )


    run_quality_checks_users = DataQualityOperator(
        task_id='Run_data_quality_checks_users',
        dag=dag,
        table = "users",
        redshift_conn_id = "redshift")


    (run_quality_checks_artist, run_quality_checks_users, run_quality_checks_songplays, 
    run_quality_checks_songs, run_quality_checks_time)

    return dag
