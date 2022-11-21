"""
This mdoule contains the dimension SUBDAG

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

def get_load_dim_subdag(
        parent_dag_name,
        task_id,
        *args, **kwargs):
    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )


    # load dimension tables
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        table = "users",
        sql = SqlQueries.user_table_insert,
        redshift_conn_id = "redshift"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        table = "songs",
        sql = SqlQueries.song_table_insert,
        redshift_conn_id = "redshift"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        table = "artists",
        sql = SqlQueries.artist_table_insert,
        redshift_conn_id = "redshift"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        table = "time",
        sql = SqlQueries.time_table_insert,
        redshift_conn_id = "redshift"
    )




    (load_song_dimension_table, 
    load_user_dimension_table, 
    load_artist_dimension_table, 
    load_time_dimension_table)

    return dag
