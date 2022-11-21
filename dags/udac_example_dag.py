
"""
This mdoule contains the DAG responsible for running the ETL of Sparkify

Author: Fabio Barbazza
Date: Nov, 2022
"""
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries
from udac_example_dim_subdag import get_load_dim_subdag
from udac_example_data_quality_subdag import get_data_quality_subdag

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


dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# create table if not exist
create_table = PostgresOperator(
    task_id="create_table",
    dag=dag,
    postgres_conn_id="redshift",
    sql = 'sql_queries/create_tables.sql'
)

# copy data from S3 to redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket = 'udacity-dend',
    #s3_key = 'log_data/{execution.year}/{execution.month}',
    s3_key = 'log_json_path.json',
    s3_json = 'auto',
    #s3_json = '2018-11-01-events.json',
    region= 'us-west-2'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id = "redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region= 'us-west-2'
)

# create fact table from staging tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table = "songplays",
    sql = SqlQueries.songplay_table_insert,
    redshift_conn_id = "redshift"
)

# load dim subdag
dim_task_id = "dim_subdag"
dim_subdag_task = SubDagOperator(
    subdag=get_load_dim_subdag(
        "udac_example_dag",
        dim_task_id,
        start_date= datetime(2019, 1, 12),
    ),
    task_id=dim_task_id,
    dag=dag,
)

# data quality checks subdag
data_quality_task_id = "data_quality_subdag"
data_quality_subdag_task = SubDagOperator(
    subdag=get_data_quality_subdag(
        "udac_example_dag",
        data_quality_task_id,
        start_date= datetime(2019, 1, 12),
    ),
    task_id=data_quality_task_id,
    dag=dag,
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


start_operator >> create_table >> (stage_events_to_redshift, stage_songs_to_redshift) >> load_songplays_table >> dim_subdag_task >> data_quality_subdag_task >> end_operator
