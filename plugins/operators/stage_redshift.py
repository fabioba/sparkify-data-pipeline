"""
This mdoule contains operations on AWS - Redshift

Author: Fabio Barbazza
Date: Nov, 2022
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    This class perform operations on AWS - Redshift.
    The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. 
    The operator creates and runs a SQL COPY statement based on the parameters provided. 
    The operator's parameters should specify where in S3 the file is loaded and what is the target table.
    """
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        """
        Execute
        """
        try:

            self.log.info('connect to aws')
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()
            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

            self.log.info('remove tables')
            redshift.run("DELETE FROM {}".format(self.table))


            self.log.info('copy data from s3 to redshift')
            rendered_key = self.s3_key.format(**context)
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

            self.log.info('s3_path: {}'.format(s3_path))

            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.ignore_headers,
                self.delimiter
            )

            self.log.info('formatted_sql: {}'.format(formatted_sql))
            
            redshift.run(formatted_sql)


        except Exception as err:
            self.log.exception(err)
            raise err





