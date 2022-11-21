"""
This mdoule contains load fact operations

Author: Fabio Barbazza
Date: Nov, 2022
"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    fact_query = """
        DROP TABLE IF EXISTS {table};
        CREATE TABLE {table} AS 
        {sql}
    """

    @apply_defaults
    def __init__(self,
                sql = "",
                table = "",
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """
        Execute
        """
        try:

            redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

            self.log.info('self.fact_query: {} running'.format(self.fact_query))

            redshift.run(self.fact_query)

            self.log.info('self.fact_query: success')


        except Exception as err:
            self.log.exception(err)
            raise err