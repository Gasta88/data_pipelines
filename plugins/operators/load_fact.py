from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class LoadFactOperator(BaseOperator):
    """A class to represent the Airflow custom operator to load facts on AWS Redshift."""
    ui_color = '#F98866'
    insert_sql = "INSERT INTO {} ({}) {}"
    
    @apply_defaults
    def __init__(self,
                 conn_id="",    
                 table="",
                 table_columns=[],
                 sql_select="",
                 append_mode=True,
                 *args, **kwargs):
        """Initialize custom operator.
        
        Parameters
        ----------
        conn_id: str
            Connection to Redshift
        table: str
            Destination table
        table_columns: list
            List of column names for the destination table
        sql_select: str
            SELECT SQL statement to capture data from staging table
        append_mode: bool
            Flag to indicate APPEND or DELETE/LOAD mode
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id 
        self.table = table
        self.table_columns = table_columns
        self.sql_select = sql_select
        self.append_mode = append_mode

    def execute(self, context):
        """Execute custom actions for the operator."""
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        if not self.append_mode:
            logging.info("Clearing data from destination fact table")
            redshift.run("TRUNCATE TABLE {}".format(self.table))
        formatted_columns = ", ".join(map(str, self.table_columns))
        formatted_sql = LoadFactOperator.insert_sql.format(
            self.table,
            formatted_columns,
            self.sql_select)
        logging.info(f"Load facts via SQL: {formatted_sql}")
        redshift.run(formatted_sql)
