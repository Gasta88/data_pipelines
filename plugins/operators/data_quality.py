from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import sys
import logging

class DataQualityOperator(BaseOperator):
    """A class to represent the Airflow custom operator to perform QC on data."""
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 qc_checks=[],
                 *args, **kwargs):
        """Initialize custom operator.
        
        Parameters
        ----------
        conn_id: str
            Connection to Redshift
        qc_checks: dict
            Archive of QC checks and respective expected results
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.qc_checks = qc_checks

    def execute(self, context):
        """Execute custom actions for the operator."""
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        for qc_test in self.qc_checks:
            sql_stmnt = qc_test.get('sql', None)
            exp_res = qc_test.get('result', None)
            if sql_stmnt is None or exp_res is None:
                raise ValueError("Missing SQL or expected result from QC test.")
                logging.info("Missing SQL or expected result from QC test.")
                sys.exit()
            exp_res = int(exp_res)
            records = redshift.get_records(sql_stmnt)
            if int(records[0][0]) != exp_res:
                logging.info(f"Data quality check failed. SQL is : {sql_stmnt}")
                raise ValueError("Data quality check failed.")
            else:
                logging.info(f"Data quality check passed. SQL is : {sql_stmnt}")
                