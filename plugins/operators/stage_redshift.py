from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import logging

class StageToRedshiftOperator(BaseOperator):
    """A class to represent the Airflow custom operator from S3 to AWS Redshift."""
    ui_color = '#358140'
    template_fields = ("s3_key",)
    events_copy_sql = """COPY {}
                         FROM '{}'
                         ACCESS_KEY_ID '{}'
                         SECRET_ACCESS_KEY '{}'
                         REGION '{}'
                         JSON '{}'
                    """
    songs_copy_sql = """COPY {}
                        FROM '{}'
                        ACCESS_KEY_ID '{}'
                        SECRET_ACCESS_KEY '{}'
                        REGION '{}'
                        JSON 'auto'
                    """
    
    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 jsonpath="",
                 *args, **kwargs):
        """Initialize custom operator.
        
        Parameters
        ----------
        conn_id: str
            Connection to Redshift
        aws_credential_id: str
            Reference the AWS connection made on the Airflow UI
        table: str
            Destination table
        s3_bucket: str
        s3_key: str
        region: str
            Geographical region where the bucket is allocated
        jsonpath: str
            Parameter to infer JSON schema for nested JSON files
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.conn_id = conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.aws_credentials_id = aws_credentials_id
        self.jsonpath = jsonpath

    def execute(self, context):
        """Execute custom actions for the operator."""
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        logging.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        logging.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        logging.info(f"Staging data to {self.table}")
        if self.table == "staging_events":
            formatted_sql = StageToRedshiftOperator.events_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region,
                self.jsonpath
                )
        else:
            formatted_sql = StageToRedshiftOperator.songs_copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.region
                )
        redshift.run(formatted_sql)


