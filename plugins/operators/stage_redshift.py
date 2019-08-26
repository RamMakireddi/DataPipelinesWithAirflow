from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Operator to copy from given S3 bucket to Redshift database.
    Note: This assumes the target tables are already created
    
    Input Parameters:
        redshift_conn_id:   The target Redshift connection id
        aws_credentials_id: AWS credentials
        s3_bucket:          S3 bucket name
        table_name:         Target table name
        s3_key:             S3 key
    """

    ui_color = '#358140'
    copy_sql="""
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
                 s3_bucket="",
                 table_name="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.s3_bucket=s3_bucket
        self.table_name=table_name
        self.s3_key=s3_key
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Copying data from S3')
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        self.log.info('S3 path: ', s3_path)
        formated_copy_sql=StageToRedshiftOperator.copy_sql.format(
            self.table_name,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        
        redshift.run(formated_copy_sql)