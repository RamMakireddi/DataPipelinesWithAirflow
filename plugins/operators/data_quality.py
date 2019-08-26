from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Operator to test data quality of all the target dimension and fact tables.
    Note: This assumes the target tables are already created
    
    Input Parameters:
        redshift_conn_id:   The target Redshift connection id
        aws_credentials_id: AWS credentials
    """
    ui_color = '#89DA59'
    records="""
        SELECT COUNT(*) FROM {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        dim_table_list=['songs','users', 'time', 'artists', 'songplays']
        
        for dim_table in dim_table_list:
            self.log.info('DataQuality - No records check for {}'.format(dim_table))
            records=redshift.get_records("SELECT COUNT(*) FROM {}".format(dim_table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError("Data quality check failed. {} returned no results".format(dim_table))
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError("Data quality check failed. {} contained 0 rows".format(dim_table))
            logging.info("Data quality on table {dim_table} check passed with {records[0][0]} records")
        
        self.log.info('DataQuality - null values check for songs duration')
        dim_table='songs'
        dim_table_field='duration'
        null_records=redshift.get_records("SELECT COUNT(*) FROM {} WHERE {} IS NULL".format(dim_table, dim_table_field))
        num_records = null_records[0][0]
        if num_records > 0:
            raise ValueError("Data quality check failed. {}.{} contained null values".format(dim_table, dim_table_field))
        logging.info("Data quality on table {dim_table} check passed with no null records in the field {dim_table_field}")
        
        
        