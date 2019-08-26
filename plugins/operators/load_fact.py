from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Operator to load data from source redshift tables to fact table.
    Note: This assumes the target tables are already created
    
    Input Parameters:
        redshift_conn_id:   The target Redshift connection id
        aws_credentials_id: AWS credentials
        table_name:         Target table name
        table_insert:       Table insert sql query
    """
    ui_color = '#F98866'
    insert_sql="""
        INSERT INTO {} {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table_name="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table_insert="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table_name=table_name
        self.table_insert=table_insert

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Load data into fact table')
        formated_insert_sql=LoadFactOperator.insert_sql.formate(self.table_name, self.table_insert)
        
        redshift.run(formated_insert_sql)
