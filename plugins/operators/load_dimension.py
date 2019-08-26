from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Operator to load data from fact to dimension tables.
    Note: This assumes the target tables are already created
    
    Input Parameters:
        redshift_conn_id:   The target Redshift connection id
        aws_credentials_id: AWS credentials
        table_name:         Target table name
        table_insert:       Table insert sql query
    """
    ui_color = '#80BD9E'
    insert_sql = 'INSERT INTO {} {}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_name="",
                 aws_credentials_id="",
                 table_insert="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table_name=table_name
        self.aws_credentials_id=aws_credentials_id
        self.table_insert=table_insert

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials_id)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Load data into dimension table {self.table_name}')
        formated_insert_sql=redshift.insert_sql.format(self.table_name, self.table_insert)
        
        redshift.run(formated_insert_sql)
