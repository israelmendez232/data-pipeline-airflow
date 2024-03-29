from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    '''



    '''
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift = '',
                 table = '',
                 sql_query = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift = redshift,
        self.table = table,
        self.sql_query = sql_query,
        self.aws_credenitals_id = aws_credentials

    def execute(self, context):
        self.log.info(' =====> Main configurations in DataQualityOperator. Starting loading the tables in Redshift')
        self.log.info(' =====> Starting loading the tables in Redshift')
        redshift_hook = PostgresHook(self.redshift)
        insert_sql = '''
            INSERT INTO {}
            {}
        '''.format(self.table, self.sql_query)
        
        self.log.info(' =====> Done with the loading in LoadFactOperator!')
