from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    '''



    '''


    self.log.info(' =====> Connecting with StageToRedshiftOperator')
    ui_color = '#358140'
    template_fields = ('s3_key')
    
    # Collect the SQL Template CSV:
    sql_template_csv = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    '''
    
    # Collect the SQL Template CSV:
    sql_template_json = '''
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        format as json {}
    '''

    @apply_defaults
    def __init__(
        self,
        aws_credentials_id='',
        redshift_conn_id='',
        s3_key='',
        s3_bucket='',
        table='',
        ignore_header=1,
        delimiter=',',
        file_extension='csv',
        *args, 
        **kwargs
    ):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.table = table
        self.ignore_header = ignore_header
        self.delimiter = delimiter
        self.file_extension = file_extension

    def execute(self, context):
        self.log.info(' =====> Done with the configuration with StageToRedshiftOperator.')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(' =====> Connecting with AWS.')

        rendered_key = self.s3_key.format(**context)
        s3_path = 's3://{}/{}'.format(self.s3_bucket,  rendered_key)
        self.log.info(' =====> Connecting with S3 bucket.')

        redshift.run('TRUNCATE {}'.format(self.table))
        self.log.info(' =====> Getting data from S3 to Redshift.')

        # Formatting the data if it's from CSV or JSON:
        if self.file_extension == 'csv':
            formatted_sql = StageToRedshiftOperator.copy_cmd.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key) + StageToRedshiftOperator.csv_source.format(
                self.ignore_header,
                self.delimiter) + self.file_extension
        else:
            formatted_sql = StageToRedshiftOperator.copy_cmd.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key) + StageToRedshiftOperator.json_source

        self.log.info(' =====> Running the SQL on the data.')
        redshift.run(formatted_sql)
        self.log.info(' =====> Done!')
