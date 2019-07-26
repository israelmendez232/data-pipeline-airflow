from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """



    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = target_table
        self.log.info(' =====> Main configurations in DataQualityOperator')

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        for table in self.tables:
            records = redshift.get_records(f"SELECT COUNT(1) FROM {table}")
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f" =====> Problems with the Data Quality Check! {table} returned with no results.")
            num_records = records[0][0]
            
            if num_records < 1:
                raise ValueError(f" =====> Problems with the Data Quality Check! {table} contained with 0 rows.")
                
            self.log.info(f" =====> Data quality on table {table} check passed with {records[0][0]} records")

        self.log.info(" =====> Done with the Data_Quality Process!")
        