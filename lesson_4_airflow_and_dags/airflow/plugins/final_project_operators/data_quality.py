from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    check_null_sql = """
        SELECT COUNT(*)
        FROM {}
        WHERE {} IS NULL
    """
    check_data_exists_sql = """
        SELECT COUNT(*)
        FROM {}
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for table, pkid in self.tables.items():
            check_data_exists = DataQualityOperator.check_data_exists_sql.format(
                table
            )
            records = redshift_hook.get_records(check_data_exists)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results"
                )
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(
                    f"Data quality check failed. {table} contained 0 rows"
                )
            self.log.info(
                f"Data quality on table {table} check passed with"
                f" {records[0][0]} records"
            )

            check_nulls_in_table = DataQualityOperator.check_null_sql.format(
                table, pkid
            )
            records = redshift_hook.get_records(check_nulls_in_table)
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    f"Data quality check failed. {table} returned no results "
                    "when checking for nulls"
                )
            num_nulls = records[0][0]
            if num_nulls != 0:
                raise ValueError(
                    f"Data quality check failed. {table} contained "
                    f"{num_records} of null rows"
                )
            self.log.info(
                f"Data quality on table {table} "
                f"check passed with {records[0][0]} records of null rows"
            )
