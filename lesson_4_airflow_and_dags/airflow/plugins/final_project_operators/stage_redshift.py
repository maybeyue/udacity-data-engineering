from airflow.secrets.metastore import MetastoreBackend
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
    """

    delete_sql = """
        DROP TABLE IF EXISTS {}
    """

    create_sql = """
        CREATE TABLE {}
        (
            {}
        )
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_format="",
                 init_table_fields="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format
        self.init_table_fields = init_table_fields

    def execute(self, context):
        metastoreBackend = MetastoreBackend()
        aws_connection = metastoreBackend.get_connection(
            self.aws_credentials_id
        )
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Dropping table if exists. Creating new table.")
        redshift.run(
            StageToRedshiftOperator.delete_sql.format(self.table)
        )
        redshift.run(StageToRedshiftOperator.create_sql.format(
            self.table, self.init_table_fields)
        )

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            aws_connection.login,
            aws_connection.password,
            self.json_format
        )
        redshift.run(formatted_sql)




