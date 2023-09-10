from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    table_insertion_sql = """
    INSERT INTO {} (
        {}
    )
    {}
    """

    create_sql = """
        CREATE TABLE IF NOT EXISTS {}
        (
            {}
        )
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_columns="",
                 query="",
                 init_table_fields="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_columns = insert_columns
        self.query = query
        self.init_table_fields = init_table_fields

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating {self.table} if table does not exist.")
        redshift.run(
            LoadFactOperator.create_sql.format(
                self.table,
                self.init_table_fields
            )
        )

        self.log.info(f"Inserting fact data for {self.table}")
        formatted_sql = LoadFactOperator.table_insertion_sql.format(
            self.table,
            self.insert_columns,
            self.query
        )
        redshift.run(formatted_sql)


