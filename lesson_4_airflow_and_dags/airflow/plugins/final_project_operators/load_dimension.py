from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    insert_sql = """
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

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_columns="",
                 init_table_fields="",
                 query="",
                 insertion_mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_columns = insert_columns
        self.query = query
        self.init_table_fields = init_table_fields
        self.insertion_mode = insertion_mode

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Creating {self.table} if table does not exist.")
        redshift.run(
            LoadDimensionOperator.create_sql.format(
                self.table,
                self.init_table_fields
            )
        )

        if self.insertion_mode == 'delete_and_insert':
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
            self.log.info("Appending data.")
        elif self.insertion_mode != 'append':
            raise ValueError(
                "Insert mode is not allowed. Please choose between"
                " append or delete_and_insert."
            )

        self.log.info("Appending data.")
        self.log.info(f"Inserting dimension data for {self.table}")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            self.table,
            self.insert_columns,
            self.query
        )
        redshift.run(formatted_sql)


