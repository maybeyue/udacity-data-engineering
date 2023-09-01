""" This module loads the staging table and inserts the data into
the appropriate tables.
"""

import configparser
import psycopg2
import logging

from sql_queries.sql_queries import (
    copy_table_queries,
    insert_table_queries,
    generated_tables
)

logger = logging.getLogger()


def load_staging_tables(cur, conn):
    """ Loads data from S3 bucket to the staging tables."""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """ Inserts data into the tables."""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def check_total_counts(cur):
    """ Checks total counts of all tables in the database."""
    for table in generated_tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        print(f"Total rows in {table} table: {cur.fetchone()[0]}")


def main():
    logging.info('Connecting to Redshift cluster.')
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()

    logging.info('Loading staging tables:')
    load_staging_tables(cur, conn)

    logging.info('Inserting tables:')
    insert_tables(cur, conn)

    logging.info('Checking total counts:')
    check_total_counts(cur)

    conn.close()


if __name__ == "__main__":
    main()
