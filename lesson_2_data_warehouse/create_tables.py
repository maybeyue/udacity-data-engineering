""" This module is used to drop existing table and create empty tables
in order to later load the data from the S3 bucket."""
import configparser
import logging
import psycopg2

from sql_queries.sql_queries import create_table_queries, drop_table_queries

logger = logging.getLogger()


def drop_tables(cur, conn):
    """Drops all tables in the database."""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Creates all tables in the database."""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()

    logger.info('Dropping existing tables.')
    drop_tables(cur, conn)

    logger.info('Creating tables.')
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
