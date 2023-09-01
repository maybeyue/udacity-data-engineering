"""This file checks for errors when copying data from the
S3 bucket to the staging tables."""

import configparser
import psycopg2

ERROR_QUERY = """
    SELECT
        starttime,
        query,
        filename,
        line_number,
        colname,
        col_length,
        position,
        raw_field_value,
        err_code,
        err_reason
    FROM stl_load_errors
    ORDER BY starttime DESC
    LIMIT 1;
"""


def check_error(cur, conn):
    # Query to fetch error details from stl_load_errors table
    cur.execute(ERROR_QUERY)
    error_details = cur.fetchall()

    # Close the cursor and connection
    cur.close()
    conn.close()

    # Print the error details in a readable format
    for error in error_details:
        print(error)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()
    check_error(cur, conn)


if __name__ == '__main__':
    main()
