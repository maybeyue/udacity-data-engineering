"""Run basic analysis on the data warehouse."""
import configparser
import psycopg2

from basic_analysis_queries import analysis_questions


def analysis(cur):
    for question, query in analysis_questions.items():
        cur.execute(query)
        print(question)
        for data in cur.fetchall():
            print(data)
        print(f"\n {'*'*20} \n")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect(
        "host={} dbname={} user={} password={} port={}".format(
            *config['CLUSTER'].values()
        )
    )
    cur = conn.cursor()
    analysis(cur)

    conn.close()


if __name__ == "__main__":
    main()
