import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from files stored in S3 to the staging tables using queries from sql_queries script
    """
    print("Loading data from JSON files stored in S3 buckets into staging tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data from staging tables into actual tables using the queries from sql_queries script
    """
    print("Inserting data from staging tables into Redshift tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Extract songs metadata and user activity data from S3, transform using a staging table, and load into tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()