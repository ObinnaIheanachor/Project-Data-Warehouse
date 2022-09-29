import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops the tables in the data warehouse in Amazon Redshift.
    We can run create_tables.py when we need to reset the database and test the ETL pipeline.
    
    """
    print('Dropping Tables')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create staging and dimensional tables from sql_queries script
    """
    for query in create_table_queries:
        print('Running ' + query + ' ')
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the Redshift cluster, drops and creates the tables by calling 'drop_tables' and 'create_tables' functions.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Connected to the cluster')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()