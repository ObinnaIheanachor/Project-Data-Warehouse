import psycopg2
import configparser
from sql_queries import count_queries


def count_query(cur, conn):
    """
    Run the queries in the 'count_queries' list on all tables.
    :param cur: cursor object to database connection
    :param conn: connection object to database
    """
    
    for query in count_queries:
        print('Running ' + query)         
        try:
            cur.execute(query)
            results = cur.fetchone()

            for row in results:
                print("   ", row)
                conn.commit()
                
        except psycopg2.Error as e:
            print(e)
            conn.close()


def main():
    """
    Run COUNT(*) query on all tables to validate that data has been loaded correctly into Redshift
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    count_query(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()