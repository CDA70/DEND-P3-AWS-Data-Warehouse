import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
        This function loads the data from the S3 data bucket into the staging Tables.
        The S3 LOG data is loaded into the staging_events table (running staging_events_copy).
        The S3 songs data is loaded into staging_songs table (running staging_songs_copy).
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()



def insert_tables(cur, conn):
    """
        This function is the actual ETL process and inserts data into the following tables
        songplay, user, song, artist, time
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        This is the main procedure that executes the load_staging_tables and Insert_tables
        it also provides the two parameters required to run the queries
        conn: open and connects to the database, the parameters are read from the config file
        cur: executes the cursor or query as specified in the functions
        Parameters:
                None
        Returns:
                None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    print('running ETL process')
    main()
    print('finished ETL process')