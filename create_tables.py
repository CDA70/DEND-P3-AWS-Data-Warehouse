import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
        This function drops all tables in the database. 
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None

    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
        This function creates the tables as listed in the datamodel
        staging_events, staging_songs, songplay, user, song, artist, time
        Parameters:
                cur: cursor or query
                conn: connection to the database
        Returns:
                None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
        This is the main procedure that executes the drop_tables and create_tables
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
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()

if __name__ == "__main__":
    print("running create_tables")
    main()
    print("finished create_tables")