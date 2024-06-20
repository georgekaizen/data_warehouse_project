import sys
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))



def load_staging_tables(cur, conn):
    """
    Load data from staging tables into the target tables using COPY statements.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    insert data from staging tables into the target tables using COPY statements.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

        
        
        
def main():
    """
    The main ETL process that connects to the database, loads staging tables, and inserts data into the target tables.
    """

    # Connecting to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Load data from staging tables
    load_staging_tables(cur, conn)

    # Insert data into target tables
    insert_tables(cur, conn)

    # Close the database connection
    conn.close()


if __name__ == "__main__":
    main()
