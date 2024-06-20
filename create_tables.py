import sys
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in the database using the provided SQL queries.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in drop_table_queries:
        # Execute DROP TABLE statement
        cur.execute(query)
        # Commit changes to the database
        conn.commit()
        


def create_table_queries(cur, conn):
    """
    Create tables in the databases using the provided SQL queries.
    Parameters:
    - cur: cursor object
    - conn: connection object
    """
    for query in create_table_queries:
        #EXECUTE CREATE TABLE statement  
        cur.execute(query)
        #Adding changes to the database         
        conn.commit()
        
     
    
    

def main():
    """
    Establishes a connect to the databases
    Drops table in the databases if they exist utilizing the created SQL Commands.
    Spins Up new tables in the Database using the  SQL Commands.
    Parameters:
    - cur: cursor object
    - conn: connection object  
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    ## Execute DROP TABLE statement 
    drop_tables(cur, conn)
    # Execute CREATE TABLE statement
    create_table_queries(cur, conn)

    #Closes the connection     
    conn.close()


if __name__ == "__main__":
    main()