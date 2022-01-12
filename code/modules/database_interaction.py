import psycopg2
import psycopg2.extras as extras
from jinjasql import JinjaSql
import pandas.io.sql as sqlio
import logging

def postgres_connect():
    # so it is possible to work with UUID types
    psycopg2.extras.register_uuid()
    
    return psycopg2.connect(
        host = "postgres",
        database = "test",
        user = "test",
        password = "postgres"
      )


def postgres_insert_df(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    logging.info("execute_values() done")
    cursor.close()
    
def sql_file_to_df(conn, file, params=None):
    """
    Use JinjaSql to prepare safe sql statements from file
    """
    sql_string = open(file).read()
    if params is not None:
        sql_string, params = JinjaSql().prepare_query(
            source = sql_string,
            data = params
        )
    return sqlio.read_sql_query(sql=sql_string, con=conn, params=params)


  
