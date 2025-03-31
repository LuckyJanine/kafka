import psycopg2
from psycopg2 import pool
import os
from dotenv import load_dotenv

load_dotenv()
db_params = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PWD'),
    'dbname': 'postgres' # default database from postgre installation
}

def connect_to_db_default():
    try:
        db_connection = psycopg2.connect(**db_params)
        print("Connected to '[postgre]' database")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    return db_connection

def connect_to_consumer_db():
    db_params['dbname'] = 'consumer_db'
    try:
        db_connection = psycopg2.connect(**db_params)
        print("Connected to '[consumer_db]' database")
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
    return db_connection

def get_connection_pool(num_consumers):
    db_params['dbname'] = 'consumer_db'
    connection_pool = pool.SimpleConnectionPool(minconn=1, maxconn=num_consumers, **db_params)
    return connection_pool

def fetch_data(connection, query, params=None):
    try:
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchall()
            return result
        else:
            raise RuntimeError("no connection made.")
    except psycopg2.Error as err:
        print(f"fetch_data query error: {err}")
        return None

def execute_query(connection, query, params=None, fetch_result=False):
    try:
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                connection.commit()
                if fetch_result:
                    result = cursor.fetchall()
                    return result
                else:
                    return None
        else:
            raise RuntimeError("no connection made.")
    except psycopg2.Error as err:
        print(f"execute query error: {err}")
        return None