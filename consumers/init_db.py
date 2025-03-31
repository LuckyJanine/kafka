import psycopg2
import postgre_db
from queries import check_table_exists, create_msg_consumption_table

DB_NAME = "consumer_db"
TABLE_NAME = "message_consumption"

def consumer_db_exist(connection):
    try:
        if connection is not None:
            with connection.cursor() as cursor:
                # cursor.execute("SELECT current_database();")
                # current_database = cursor.fetchone()[0]
                cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (DB_NAME,))
                exist = cursor.fetchone() is not None
                return exist
    except psycopg2.Error as err:
        raise RuntimeError(f"consumer_db_exist() call failed: {err}") from err

def create_consumer_db():
    try:
        conn = postgre_db.connect_to_db_default()
        conn.autocommit = True
        if conn is not None:
            with conn.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE {DB_NAME};")
                print(f"{DB_NAME} created.")
    except psycopg2.Error as err:
        raise RuntimeError(f"create_consumer_db() call failed: {err}") from err
    finally:
        conn.close()
        print("conn closed ...")

def main():
    try:
        connection_default = postgre_db.connect_to_db_default()
        db_exist = consumer_db_exist(connection_default)
        connection_default.close()
        if not db_exist:
            create_consumer_db() # need a separate connection
        else:
            print(f"{DB_NAME} already exist.")

        # switch connection to consumer_db
        connection_consumer_db = postgre_db.connect_to_consumer_db()
        table_exists = postgre_db.fetch_data(connection_consumer_db, check_table_exists, (TABLE_NAME,))
        # print(table_exists)
        if table_exists[0][0]:
            print(f"{TABLE_NAME} exists")
        else:
            # postgre_db.execute_query(connection, 'CREATE EXTENSION IF NOT EXISTS "uuid-ossp";')
            postgre_db.execute_query(connection_consumer_db, create_msg_consumption_table, (TABLE_NAME,))
            print(f"{TABLE_NAME} created")
    finally:
        connection_consumer_db.close()
        print("consumer_db connection closed ...")

if __name__ == '__main__':
    print("Hello, World!")
    main()
