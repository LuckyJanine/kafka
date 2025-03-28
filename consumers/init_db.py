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
        raise RuntimeError(f"consumer_db_exist() call failed: {e}") from err

def create_consumer_db(connection):
    try:
        if connection is not None:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE {DB_NAME};")
                print(f"{DB_NAME} created.")
    except psycopg2.Error as err:
        raise RuntimeError(f"create_consumer_db() call failed: {e}") from err

def main():
    connection = postgre_db.connect_to_db_default()
    db_exist = consumer_db_exist(connection)
    if not db_exist:
        create_consumer_db(connection)
    else:
        print("{DB_NAME} already exist.")
    try:
        table_exists = postgre_db.fetch_data(connection, check_table_exists, (TABLE_NAME,))
        if table_exists[0][0]:
            print("{TABLE_NAME} exists")
        else:
            postgre_db.execute_query(connection, create_msg_consumption_table, (TABLE_NAME,))
            print("{TABLE_NAME} created")
    finally:
        connection.close()
        print("connection closed ...")

if __name__ == '__main__':
    print("Hello, World!")
    main()
