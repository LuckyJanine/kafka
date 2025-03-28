import psycopg2
import os
from dotenv import load_dotenv

def connect_to_db():
    load_dotenv()
    db_params = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'user': os.getenv('DB_USER'),
        'pwd': os.getenv('DB_PWD')
    }
    print(f"{db_params.get('host')}")
#     try:
#         conn = psycopg2.connect(**db_params)
#         print("Connection successful!")
#         conn.close()
#     except Exception as e:
#         print(f"Error: {e}")
