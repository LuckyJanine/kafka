import psycopg2
import postgre_db
from queries import data_load_query
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

import os
from dotenv import load_dotenv

load_dotenv()
target_dict = os.getenv('TARGET_DICT')

def main():
    try:
        db_connection = postgre_db.connect_to_consumer_db()
        with db_connection.cursor() as cursor:
            cursor.execute(data_load_query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['topic', 'partition',
                                             'log_append_time', 'receiving_time', 'processed_time',
                                             'consumer_id', 'consumer_group',
                                             'offset_lag', 'payload_size'])
            df['message_life_time'] = (df['processed_time'] - df['log_append_time']).dt.total_seconds()
            df['message_process_time'] = (df['processed_time'] - df['receiving_time']).dt.total_seconds()
            # df['consumer_group_cat'] = df['consumer_group'].astype('category')
            # df['consumer_group_cat'] = df['consumer_group_cat'].cat.codes
            # print(df.dtypes)
            preprocess_data(df)
            # kmeans_clustering(df)
            dbscan_clustering(df)
            # hdbscan_clustering(df)
            # train_and_process(df)
    except psycopg2.Error as e:
        print(f"connection error database: {e}")
    except Exception as e:
        print(f"unexpected error: {e}")
    finally:
        if db_connection is not None:
            db_connection.close()
            print("connection closed.")
        if df is not None:
            print("Dataset processed ...")

if __name__ == '__main__':
    print("Hello, World!")
    main()