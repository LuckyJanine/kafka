import psycopg2
import postgre_db
from queries import data_load_query
import pandas as pd

def load_data():
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
            return df
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


def preprocess_data(df):
    partitions=['flight-atc : P0', 'flight-atc : P1', 'flight-atc : P2',
                'flight-atc : P3', 'flight-atc : P4', 'flight-atc : P5',
                'engine-logs : P0', 'engine-logs : P1', 'engine-logs : P2',
                'engine-logs : P3', 'engine-logs : P4', 'engine-logs : P5',
                'hydraulic-logs : P0', 'hydraulic-logs : P1',
                'hydraulic-logs : P2', 'hydraulic-logs : P3']
    df['topic_partition'] = df['topic'].astype(str) + " : P" + df['partition'].astype(str)
    df['topic_partition'] = pd.Categorical(df['topic_partition'], categories=partitions, ordered=True)
    df['consumergroup_consumer'] = df['consumer_group'].astype(str) + " : C" + df['consumer_id'].astype(str)
    df['consumergroup_consumer'] = pd.Categorical(df['consumergroup_consumer'], categories=sorted(df['consumergroup_consumer'].unique()), ordered=True)
    return df