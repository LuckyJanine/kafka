import psycopg2
import postgre_db
from queries import data_load_query
import pandas as pd
import numpy as np

import lightgbm as lgb
from lightgbm import LGBMRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

import os
from dotenv import load_dotenv

load_dotenv()
target_dict = os.getenv('TARGET_DICT')

df = None

def 

def train_and_process(df):
    topics = df['topic'].unique()
    for topic in topics:
        topic_df = df[df['topic'] == topic]
        X = topic_df[['log_append_hour', 'log_append_minute', 'log_append_second',
                      'receiving_hour', 'receiving_minute', 'receiving_second',
                      'processed_hour', 'processed_minute', 'processed_second',
                      'consumer_id', 'offset_lag']]
        y = topic_df['message_life_time']

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = LGBMRegressor()
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)

        # Print results for the current topic
        print(f"Topic {topic}:")
        print(f"Mean Squared Error (MSE): {mse}")
        print(f"Root Mean Squared Error (RMSE): {rmse}\n")

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
            df['consumer_group_cat'] = df['consumer_group'].astype('category')
            df['consumer_group_cat'] = df['consumer_group_cat'].cat.codes
            print(df.dtypes)
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