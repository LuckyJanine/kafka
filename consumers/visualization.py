import psycopg2
import postgre_db
from queries import data_load_query
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.gridspec as gridspec

import os
from dotenv import load_dotenv

load_dotenv()
target_dict = os.getenv('TARGET_DICT')

df = None

def plot_distribution(df): # violinplot with quartiles
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 16), gridspec_kw={'hspace': 0.8})
    df['topic_partition'] = df['topic'].astype(str) + " : P" + df['partition'].astype(str)
    # message life time VS partitions
    partitions=['flight-atc : P0', 'flight-atc : P1', 'flight-atc : P2',
                'flight-atc : P3', 'flight-atc : P4', 'flight-atc : P5',
                'engine-logs : P0', 'engine-logs : P1', 'engine-logs : P2',
                'engine-logs : P3', 'engine-logs : P4', 'engine-logs : P5',
                'hydraulic-logs : P0', 'hydraulic-logs : P1',
                'hydraulic-logs : P2', 'hydraulic-logs : P3']
    df['topic_partition'] = pd.Categorical(df['topic_partition'], categories=partitions, ordered=True)
    sns.violinplot(
        ax=axes[0],
        x='topic_partition',
        y='message_life_time',
        data=df,
        inner='quartile',
        hue=df['topic'],
        palette='Set2'
    )
    axes[0].set_title('Message Lifetime vs Partitions')
    axes[0].tick_params(axis='x', rotation=45)
    axes[0].set_ylabel('Message Lifetime (seconds)')
    # message life time VS consumers
    df['consumergroup_consumer'] = df['consumer_group'].astype(str) + " : C" + df['consumer_id'].astype(str)
    df['consumergroup_consumer'] = pd.Categorical(df['consumergroup_consumer'], categories=sorted(df['consumergroup_consumer'].unique()), ordered=True)
    sns.violinplot(
        ax=axes[1],
        x='consumergroup_consumer',
        y='message_life_time',
        data=df,
        inner='quartile',
        hue=df['topic'],
        palette='Set2'
    )
    axes[1].set_title('Message Lifetime vs Consumers')
    axes[1].tick_params(axis='x', rotation=45)
    axes[1].set_ylabel('Message Lifetime (seconds)')
    axes[1].set_xlabel('Consumer Group_consumer')
    plt.savefig(f"{target_dict}message_life_time_distribution_violinplots.png")

def plot_scatter(df):
    fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(12, 16), gridspec_kw={'hspace': 0.6})
    fig.delaxes(axes[0, 1])
    top_ax = fig.add_subplot(2, 1, 1)
    # topic: ATC
    atc_records = df[df['topic'] == 'flight-atc']
    # df_sorted = atc_records.sort_values('message_life_time', ascending=False)
    # print(atc_records[['topic', 'log_append_time', 'message_life_time', 'consumergroup_consumer']].head(5))
    sns.scatterplot(
        data=atc_records,
        x='log_append_time',
        y='message_life_time',
        hue=atc_records['consumergroup_consumer'],
        hue_order=atc_records['consumergroup_consumer'].unique(),
        palette='Set2',
        ax=top_ax
    )
    top_ax.set_title("flight atc message Consumption Scatter Plot")
    # topic: engine logs
    engine_records = df[df['topic'] == 'engine-logs']
    sns.scatterplot(
        data=engine_records,
        x='log_append_time',
        y='message_life_time',
        hue=engine_records['consumergroup_consumer'],
        hue_order=engine_records['consumergroup_consumer'].unique(),
        palette='Set2',
        ax=axes[1, 0]
    )
    axes[1, 0].tick_params(axis='x', rotation=45)
    axes[1, 0].set_title("engine logs message Consumption Scatter Plot")
    # topic: hydraulic logs
    hydraulic_records = df[df['topic'] == 'hydraulic-logs']
    sns.scatterplot(
        data=hydraulic_records,
        x='log_append_time',
        y='message_life_time',
        hue=hydraulic_records['consumergroup_consumer'],
        hue_order=hydraulic_records['consumergroup_consumer'].unique(),
        palette='Set2',
        ax=axes[1, 1]
    )
    axes[1, 1].tick_params(axis='x', rotation=45)
    axes[1, 1].set_title("hydraulic logs message Consumption Scatter Plot")
    plt.savefig(f"{target_dict}message_life_time_vs_log_append_time_scatterplots.png")

def main():
    try:
        db_connection = postgre_db.connect_to_consumer_db()
        with db_connection.cursor() as cursor:
            cursor.execute(data_load_query)
            data = cursor.fetchall()
            df = pd.DataFrame(data, columns=['topic', 'partition', 'log_append_time', 'receiving_time', 'processed_time', 'consumer_id', 'consumer_group', 'offset_lag'])
            df['message_life_time'] = (df['processed_time'] - df['log_append_time']).dt.total_seconds()
            # print(df.dtypes)
            # print(df[['topic', 'partition', 'consumer_id', 'message_life_time', 'offset_lag']].head(10))
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

if __name__ == '__main__':
    print("Hello, World!")
    df = main()
    plot_distribution(df)
    plot_scatter(df)