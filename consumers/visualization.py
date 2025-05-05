import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.gridspec as gridspec

from data_utils import load_data, preprocess_data

import os
from dotenv import load_dotenv

load_dotenv()
target_dict = os.getenv('TARGET_DICT')

df = None

def plot_distribution(df): # violinplot with quartiles
    fig, axes = plt.subplots(nrows=2, ncols=1, figsize=(12, 16), gridspec_kw={'hspace': 0.8})
    # message life time VS partitions
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
    plt.savefig(f"{target_dict}message_life_time_distribution_violinplots_vis.png")

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
    plt.savefig(f"{target_dict}message_life_time_scatterplots_vis.png")
    # plt.savefig(f"{target_dict}trail_scatterplots.png")

def main():
    try:
        df = load_data()
        df = preprocess_data(df)
        if df is not None:
            print("dataframe loaded.")
            print(df.head())
            return df
    except Exception as e:
        print(f"unexpected error: {e}")
    finally:
        print("visualization done.")

if __name__ == '__main__':
    print("Hello, World!")
    df = main()
    plot_distribution(df)
    plot_scatter(df)