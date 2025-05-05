import psycopg2
import postgre_db
from queries import data_load_query
import pandas as pd
import numpy as np

from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score, silhouette_samples
from sklearn.decomposition import PCA

# import hdbscan

import matplotlib.pyplot as plt
import seaborn as sns

import lightgbm as lgb
from lightgbm import LGBMRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

import os
from dotenv import load_dotenv

load_dotenv()
target_dict = os.getenv('TARGET_DICT')

df = None

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

def hdbscan_clustering(df):
    features = ['message_process_time', 'message_life_time', 'offset_lag', 'payload_size']
    scaled = StandardScaler().fit_transform(df[features])
    clusterer = hdbscan.HDBSCAN(min_cluster_size=10)
    df['cluster'] = clusterer.fit_predict(scaled)
    pca = PCA(n_components=2)
    df[['x', 'y']] = pca.fit_transform(scaled)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='x', y='y', hue='cluster', palette='tab10', style='cluster')
    plt.title("HDBSCAN Clustering")
    plt.legend(title='Cluster')
    plt.savefig(f"{target_dict}hdbscan_clustering.png")

def kmeans_clustering(df):
    features = ['message_process_time', 'message_life_time', 'offset_lag', 'payload_size']
    scaled = StandardScaler().fit_transform(df[features])
    kmeans = KMeans(n_clusters=3, random_state=42)
    df['cluster'] = kmeans.fit_predict(scaled)
    labels = df['cluster']
    summarize_silhouette_scores(scaled, labels)
    pca = PCA(n_components=2)
    pca_components = pca.fit_transform(scaled)
    df['PCA_x'] = pca_components[:, 0]
    df['PCA_y'] = pca_components[:, 1]
    loadings = pd.DataFrame(pca.components_, columns=features, index=['PC1', 'PC2'])
    print(loadings.T)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='PCA_x', y='PCA_y', hue='cluster', palette='tab10', style='cluster')
    plt.xlabel("PCA x")
    plt.ylabel("PCA y")
    plt.title("KMeans Clustering")
    plt.legend(title='Cluster')
    plt.savefig(f"{target_dict}kmeans_clustering.png")
    # cluster_2_df = df[df['cluster'] == 2]
    # print(cluster_2_df.head())
    # kmeans_plot_the_cluster(cluster_2_df)

def kmeans_plot_the_cluster(cluster_df):
    df = cluster_df.copy()
    df['consumergroup_consumer'] = df['consumergroup_consumer'].cat.remove_unused_categories()
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df,
        x='PCA_x', y='PCA_y',
        hue='consumer_group',
        style='consumergroup_consumer',
        palette='tab10',
        s=80
    )
    cluster_color = sns.color_palette('tab10')[2]
    plt.title(f"Cluster 2 – Consumer Groups / Consumers", color=cluster_color, fontweight='bold')
    plt.xlabel("PCA x")
    plt.ylabel("PCA y")
    plt.savefig(f"{target_dict}kmeans_single_cluster_2_scatterplot.png")

def summarize_silhouette_scores(scaled, labels):
    overall_score = silhouette_score(scaled, labels)
    print(f"Overall Silhouette Score: {overall_score:.4f}")
    sample_values = silhouette_samples(scaled, labels)
    # print(sample_values[:5])
    unique_labels = np.unique(labels)
    cluster_silhouette_scores = {}
    for label in unique_labels:
        cluster_scores = sample_values[labels == label]
        cluster_avg_score = np.mean(cluster_scores)
        cluster_silhouette_scores[label] = cluster_avg_score
        print(f"Cluster {label}: Average Silhouette Score = {cluster_avg_score:.4f}")

def dbscan_clustering(df):
    features = ['message_process_time', 'message_life_time', 'offset_lag', 'payload_size']
    scaled = StandardScaler().fit_transform(df[features])
    db = DBSCAN(eps=0.5, min_samples=10).fit(scaled)
    df['cluster'] = db.labels_
    pca = PCA(n_components=2)
    pca_components = pca.fit_transform(scaled)
    loadings = pd.DataFrame(pca.components_, columns=features, index=['PC1', 'PC2'])
    print(loadings.T)
    labels = df['cluster']
    summarize_silhouette_scores(scaled, labels)
    core_samples = scaled[labels != -1]
    core_labels = labels[labels != -1]
    score = silhouette_score(core_samples, core_labels)
    print(score)
    # df[['x', 'y']] = pca.fit_transform(scaled)
    df['PCA_x'] = pca_components[:, 0]
    df['PCA_y'] = pca_components[:, 1]
    # z_feature = df['message_life_time']
    # fig = plt.figure(figsize=(10, 6))
    plt.figure(figsize=(10, 6))
    # ax = fig.add_subplot(111, projection='3d')
    # sc = ax.scatter(df['PCA_x'], df['PCA_y'], z_feature, c=z_feature, cmap='viridis')
    # ax.set_xlabel('PCA x')
    # ax.set_ylabel('PCA y')
    # ax.set_zlabel('Message Lifetime (seconds)')
    # plt.colorbar(sc, label='Z feature scale')
    # plt.title("3D Scatter: PCA x/y + Message Lifetime")
    sns.scatterplot(
        data=df,
        x='PCA_x',
        y='PCA_y',
        hue='cluster',
        palette='tab10',
        style='cluster',
        alpha=0.8
    )
    plt.xlabel("PCA x")
    plt.ylabel("PCA y")
    plt.title("DBSCAN Clustering")
    plt.legend(title='Cluster')
    plt.savefig(f"{target_dict}dbscan_clustering.png")
    # plt.savefig(f"{target_dict}dbscan_clustering_lifetime.png")
    # plt.savefig(f"{target_dict}dbscan_clustering_3D.png")
    cluster_1_df = df[df['cluster'] == 1]
    # plot_the_cluster(cluster_2_df)
    nested_clustering(cluster_1_df)

def nested_clustering(df_cluster):
    features = ['message_process_time', 'message_life_time', 'offset_lag', 'payload_size']
    scaled = StandardScaler().fit_transform(df_cluster[features])
    db = DBSCAN(eps=0.5, min_samples=10).fit(scaled)
    df_cluster['nested_cluster'] = db.labels_
    summarize_silhouette_scores(scaled, db.labels_)
    # print(df_cluster.head())
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df_cluster,
        x='PCA_x',
        y='PCA_y',
        hue='nested_cluster',
        palette='Set2',
        style='nested_cluster',
        alpha=0.8
    )
    plt.xlabel("PCA x")
    plt.ylabel("PCA y")
    plt.title("DBSCAN Subset (nested) Clustering")
    plt.legend(title='Cluster')
    plt.savefig(f"{target_dict}dbscan_nested_clustering.png")
    nested_cluster_1_df = df_cluster[df_cluster['nested_cluster'] == 1]
    plot_the_cluster(nested_cluster_1_df)

def plot_the_cluster(df_cluster):
    df = df_cluster.copy()
    # df['topic_partition'] = df['topic_partition'].cat.remove_unused_categories()
    df['consumergroup_consumer'] = df['consumergroup_consumer'].cat.remove_unused_categories()
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df,
        x='PCA_x', y='PCA_y',
        hue='consumer_group',
        style='consumergroup_consumer',
        palette='tab10',
        s=80
    )
    plt.xlabel("PCA x")
    plt.ylabel("PCA y")
    cluster_color = sns.color_palette('Set2')[2]
    plt.title(f"(DBSCAN) nested (sub)Cluster 1 – Consumer Group", color=cluster_color, fontweight='bold')
    plt.savefig(f"{target_dict}dbscan_single_sub_cluster_1_scatterplot.png")

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