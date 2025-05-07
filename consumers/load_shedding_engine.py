from data_utils import load_data, preprocess_data
from cluster_ml_analysis import dbscan_main_clustering, summarize_primary_clusters, dbscan_nested_clustering, summarize_secondary_clusters
from regression_stat_analysis import profile_backlog_batch

# const
# key: topic
# value: consumer group
PROJECT_SETUP = {
    "flight-atc": "atc_group",
    "engine-logs": "engine_log_group",
    "hydraulic-logs": "hydraulic_log_group"
}

class MessageLifetimeExtremes:
    def __init__(self, base, increase):
        self.baseline = base
        self.batch_increase = increase
    @property
    def estimate_sum(self):
        return self.baseline + self.batch_increase


# initialize variable
# key: consumer group
# value: MessageLifetimeExtremes class
max_lifetime_by_consumer_group = {
    "atc_group": MessageLifetimeExtremes(0, 0),
    "engine_log_group": MessageLifetimeExtremes(0, 0),
    "hydraulic_log_group": MessageLifetimeExtremes(0, 0)
}

df = None

def update_max_lifetime_by_consumer_group(consumer_group, baseline, max_lifetime_increase):
    current_baseline = max_lifetime_by_consumer_group[consumer_group].baseline
    current_lifetime_increase = max_lifetime_by_consumer_group[consumer_group].batch_increase
    if baseline > current_baseline:
        max_lifetime_by_consumer_group[consumer_group].baseline = baseline
    if max_lifetime_increase > current_lifetime_increase:
        max_lifetime_by_consumer_group[consumer_group].batch_increase = max_lifetime_increase

def main():
    try:
        df = load_data()
        if df is not None:
            print("dataframe loaded.")
        df = preprocess_data(df)
        df, primary_score = dbscan_main_clustering(df)
        if primary_score > 0.80:
            print("first level clustering went well. Start summarizing ...")
            df = df.sort_values('log_append_time').reset_index(drop=True)
            primary_cluster_summary = summarize_primary_clusters(df)
            for cluster_summary in primary_cluster_summary:
                cluster_id = cluster_summary['cluster']
                current_cluster = df[df['cluster'] == cluster_id]
                if not cluster_summary['substructure']: # no need for further 'nesting'
                    dominant_consumer_group = cluster_summary['dominant_consumer_group']
                    target_baseline = cluster_summary['max_message_life_time']
                    target_batch_lifetime_increase = profile_backlog_batch(current_cluster)
                    update_max_lifetime_by_consumer_group(dominant_consumer_group, target_baseline, target_batch_lifetime_increase)
                else:
                    sub_cluster_labels = dbscan_nested_clustering(current_cluster)
                    current_cluster['nested_cluster'] = sub_cluster_labels
                    # print(current_cluster.head())
                    secondary_cluster_summary = summarize_secondary_clusters(current_cluster)
                    # print(secondary_cluster_summary)
                    for sub_cluster_summary in secondary_cluster_summary:
                        sub_cluster_id = sub_cluster_summary['sub_cluster']
                        dominant_consumer_group = sub_cluster_summary['dominant_consumer_group']
                        current_sub_cluster = current_cluster[current_cluster['nested_cluster'] == sub_cluster_id]
                        target_baseline = sub_cluster_summary['max_message_life_time']
                        target_batch_lifetime_increase = profile_backlog_batch(current_sub_cluster)
                        update_max_lifetime_by_consumer_group(dominant_consumer_group, target_baseline, target_batch_lifetime_increase)
        else:
            print("clustering didn't work well....")
    except Exception as e:
        print(f"error: {e}")
    finally:
        print("message lifetime prediction done.")

if __name__ == '__main__':
    print("Hello, from Load-shedding Engine!")
    main()
    print("===========================")
    for group in max_lifetime_by_consumer_group.values():
        print(f"{group.baseline}, {group.batch_increase}, {group.estimate_sum}")
    response = input("would you like to proceed with the retention time suggestions?\n (y/n): ").strip().lower()
    if response == 'y':
        print("Continuing...")
    elif response == 'n':
        print("Stopping the program.\n Please make sure Kafka broker has enough storage for log segments ...")
    else:
        print("Invalid input.")
