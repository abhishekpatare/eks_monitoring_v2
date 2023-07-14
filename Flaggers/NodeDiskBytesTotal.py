import pandas as pd


def get_node_disk_total_bytes_stats(node):
    return [
        node.node_name,
        node.instance_type,
        node.ebs_baseline_bandwidth,
        node.disk_total_bytes['values'].mean(),
        node.disk_total_bytes['values'].median(),
        node.disk_total_bytes['values'].quantile(0.95),
        node.disk_total_bytes['values'].quantile(0.99),
        node.disk_total_bytes['values'].max(),
    ]


def flag_nodes_by_high_avg_disk_total_bytes(logger,node_data, threshold):
    bad_node_list = []

    for _, node in node_data.items():
        try:
            if node.ebs_baseline_bandwidth is None:
                continue
            if node.disk_total_bytes is None:
                continue
            if node.disk_total_bytes['values'].mean() > threshold * node.ebs_baseline_bandwidth * 1.25 * 1E8:
                bad_node_list.append(get_node_disk_total_bytes_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for high avg disk bytes node:{node.node_name}", e)
    df = pd.DataFrame(bad_node_list, columns=[
        'node name',
        "instance type",
        'node ebs baselimit(MB/sec)',
        'Avg disk total bytes rate (MB/sec)',
        "Median disk total bytes(MB/sec)",
        "95%tile disk total bytes(MB/sec)",
        "99%tile disk total bytes(MB/sec)",
        "Max disk total bytes(MB/sec)"
    ])
    return df


def flag_nodes_by_high_probability_of_high_disk_total_bytes(logger,node_data, threshold, prob_limit):
    bad_node_list = []
    for _, node in node_data.items():
        try:
            if node.ebs_baseline_bandwidth is None:
                continue
            if node.disk_total_bytes is None:
                continue
            node_bandwidth_limit = node.ebs_baseline_bandwidth * 1.25 * 1E8  # conversion from Gbps to bytes/sec
            high_usage_freq = 0
            for disk_total_bytes in node.disk_total_bytes['values']:
                high_usage_freq += disk_total_bytes > threshold * node_bandwidth_limit
            if high_usage_freq > prob_limit * len(node.disk_total_bytes['values']):
                bad_node_list.append(get_node_disk_total_bytes_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for high avg tx bytes node:{node.node_name}", e)
    df = pd.DataFrame(bad_node_list, columns=[
        'node name',
        "instance type",
        'node ebs baselimit(MB/sec)',
        'Avg disk total bytes rate (MB/sec)',
        "Median disk total bytes(MB/sec)",
        "95%tile disk total bytes(MB/sec)",
        "99%tile disk total bytes(MB/sec)",
        "Max disk total bytes(MB/sec)"
    ])
    
    return df
