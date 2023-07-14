import pandas as pd


def get_node_rx_bytes_stats(node):
    return [
        node.node_name,
        node.instance_type,
        node.network_bandwidth_limit,
        node.network_rx_bytes['values'].mean(),
        node.network_rx_bytes['values'].median(),
        node.network_rx_bytes['values'].quantile(0.95),
        node.network_rx_bytes['values'].quantile(0.99),
        node.network_rx_bytes['values'].max(),
    ]


def flag_nodes_by_high_avg_network_rx_bytes(logger, node_data, threshold):
    bad_node_list = []

    for _, node in node_data.items():
        try:
            if node.network_bandwidth_limit is None:
                continue
            if node.network_rx_bytes is None:
                continue
            if node.network_rx_bytes['values'].mean() > threshold * node.network_bandwidth_limit * 1.25 * 1E8:
                bad_node_list.append(get_node_rx_bytes_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for high avg rx bytes node:{node.node_name}", e)
    df = pd.DataFrame(bad_node_list, columns=[
        'node name',
        "instance type",
        'node bandwidth baselimit(bytes/sec)',
        'Avg Network Received rate (bytes/sec)',
        "Median rx bytes",
        "95%tile rx bytes",
        "99%tile rx bytes",
        "Max rx bytes"
    ])
    return df


def flag_nodes_by_high_probability_of_high_network_rx_bytes(logger,node_data, threshold, prob_limit):
    bad_node_list = []
    for _, node in node_data.items():
        try:
            if node.network_bandwidth_limit is None:
                continue
            if node.network_rx_bytes is None:
                continue
            node_bandwidth_limit = node.network_bandwidth_limit * 1.25 * 1E8  # conversion from Gbps to bytes/sec
            high_usage_freq = 0
            for rx_bytes in node.network_rx_bytes['values']:
                high_usage_freq += rx_bytes > threshold * node_bandwidth_limit
            if high_usage_freq > prob_limit * len(node.network_rx_bytes):
                bad_node_list.append(get_node_rx_bytes_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for frequent high rx bytes node:{node.node_name}", e)
    df = pd.DataFrame(bad_node_list, columns=[
        'node name',
        "instance type",
        'node bandwidth baselimit(bytes/sec)',
        'Avg Network Received rate (bytes/sec)',
        "Median rx bytes",
        "95%tile rx bytes",
        "99%tile rx bytes",
        "Max rx bytes"
    ])
    return df
