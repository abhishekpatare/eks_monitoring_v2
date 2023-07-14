import pandas as pd


def get_node_cpu_stats(node):

    row = [
        node.node_name,
        node.instance_type,
        node.cpu_limit,
        (node.cpu_usage['values'].mean() / node.cpu_limit)*100,
        (node.cpu_usage['values'].median() / node.cpu_limit)*100,
        (node.cpu_usage['values'].quantile(0.95) / node.cpu_limit)*100,
        (node.cpu_usage['values'].quantile(0.99) / node.cpu_limit)*100,
        (node.cpu_usage['values'].max() / node.cpu_limit)*100,
    ]
    return row


def flag_nodes_by_high_avg_cpu_utilization(logger ,node_data, threshold):
    _table = []
    for _, node in node_data.items():
        try:
            if node.cpu_usage is None:
                continue
            if node.cpu_usage['values'].mean() > node.cpu_limit * threshold:
                _table.append(get_node_cpu_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for high avg cpu util node:{node.node_name}", e)

    df = pd.DataFrame(_table, columns=['node name',
                                       "instance type",
                                       "cpu limit",
                                       'Avg CPU Utilization',
                                       'Median CPU Utilization',
                                       '95%tile CPU Utilization',
                                       '99%tile CPU Utilization',
                                       'Max CPU Utilization'
                                       ])
    return df


def flag_nodes_by_high_probability_of_high_cpu_utilization(logger,node_data, cpu_util_threshold, prob_limit):
    _table = []
    for _, node in node_data.items():
        try:
            if node.cpu_usage is None:
                continue
            high_usage_freq = 0
            for cpu_usage in node.cpu_usage['values']:
                high_usage_freq += cpu_usage > cpu_util_threshold * node.cpu_limit
            if high_usage_freq > prob_limit * len(node.cpu_usage['values']):
                _table.append(get_node_cpu_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for more frequent high cpu util node:{node.node_name}", e)
    df = pd.DataFrame(_table, columns=['node name',
                                       "instance type",
                                       "cpu limit",
                                       'Avg CPU Utilization',
                                       'Median CPU Utilization',
                                       '95%tile CPU Utilization',
                                       '99%tile CPU Utilization',
                                       'Max CPU Utilization'
                                       ])
    return df
