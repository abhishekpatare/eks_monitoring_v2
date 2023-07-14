import pandas as pd


def get_node_memory_stats(node):
    return [
        node.node_name,
        node.instance_type,
        node.memory_limit,
        (node.memory_usage['values'].mean() / node.memory_limit)*100,
        (node.memory_usage['values'].median() / node.memory_limit)*100,
        (node.memory_usage['values'].quantile(0.95) / node.memory_limit)*100,
        (node.memory_usage['values'].quantile(0.99) / node.memory_limit)*100,
        (node.memory_usage['values'].max() / node.memory_limit)*100,
    ]


def flag_nodes_by_high_avg_memory_utilization(logger, node_data, threshold):
    _table = []
    for _, node in node_data.items():
        try:
            if node.memory_usage is None:
                continue
            if node.memory_usage['values'].mean() > threshold * node.memory_limit:
                _table.append(get_node_memory_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for high avg memory utilization node:{node.node_name}", e)
    df = pd.DataFrame(_table, columns=['node name',
                                       "instance type",
                                       "Memory limit",
                                       'Avg Memory Utilization(%)',
                                       'Median Memory Utilization(%)',
                                       '95%tile Memory Utilization(%)',
                                       '99%tile Memory Utilization(%)',
                                       'Max Memory Utilization(%)'])
    return df


def flag_nodes_by_high_probability_of_high_memory_utilization(logger,node_data, memory_util_threshold, prob_limit):
    _table = []
    for _, node in node_data.items():
        try:
            high_usage_freq = 0
            if node.memory_usage is None:
                continue
            for mem_use in node.memory_usage['values']:
                high_usage_freq += mem_use > memory_util_threshold * node.memory_limit
            if high_usage_freq > prob_limit * len(node.memory_usage['values']):
                _table.append(get_node_memory_stats(node))
        except Exception as e:
            logger.error(f"Error flagging nodes for more frequent high memory utilization node:{node.node_name}", e)
    df = pd.DataFrame(_table, columns=['node name',
                                       "instance type",
                                       "Memory limit",
                                       'Avg Memory Utilization(%)',
                                       'Median Memory Utilization(%)',
                                       '95%tile Memory Utilization(%)',
                                       '99%tile Memory Utilization(%)',
                                       'Max Memory Utilization(%)'])
    return df
