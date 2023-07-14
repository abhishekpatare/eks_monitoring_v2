import pandas as pd


def calculate_exp_request(usage, margin):
    avg = usage['values'].mean()
    stddev = usage['values'].std()
    exp_request = avg + margin * stddev
    return exp_request


def flag_pods_for_wrong_cpu_requests(logger, pod_data, margin=1, threshold=0.7):
    _table = []
    for (namespace, pod_name), pod in pod_data.items():
        try:
            if pod.cpu_usage is None:
                logger.info(f"Cpu usage not available for pod:{pod_name}, namespace:{namespace}")
                continue
            exp_cpu_req = calculate_exp_request(pod.cpu_usage, margin)
            bad_cpu_request = (abs(pod.cpu_request - exp_cpu_req) / exp_cpu_req > threshold)

            if bad_cpu_request:
                _table.append(
                    [
                        namespace,
                        pod_name,
                        pod.cpu_request,
                        pod.cpu_limit,
                        pod.cpu_usage['values'].mean(),
                        pod.cpu_usage['values'].median(),
                        pod.cpu_usage['values'].quantile(0.95),
                        pod.cpu_usage['values'].quantile(0.99),
                        pod.cpu_usage['values'].max(),
                        exp_cpu_req
                    ])
        except Exception as e:
            logger.error(f"Error flagging pods for wrong cpu requests for namespace:{namespace},pod:{pod_name}", e)
    df = pd.DataFrame(_table, columns=[
        "Namespace",
        "Pod Name",
        "CPU Request",
        "CPU Limit",
        "Avg CPU Usage(Cores)",
        "Median Usage",
        "95%tile CPU Usage",
        "99%tile CPU Usage",
        "Max CPU Usage",
        "Suggested Request"
    ])
    df.sort_values(['Namespace'])
    return df


def flag_pods_for_wrong_memory_requests(logger, pod_data, margin=1, threshold=0.7):
    _table = []
    for (namespace, pod_name), pod in pod_data.items():
        try:
            if pod.memory_usage is None:
                logger.info(f"Memory usage not available for pod:{pod_name}, namespace:{namespace}")
                continue
            exp_memory_req = calculate_exp_request(pod.memory_usage, margin)
            bad_memory_request = abs(pod.memory_request - exp_memory_req) / exp_memory_req > threshold

            if bad_memory_request:
                _table.append(
                    [
                        namespace,
                        pod_name,
                        pod.memory_request,
                        pod.memory_limit,
                        pod.memory_usage['values'].mean(),
                        pod.memory_usage['values'].median(),
                        pod.memory_usage['values'].quantile(0.95),
                        pod.memory_usage['values'].quantile(0.99),
                        pod.memory_usage['values'].max(),
                        exp_memory_req
                    ])
        except Exception as e:
            logger.error(f"Error flagging pods for wrong memory requests for namespace:{namespace},pod:{pod_name}", e)

    df = pd.DataFrame(_table, columns=["Namespace",
                                       "Pod Name",
                                       "Memory Request(MB)",
                                       "Memory Limit(MB)",
                                       "Avg Memory Usage(MB)",
                                       "Median(MB)"
                                       "90%tile Memory Usage(MB)",
                                       "95%tile Memory Usage(MB)",
                                       "99%tile Memory Usage(MB)",
                                       "Max Memory Usage(MB)",
                                       "Suggested Request(MB)"
                                       ])
    df.sort_values(['Namespace'])
    return df


def calculate_skewness(node_compute_to_memory_ratio, pod_compute_to_memory_ratio):
    return abs(node_compute_to_memory_ratio - pod_compute_to_memory_ratio) / node_compute_to_memory_ratio


def get_approximate_ratio(ratio):
    if ratio > 1:
        return f"{round(ratio)}:1"
    elif ratio != 0:
        return f"1:{round(1 / ratio)}"
    else:
        return None


def flag_pods_by_wrong_node_placement_by_requests(logger,pod_data, node_data, threshold):
    _table = []
    for _, pod in pod_data.items():
        try:
            node = node_data[pod.node_name]
            node_compute_to_memory_ratio = (node.cpu_limit / node.memory_limit) * 1000
            if pod.cpu_request == 0 or pod.memory_request == 0:
                continue
            pod_compute_to_memory_ratio = (pod.cpu_request / pod.memory_request) * 1000
            pod_skewness = calculate_skewness(node_compute_to_memory_ratio, pod_compute_to_memory_ratio)

            if pod_skewness > threshold:
                _table.append(
                    [
                        pod.namespace,
                        pod.pod_name,
                        node.node_name,
                        node.instance_type,
                        node_compute_to_memory_ratio,
                        pod_compute_to_memory_ratio,
                        get_approximate_ratio(pod_compute_to_memory_ratio)
                    ])
        except Exception as e:
            logger.error(f"Error flagging pods for wrong cpu requests for namespace:{pod.namespace},pod:{pod.pod_name}",e)

    df = pd.DataFrame(_table, columns=[
        "namespace",
        "pod_name",
        "node_name",
        "node instance type",
        "node cpu/memory",
        "pod cpu/memory",
        "Approx. pod cpu:memory as integer ratio"
    ])
    df.sort_values(['namespace'])
    return df
