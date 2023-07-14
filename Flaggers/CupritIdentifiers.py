
import pandas as pd
import datetime

from Flaggers.utils import search_lb, search_rb, get_windows


def get_pods_windows( metric, win_st_ts, win_end_ts):
    if metric is None:
        return None
    if metric['timestamp'].iloc[0] > win_end_ts or metric['timestamp'].iloc[-1] < win_st_ts:
        return None
    l_idx = search_lb(metric['timestamp'], win_st_ts)
    r_idx = search_rb(metric['timestamp'], win_end_ts)
    win_df = metric['values'].iloc[l_idx:r_idx + 1]
    return win_df


def get_culprits(_pods, win_start, win_end):
    _culprits = []
    max_avg_pod = max(_pods, key=lambda x: x[1].mean())
    max_max_pod = max(_pods, key=lambda x: x[1].max())
    max_median_pod = max(_pods, key=lambda x: x[1].median())
    _culprits.append({'pod': max_avg_pod[0], 'win_df': max_avg_pod[1], "win_start": win_start, "win_end": win_end})
    _culprits.append({'pod': max_max_pod[0], 'win_df': max_max_pod[1], "win_start": win_start, "win_end": win_end})
    _culprits.append(
        {'pod': max_median_pod[0], 'win_df': max_median_pod[1], "win_start": win_start, "win_end": win_end})
    return _culprits


def get_report_from_culprits(_culprits):
    _table = []
    for culprit in _culprits:
        pod = culprit['pod']
        win_df = culprit['win_df']
        _table.append([
            pod.node_name,
            pod.namespace,
            pod.pod_name,
            win_df.mean(),
            win_df.median(),
            win_df.quantile(0.99),
            win_df.max(),
            culprit['win_start'],
            culprit['win_end']
        ])
    return _table


def mark_culprit_pods_for_high_cpu(bad_nodes, node_pod_dict, node_data, min_window_diff, max_win_size,
                                   threshold_fraction):
    _culprits = []
    for node_name in bad_nodes['node name']:
        node_item = node_data[node_name]
        threshold = threshold_fraction * node_item.cpu_limit
        bad_windows = get_windows(node_item.cpu_usage['values'], node_item.cpu_usage['timestamp'], threshold,
                                  min_window_diff, max_win_size)

        for win in bad_windows:
            win_start = str(datetime.datetime.fromtimestamp(win[0]))
            win_end = str(datetime.datetime.fromtimestamp(win[1]))
            win_st_ts, win_end_ts = win

            _pods = []
            for pod in node_pod_dict[node_name]:
                win_df = get_pods_windows(pod.cpu_usage, win_st_ts, win_end_ts)
                if win_df is not None:
                    _pods.append([pod, win_df])
            if len(_pods) == 0:
                continue
            curr_culprits = get_culprits(_pods, win_start, win_end)
            _culprits.extend(curr_culprits)

    df = pd.DataFrame(get_report_from_culprits(_culprits), columns=[
        "node name",
        "namespace",
        "pod name",
        "avg cpu usage in window",
        "median cpu usage in window",
        "99%tile cpu usage in window",
        "max cpu usage in window",
        "window start time",
        "window end time"
    ])
    df.drop_duplicates(inplace=True)
    df.sort_values(['namespace'])
    return df


def mark_culprit_pods_for_high_memory(bad_nodes, node_pod_dict, node_data, min_window_diff, max_win_size,
                                      threshold_fraction):
    _culprits = []
    for node_name in bad_nodes['node name']:
        node_item = node_data[node_name]
        threshold = node_item.memory_limit * threshold_fraction
        bad_windows = get_windows(node_item.memory_usage['values'], node_item.memory_usage['timestamp'], threshold,
                                  min_window_diff, max_win_size)
        for win in bad_windows:
            win_start = str(datetime.datetime.fromtimestamp(win[0]))
            win_end = str(datetime.datetime.fromtimestamp(win[1]))
            win_st_ts, win_end_ts = win

            _pods = []
            for pod in node_pod_dict[node_name]:
                win_df = get_pods_windows( pod.memory_usage, win_st_ts, win_end_ts)
                if win_df is not None:
                    _pods.append([pod, win_df])
            if len(_pods) == 0:
                continue
            curr_culprits = get_culprits(_pods, win_start, win_end)
            _culprits.extend(curr_culprits)
    df = pd.DataFrame(get_report_from_culprits(_culprits), columns=[
        "node name",
        "namespace",
        "pod name",
        "avg memory usage in window(MB)",
        "median memory usage in window(MB)",
        "99%tile memory usage in window(MB)",
        "max memory usage in window(MB)",
        "window start time",
        "window end time"
    ])
    df.drop_duplicates(inplace=True)
    df.sort_values(['namespace'])
    return df


def mark_culprit_pods_for_high_tx_bytes(bad_nodes, node_pod_dict, node_data, min_window_diff, max_win_size,
                                        threshold_fraction):
    _culprits = []
    for node_name in bad_nodes['node name']:
        node_item = node_data[node_name]
        threshold = node_item.network_bandwidth_limit * threshold_fraction
        bad_windows = get_windows(node_item.network_tx_bytes['values'], node_item.network_tx_bytes['timestamp'],
                                  threshold,
                                  min_window_diff, max_win_size)
        for win in bad_windows:
            win_start = str(datetime.datetime.fromtimestamp(win[0]))
            win_end = str(datetime.datetime.fromtimestamp(win[1]))
            win_st_ts, win_end_ts = win

            _pods = []
            for pod in node_pod_dict[node_name]:
                win_df = get_pods_windows(pod.network_tx_bytes, win_st_ts, win_end_ts)
                if win_df is not None:
                    _pods.append([pod, win_df])
            if len(_pods) == 0:
                continue
            curr_culprits = get_culprits(_pods, win_start, win_end)
            _culprits.extend(curr_culprits)
    df = pd.DataFrame(get_report_from_culprits(_culprits), columns=[
        "node name",
        "namespace",
        "pod name",
        "avg tx bytes in window(MB/s)",
        "median tx bytes in window(MB/s)",
        "99%tile tx bytes in window(MB/s)",
        "max tx bytes in window(MB/s)",
        "window start time",
        "window end time"
    ])
    df.drop_duplicates(inplace=True)
    df.sort_values(['namespace'])
    return df


def mark_culprit_pods_for_high_rx_bytes(bad_nodes, node_pod_dict, node_data, min_window_diff, max_win_size,
                                        threshold_fraction):
    _culprits = []
    for node_name in bad_nodes['node name']:
        node_item = node_data[node_name]
        threshold = node_item.network_bandwidth_limit * threshold_fraction
        bad_windows = get_windows(node_item.network_rx_bytes['values'], node_item.network_rx_bytes['timestamp'],
                                  threshold,
                                  min_window_diff, max_win_size)
        for win in bad_windows:
            win_start = str(datetime.datetime.fromtimestamp(win[0]))
            win_end = str(datetime.datetime.fromtimestamp(win[1]))
            win_st_ts, win_end_ts = win

            _pods = []
            for pod in node_pod_dict[node_name]:

                win_df = get_pods_windows(pod.network_rx_bytes, win_st_ts, win_end_ts)
                if win_df is not None:
                    _pods.append([pod, win_df])
            if len(_pods) == 0:
                continue
            curr_culprits = get_culprits(_pods, win_start, win_end)
            _culprits.extend(curr_culprits)
    df = pd.DataFrame(get_report_from_culprits(_culprits), columns=[
        "node name",
        "namespace",
        "pod name",
        "avg rx bytes in window(MB/s)",
        "median rx bytes in window(MB/s)",
        "99%tile rx bytes in window(MB/s)",
        "max rx bytes in window(MB/s)",
        "window start time",
        "window end time"
    ])
    df.drop_duplicates(inplace=True)
    df.sort_values(['namespace'])
    return df


def mark_culprit_pods_for_total_disk_bytes(bad_nodes, node_pod_dict, node_data, min_window_diff, max_win_size,
                                           threshold_fraction):
    _culprits = []
    for node_name in bad_nodes['node name']:
        node_item = node_data[node_name]
        threshold = node_item.ebs_baseline_bandwidth * threshold_fraction
        bad_windows = get_windows(node_item.disk_total_bytes['values'], node_item.disk_total_bytes['timestamp'],
                                  threshold,
                                  min_window_diff, max_win_size)
        for win in bad_windows:
            win_start = str(datetime.datetime.fromtimestamp(win[0]))
            win_end = str(datetime.datetime.fromtimestamp(win[1]))
            win_st_ts, win_end_ts = win

            _pods = []
            for pod in node_pod_dict[node_name]:
                win_df = get_pods_windows(pod.disk_total_bytes, win_st_ts, win_end_ts)
                if win_df is not None:
                    _pods.append([pod, win_df])
            if len(_pods) == 0:
                continue
            curr_culprits = get_culprits(_pods, win_start, win_end)
            _culprits.extend(curr_culprits)
    df = pd.DataFrame(get_report_from_culprits(_culprits), columns=[
        "node name",
        "namespace",
        "pod name",
        "avg disk total bytes in window(MB/s)",
        "median disk total bytes in window(MB/s)",
        "99%tile disk total bytes in window(MB/s)",
        "max disk total bytes in window(MB/s)",
        "window start time",
        "window end time"
    ])
    df.drop_duplicates(inplace=True)
    df.sort_values(['namespace'])
    return df

