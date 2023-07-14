import argparse
import datetime
import os
import time
import prometheus_api_client
import boto3
import config
import logging
import pandas as pd

from Flaggers.CupritIdentifiers import mark_culprit_pods_for_high_cpu, mark_culprit_pods_for_high_memory, \
    mark_culprit_pods_for_high_rx_bytes
from Flaggers.NodeCPUFlaggers import flag_nodes_by_high_probability_of_high_cpu_utilization, \
    flag_nodes_by_high_avg_cpu_utilization
from Flaggers.NodeDiskBytesTotal import flag_nodes_by_high_probability_of_high_disk_total_bytes, \
    flag_nodes_by_high_avg_disk_total_bytes
from Flaggers.NodeMemoryFlaggers import flag_nodes_by_high_probability_of_high_memory_utilization, \
    flag_nodes_by_high_avg_memory_utilization
from Flaggers.NodeRXBytesFlagger import flag_nodes_by_high_probability_of_high_network_rx_bytes, \
    flag_nodes_by_high_avg_network_rx_bytes
from Flaggers.NodeTXBytesFlagger import flag_nodes_by_high_probability_of_high_network_tx_bytes, \
    flag_nodes_by_high_avg_network_tx_bytes
from Flaggers.PodFlaggers import flag_pods_by_wrong_node_placement_by_requests, flag_pods_for_wrong_cpu_requests, \
    flag_pods_for_wrong_memory_requests
from data_providers.NodeDataProvider import NodeDataProvider
from data_providers.PodDataProvider import PodDataProvider


def get_logger(logs_output_file_path='./logs.log'):
    try:
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        logging.basicConfig(
            # filename=logs_output_file_path,
            # filemode="w",
            level=logging.DEBUG,
            format="%(asctime)s,%(levelname)-8s [%(filename)s:%(lineno)d] %(message)s",
            datefmt="%Y-%m-%d:%H:%M:%S",
        )
        logger.info("created at " + str(time.ctime()))
        return logger
    except Exception as ex:
        print(ex)
        print("Failed to create log file: might be due to invalid path")
        exit(1)


def get_start_and_end_time(timeDelta):
    end = datetime.datetime.now()
    start = end - datetime.timedelta(minutes=timeDelta)
    return start, end


def get_prometheus_client(prometheus_endpoint):
    try:
        prometheus_api = prometheus_api_client.PrometheusConnect(prometheus_endpoint)
        return prometheus_api
    except Exception as e:
        exit(0)


def get_ec2_client(region_name):
    ec2_client = boto3.client('ec2', region_name=region_name)
    return ec2_client


def get_data_providers(start, end):
    prometheus_api = get_prometheus_client(config.PROMETHEUS_URL)
    ec2_client = get_ec2_client(config.AWS_REGION)
    node_data_provider = NodeDataProvider(
        prometheus_api,
        ec2_client,
        start,
        end,
        config.STEP,
        config.RATE_DELTA,
        logger,
        config.NETWORK_BANDWIDTH_FILE_PATH)
    pod_data_provider = PodDataProvider(
        prometheus_api,
        start,
        end,
        config.STEP,
        config.RATE_DELTA,
        logger)
    return node_data_provider, pod_data_provider


def create_report_info(report_writer, start, end):
    info = [
        ["cluster", config.CLUSTER_NAME],
        ["prometheus endpoint", config.PROMETHEUS_URL],
        ["start time", start],
        ["end time", end]
    ]
    df = pd.DataFrame(info)
    df.to_excel(report_writer, sheet_name="report info", index=False)


def create_wrong_pod_placement_report(report_writer, pod_data, node_data, logger):
    wrong_pod_placement_report = flag_pods_by_wrong_node_placement_by_requests(logger, pod_data, node_data,
                                                                               config.POD_SKEWNESS_THRESHOLD)
    wrong_pod_placement_report.to_excel(report_writer, sheet_name="Pod placement report", index=False)


def create_pod_cpu_usage_vs_request_report(report_writer, pod_data, logger):
    pod_cpu_report = flag_pods_for_wrong_cpu_requests(logger, pod_data, config.POD_REQUEST_MARGIN_FACTOR,
                                                      config.REQUEST_DIFFERENCE_THRESHOLD)
    pod_cpu_report.to_excel(report_writer, sheet_name="Pod CPU Usage vs Request", index=False)


def create_pod_memory_usage_vs_request_report(report_writer, pod_data, logger):
    pod_memory_report = flag_pods_for_wrong_memory_requests(logger, pod_data, config.POD_REQUEST_MARGIN_FACTOR,
                                                            config.REQUEST_DIFFERENCE_THRESHOLD)
    pod_memory_report.to_excel(report_writer, sheet_name="Pod Memory Usage vs Request", index=False)


def group_pods_by_nodes(pod_data):
    node_pod_dict = {}
    for pod_name in pod_data.keys():
        if pod_data[pod_name].node_name not in node_pod_dict:
            node_pod_dict[pod_data[pod_name].node_name] = []
        node_pod_dict[pod_data[pod_name].node_name].append(pod_data[pod_name])
    return node_pod_dict


def create_bad_nodes_by_high_cpu_report(report_writer, node_data, node_pod_dict, logger):
    bad_nodes_by_high_occurrence_of_high_cpu_usage = flag_nodes_by_high_probability_of_high_cpu_utilization(logger,
                                                                                                            node_data,
                                                                                                            config.NODE_CPU_UTILIZATION_THRESHOLD,
                                                                                                            config.NODE_CPU_HIGH_UTIL_EXP_PROB)
    bad_nodes_by_high_avg_cpu_usage = flag_nodes_by_high_avg_cpu_utilization(logger,
                                                                             node_data,
                                                                             config.NODE_CPU_UTILIZATION_THRESHOLD)
    bad_nodes = pd.concat([bad_nodes_by_high_occurrence_of_high_cpu_usage, bad_nodes_by_high_avg_cpu_usage])

    bad_nodes.drop_duplicates(inplace=True)

    bad_nodes.to_excel(report_writer, sheet_name="High CPU nodes", index=False)
    possible_culprit_pods = mark_culprit_pods_for_high_cpu(bad_nodes,
                                                           node_pod_dict,
                                                           node_data,
                                                           config.MIN_WINDOW_DIFF,
                                                           config.MAX_WINDOW_SIZE,
                                                           config.NODE_CPU_UTILIZATION_THRESHOLD)
    possible_culprit_pods.to_excel(report_writer, sheet_name="Culprits for high cpu nodes", index=False)


def create_bad_nodes_by_high_memory_report(report_writer, node_data, node_pod_dict, logger):
    bad_nodes_by_high_occurrence_of_high_memory_usage = flag_nodes_by_high_probability_of_high_memory_utilization(
        logger,
        node_data,
        config.NODE_MEMORY_UTILIZATION_THRESHOLD,
        config.NODE_MEMORY_HIGH_UTIL_EXP_PROB)
    bad_nodes_by_high_avg_memory_usage = flag_nodes_by_high_avg_memory_utilization(logger,
                                                                                   node_data,
                                                                                   config.NODE_MEMORY_UTILIZATION_THRESHOLD)
    bad_nodes = pd.concat([bad_nodes_by_high_occurrence_of_high_memory_usage, bad_nodes_by_high_avg_memory_usage])
    bad_nodes.drop_duplicates(inplace=True)
    bad_nodes.to_excel(report_writer, sheet_name="High Memory Util Nodes", index=False)

    possible_culprit_pods = mark_culprit_pods_for_high_memory(bad_nodes,
                                                              node_pod_dict,
                                                              node_data,
                                                              config.MIN_WINDOW_DIFF,
                                                              config.MAX_WINDOW_SIZE,
                                                              config.NODE_MEMORY_UTILIZATION_THRESHOLD)
    possible_culprit_pods.to_excel(report_writer, sheet_name="Culprits for high memory nodes", index=False)


def create_bad_nodes_by_high_rx_bytes_report(report_writer, node_data, node_pod_dict, logger):
    bad_nodes_by_high_occurrence_of_high_rx_bytes_usage = flag_nodes_by_high_probability_of_high_network_rx_bytes(
        logger,
        node_data,
        config.NODE_RX_BYTES_USAGE_THRESHOLD,
        config.NODE_NETWORK_BYTES_PROB_LIMIT)
    bad_nodes_by_high_avg_rx_bytes_usage = flag_nodes_by_high_avg_network_rx_bytes(logger,
                                                                                   node_data,
                                                                                   config.NODE_RX_BYTES_USAGE_THRESHOLD)
    bad_nodes = pd.concat([bad_nodes_by_high_occurrence_of_high_rx_bytes_usage, bad_nodes_by_high_avg_rx_bytes_usage])
    bad_nodes.drop_duplicates(inplace=True)
    bad_nodes.to_excel(report_writer, sheet_name="High RX bytes nodes", index=False)
    possible_culprit_pods = mark_culprit_pods_for_high_rx_bytes(bad_nodes,
                                                                node_pod_dict,
                                                                node_data,
                                                                config.MIN_WINDOW_DIFF,
                                                                config.MAX_WINDOW_SIZE,
                                                                config.NODE_RX_BYTES_USAGE_THRESHOLD)
    possible_culprit_pods.to_excel(report_writer, sheet_name="Culprit pods for high rx bytes", index=False)


def create_bad_nodes_by_high_tx_bytes_report(report_writer, node_data, node_pod_dict, logger):
    bad_nodes_by_high_occurrence_of_high_tx_bytes_usage = flag_nodes_by_high_probability_of_high_network_tx_bytes(
        logger,
        node_data,
        config.NODE_TX_BYTES_USAGE_THRESHOLD,
        config.NODE_NETWORK_BYTES_PROB_LIMIT)
    bad_nodes_by_high_avg_tx_bytes_usage = flag_nodes_by_high_avg_network_tx_bytes(logger,
                                                                                   node_data,
                                                                                   config.NODE_TX_BYTES_USAGE_THRESHOLD)
    bad_nodes = pd.concat([bad_nodes_by_high_occurrence_of_high_tx_bytes_usage, bad_nodes_by_high_avg_tx_bytes_usage])
    bad_nodes.drop_duplicates(inplace=True)
    bad_nodes.to_excel(report_writer, sheet_name="High TX bytes nodes", index=False)

    possible_culprit_pods = mark_culprit_pods_for_high_rx_bytes(bad_nodes,
                                                                node_pod_dict,
                                                                node_data,
                                                                config.MIN_WINDOW_DIFF,
                                                                config.MAX_WINDOW_SIZE,
                                                                config.NODE_TX_BYTES_USAGE_THRESHOLD)
    possible_culprit_pods.to_excel(report_writer, sheet_name="Culprits for high tx bytes", index=False)


def create_bad_nodes_by_high_disk_total_report(report_writer, node_data, node_pod_dict, logger):
    bad_nodes_by_high_occurrence_of_high_disk_total_bytes_usage = flag_nodes_by_high_probability_of_high_disk_total_bytes(
        logger,
        node_data,
        config.NODE_DISK_BYTES_USAGE_THRESHOLD,
        config.NODE_DISK_TOTAL_BYTES_PROB_LIMIT)
    bad_nodes_by_high_avg_disk_total_bytes_usage = flag_nodes_by_high_avg_disk_total_bytes(logger,
                                                                                           node_data,
                                                                                           config.NODE_DISK_BYTES_USAGE_THRESHOLD)
    bad_nodes = pd.concat(
        [bad_nodes_by_high_occurrence_of_high_disk_total_bytes_usage, bad_nodes_by_high_avg_disk_total_bytes_usage])
    bad_nodes.drop_duplicates(inplace=True)
    bad_nodes.to_excel(report_writer, sheet_name="High Disk total bytes nodes", index=False)

    possible_culprit_pods = mark_culprit_pods_for_high_rx_bytes(bad_nodes,
                                                                node_pod_dict,
                                                                node_data,
                                                                config.MIN_WINDOW_DIFF,
                                                                config.MAX_WINDOW_SIZE,
                                                                config.NODE_DISK_BYTES_USAGE_THRESHOLD)
    possible_culprit_pods.to_excel(report_writer, sheet_name="Culprits for high disk bytes", index=False)


if __name__ == '__main__':
    logger = get_logger()
    report_writer = pd.ExcelWriter(config.OUTPUT_FILE_PATH, engine='xlsxwriter')
    start, end = get_start_and_end_time(config.TIMEDELTA)
    node_data_provider, pod_data_provider = get_data_providers(start, end)
    node_data = node_data_provider.get_data()
    pod_data = pod_data_provider.get_data()
    create_report_info(report_writer,start,end)
    create_wrong_pod_placement_report(report_writer, pod_data, node_data, logger)
    create_pod_cpu_usage_vs_request_report(report_writer, pod_data, logger)
    create_pod_memory_usage_vs_request_report(report_writer, pod_data, logger)
    node_pod_dict = group_pods_by_nodes(pod_data)
    create_bad_nodes_by_high_cpu_report(report_writer, node_data, node_pod_dict, logger)
    create_bad_nodes_by_high_memory_report(report_writer, node_data, node_pod_dict, logger)
    create_bad_nodes_by_high_rx_bytes_report(report_writer, node_data, node_pod_dict, logger)
    create_bad_nodes_by_high_tx_bytes_report(report_writer, node_data, node_pod_dict, logger)
    create_bad_nodes_by_high_disk_total_report(report_writer, node_data, node_pod_dict, logger)
    report_writer.close()
