import logging
import time

import boto3
import prometheus_api_client

import config
from data_providers.NodeDataProvider import NodeDataProvider
from data_providers.PodDataProvider import PodDataProvider
from main import get_start_and_end_time, get_ec2_client, get_prometheus_client


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
def get_data_providers(start, end):
    prometheus_api = get_prometheus_client(config.PROMETHEUS_URL)
    ec2_client = get_ec2_client()
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


logger = get_logger()
start, end = get_start_and_end_time(config.TIMEDELTA)
node_data_provider, pod_data_provider = get_data_providers(start, end)
node_data = node_data_provider.get_data()
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.cpu_limit))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.memory_limit))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.instance_type))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.network_bandwidth_limit))
for _,node in node_data.items():
    logger.debug((node.node_name , node.ebs_baseline_bandwidth))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.cpu_usage))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.memory_usage))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.network_rx_bytes))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.network_tx_bytes))
# for _,node in node_data.items():
#     logger.debug((node.node_name , node.disk_total_bytes))
