import re
import pandas as pd

from data_providers.NodeData import NodeData
from data_providers.utils import Gb_to_MB


class NodeDataProvider:
    def __init__(self, prometheus_api, ec2_client, start_time, end_time, step, rate_deta, logger,
                 network_band_width_file):
        self.prometheus_api = prometheus_api
        self.ec2_client = ec2_client
        self.start_time = start_time
        self.end_time = end_time
        self.step = step
        self.rate_delta = rate_deta
        self.logger = logger
        self.network_bandwidth_file = network_band_width_file

    def _prometheus_query(self, query):
        res = self.prometheus_api.custom_query_range(
            query=query,
            start_time=self.start_time,
            end_time=self.end_time,
            step=self.step
        )
        return res

    def _parse_node_name(self, _data):
        if 'node' not in _data['metric']:
            return None
        node_name = re.sub(".ec2.internal", "", _data['metric']['node'])
        return node_name

    def get_data(self):
        node_data = {}
        self.get_node_cpu_capacity(node_data)
        self.get_node_memory_capacity(node_data)
        self.get_node_instance_type(node_data)
        self.get_node_network_bandwidths(node_data)
        self.get_node_ebs_bandwidths(node_data)
        self.get_cpu_usage_data(node_data)
        self.get_memory_usage_data(node_data)
        self.get_node_network_rx_bytes(node_data)
        self.get_node_network_tx_bytes(node_data)
        self.get_node_disk_total_bytes(node_data)
        return node_data

    def get_node_cpu_capacity(self, node_data):
        try:
            node_cpu_cap_res = self._prometheus_query("kube_node_status_capacity{resource='cpu'}")
            for _data in node_cpu_cap_res:
                node_name = self._parse_node_name(_data)
                if node_name is None:
                    continue
                if node_name not in node_data:
                    node_data[node_name] = NodeData(node_name)
                node = node_data[node_name]
                node.cpu_limit = float(_data['values'][0][1])
        except Exception as e:
            self.logger.error("Error getting node cpu capacity", e)

    def get_node_memory_capacity(self, node_data):
        try:
            node_cpu_cap_res = self._prometheus_query("(kube_node_status_capacity{resource='memory'})/1000000")
            for _data in node_cpu_cap_res:
                node_name = self._parse_node_name(_data)
                if node_name is None:
                    continue
                node = node_data[node_name]
                node.memory_limit = float(_data['values'][0][1])
        except Exception as e:
            self.logger.error("Error getting node memory capacity", e)

    def get_node_instance_type(self, node_data):
        try:
            node_instance_type_res = self._prometheus_query("kube_node_labels")
            for _data in node_instance_type_res:
                node_name = self._parse_node_name(_data)
                if node_name is None:
                    continue
                if node_name not in node_data:
                    self.logger.info(f"Instance type not known for node: {node_name}")
                    continue
                node = node_data[node_name]
                node.instance_type = _data['metric']['label_node_kubernetes_io_instance_type']
        except Exception as e:
            self.logger.error("Error getting  instance type", e)

    def get_node_network_bandwidths(self, node_data):
        try:
            bandwidth_map = self.get_network_bandwidth_map()
            for _, node in node_data.items():
                if node.instance_type not in bandwidth_map:
                    self.logger.info(f"Network bandwidth not availabe for instance type: {node.instance_type}")
                    continue
                node.network_bandwidth_limit = bandwidth_map[node.instance_type]
        except Exception as e:
            self.logger.error("Error getting network bandwidth for node", e)

    def get_network_bandwidth_map(self):
        try:
            bandwidth_map = {}
            file = open(
                "/Users/abhishek.patare/PycharmProjects/prometheus_monitoring_v2/data_providers/network_bandwidths",
                'r')
            file.readline()
            while True:
                line = file.readline()
                if line:
                    data = line.split(sep=',')
                    bandwidth_map[data[0]] = Gb_to_MB(float(data[1]))
                else:
                    break
            return bandwidth_map
        except Exception as e:
            self.logger.error("Error getting network bandwidth for node", e)
            return {}

    def get_node_ebs_bandwidths(self, node_data):
        try:
            instance_types_list = self._get_instance_types_list(node_data)
            ebs_bandwidth_map = self._get_ebs_bandwidth_map(instance_types_list)
            for _, node in node_data.items():
                if node.instance_type not in ebs_bandwidth_map:
                    self.logger.info(f"EBS bandwidth not available for instance type: {node.instance_type}")
                    continue
                node.ebs_baseline_bandwidth = ebs_bandwidth_map[node.instance_type]
        except Exception as e:
            self.logger.error("Error getting node ebs bandwidth", e)

    def _get_instance_types_list(self, node_data):
        instance_type_set = set({})
        for _, node in node_data.items():
            if node.instance_type is None:
                self.logger.info(f"Instance type not known for node : {node.node_name}")
                continue
            instance_type_set.add(node.instance_type)
        return list(instance_type_set)

    def _get_ebs_bandwidth_map(self, instance_type_list):
        try:
            res = self.ec2_client.describe_instance_types(InstanceTypes=instance_type_list)
            ebs_bandwidth_map = {}
            for e in res['InstanceTypes']:
                if 'EbsInfo' in e and 'EbsOptimizedInfo' in e['EbsInfo']:
                    ebs_bandwidth_map[e['InstanceType']] = e['EbsInfo']['EbsOptimizedInfo']['BaselineBandwidthInMbps']
            return ebs_bandwidth_map
        except Exception as e:
            self.logger.error("Error getting ebs bandwidths", e)
            return {}

    def get_cpu_usage_data(self, node_data):
        try:
            node_cpu_usage_res = self._prometheus_query(
                f"sum(rate(node_cpu_seconds_total{{mode!='idle',mode!='iowait',mode!='steal'}}[{self.rate_delta}m]))by(instance)*on(instance)group_left(nodename) node_uname_info")
            for _data in node_cpu_usage_res:
                # node_name = self._parse_node_name(_data,'instance')
                if 'nodename' not in _data['metric']:
                    continue
                node_name = _data['metric']['nodename']
                if node_name is None:
                    continue

                node = node_data[node_name]
                node.cpu_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.cpu_usage = node.cpu_usage.astype({'timestamp': int, 'values': float})

        except Exception as e:
            self.logger.error("Error getting node cpu usage", e)

    def get_memory_usage_data(self, node_data):
        try:
            node_memory_usage_res = self._prometheus_query(
                "((node_memory_MemTotal_bytes - node_memory_MemFree_bytes- node_memory_Buffers_bytes - node_memory_Cached_bytes)*on(instance)group_left(nodename) node_uname_info)/1000000")
            for _data in node_memory_usage_res:
                # node_name = self._parse_node_name(_data)
                if 'nodename' not in _data['metric']:
                    continue
                node_name = _data['metric']['nodename']
                if node_name is None:
                    continue
                node = node_data[node_name]
                node.memory_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.memory_usage = node.memory_usage.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting node memory usage", e)

    def get_node_network_rx_bytes(self, node_data):
        try:
            node_network_rx_bytes_res = self._prometheus_query(
                f"(sum(rate(node_network_receive_bytes_total[{self.rate_delta}m])) by (instance)*on(instance)group_left(nodename) node_uname_info)/1000000")
            for _data in node_network_rx_bytes_res:
                # node_name = self._parse_node_name(_data)
                if 'nodename' not in _data['metric']:
                    continue
                node_name = _data['metric']['nodename']
                if node_name is None:
                    continue
                node = node_data[node_name]
                node.network_rx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.network_rx_bytes = node.network_rx_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting node rx bytes", e)

    def get_node_network_tx_bytes(self, node_data):
        try:
            node_network_tx_bytes_res = self._prometheus_query(
                f"(sum(rate(node_network_transmit_bytes_total[{self.rate_delta}m])) by (instance)*on(instance)group_left(nodename) node_uname_info)/1000000")
            for _data in node_network_tx_bytes_res:
                # node_name = self._parse_node_name(_data)
                if 'nodename' not in _data['metric']:
                    continue
                node_name = _data['metric']['nodename']
                if node_name is None:
                    continue
                node = node_data[node_name]
                node.network_tx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.network_tx_bytes = node.network_tx_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting node tx bytes", e)

    def get_node_disk_total_bytes(self, node_data):
        try:
            node_disk_total_bytes_res = self._prometheus_query(
                f"(sum(rate(node_disk_written_bytes_total{{device=~'nvme...'}}[{self.rate_delta}m]) + rate(node_disk_read_bytes_total{{device=~'nvme...'}}[{self.rate_delta}m]))by(instance)*on(instance)group_left(nodename) node_uname_info)/1000000")
            for _data in node_disk_total_bytes_res:
                if 'nodename' not in _data['metric']:
                    continue
                node_name = _data['metric']['nodename']
                if node_name is None:
                    continue
                node = node_data[node_name]
                node.disk_total_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                node.disk_total_bytes = node.disk_total_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting node disk total bytes", e)
