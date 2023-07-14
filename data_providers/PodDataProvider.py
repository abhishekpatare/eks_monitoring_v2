import pandas as pd
import re
import config
from data_providers.PodData import PodData


class PodDataProvider:
    def __init__(self, prometheus_api, start_time, end_time, step, rate_delta, logger):
        self.prometheus_api = prometheus_api
        self.start_time = start_time
        self.end_time = end_time
        self.step = step
        self.rate_delta = rate_delta
        self.logger = logger

    def get_data(self):
        pod_data = {}
        self.get_pod_cpu_request(pod_data)
        self.get_pod_memory_request(pod_data)
        self.get_pod_cpu_limits(pod_data)
        self.get_pod_memory_limits(pod_data)
        self.get_pod_cpu_data(pod_data)
        self.get_pod_memory_data(pod_data)
        self.get_pod_network_rx_bytes(pod_data)
        self.get_pod_network_tx_bytes(pod_data)
        self.get_pod_disk_total_bytes(pod_data)
        return pod_data

    def _prometheus_query(self, query):
        res = self.prometheus_api.custom_query_range(
            query=query,
            start_time=self.start_time,
            end_time=self.end_time,
            step=self.step
        )
        return res

    def _parse_pod_res(self, _data):
        # self.logger.debug(_data['metric'])
        if 'namespace' not in _data['metric']:
            return None, None, None
        if 'pod' not in _data['metric']:
            return None, None, None
        if 'node' not in _data['metric']:
            return None, None, None
        namespace = _data['metric']['namespace']
        pod_name = _data['metric']['pod']
        node_name = re.sub(".ec2.internal", "", _data['metric']['node'])
        return namespace, pod_name, node_name

    def _parse_pod_res1(self, _data):
        # self.logger.debug(_data['metric'])
        if 'namespace' not in _data['metric']:
            return None, None, None
        if 'pod' not in _data['metric']:
            return None, None, None
        if 'instance' not in _data['metric']:
            return None, None, None
        namespace = _data['metric']['namespace']
        pod_name = _data['metric']['pod']
        node_name = re.sub(".ec2.internal", "", _data['metric']['instance'])
        return namespace, pod_name, node_name

    def get_pod_cpu_request(self, pod_data):
        try:
            pod_cpu_request_res = self._prometheus_query(
                "sum(kube_pod_container_resource_requests{resource='cpu',pod!='POD'})by(pod,namespace,node)")
            for _data in pod_cpu_request_res:
                namespace, pod_name, node_name = self._parse_pod_res(_data)
                if namespace is None:
                    # self.logger.debug("Null namesace")
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.cpu_request = float(_data['values'][0][1])
        except Exception as e:
            self.logger.error("Error while getting pod cpu request", e)

    def get_pod_memory_request(self, pod_data):
        try:
            pod_memory_request_res = self._prometheus_query(
                "(sum(kube_pod_container_resource_requests{resource='memory',pod!='POD'})by(pod,namespace,node))/1000000")
            for _data in pod_memory_request_res:
                namespace, pod_name, node_name = self._parse_pod_res(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_request = float(_data['values'][0][1])

        except Exception as e:
            self.logger.error("Error getting pod memory request", e)

    def get_pod_cpu_limits(self, pod_data):
        try:
            pod_cpu_limit_res = self._prometheus_query(
                "sum(kube_pod_container_resource_limits{resource='cpu',pod!='POD'})by(pod,namespace,node)")
            for _data in pod_cpu_limit_res:
                namespace, pod_name, node_name = self._parse_pod_res(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.cpu_limit = float(_data['values'][0][1])
        except Exception as e:
            self.logger.error("Error getting pod cpu limits", e)

    def get_pod_memory_limits(self, pod_data):
        try:
            pod_memory_limit_res = self._prometheus_query(
                "(sum(kube_pod_container_resource_limits{resource='memory',pod!='POD'})by(pod,namespace,node))/1000000")
            for _data in pod_memory_limit_res:
                namespace, pod_name, node_name = self._parse_pod_res(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_limit = float(_data['values'][0][1])
        except Exception as e:
            self.logger.error("Error getting pod memory limits", e)

    def get_pod_cpu_data(self, pod_data):
        try:
            pod_cpu_res = self._prometheus_query(
                f"sum(rate(container_cpu_usage_seconds_total{{namespace!='',pod!='',pod!='POD',instance!=''}}[{self.rate_delta}m])) by(namespace,pod,instance)")
            for _data in pod_cpu_res:
                namespace, pod_name, node_name = self._parse_pod_res1(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.cpu_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.cpu_usage = pod.cpu_usage.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting pod cpu usage", e)

    def get_pod_memory_data(self, pod_data):
        try:
            pod_memory_res = self._prometheus_query(
                "(sum(container_memory_usage_bytes{namespace!='',pod!='',pod!='POD',instance!=''}) by (pod,namespace,instance))/1000000")
            for _data in pod_memory_res:
                namespace, pod_name, node_name = self._parse_pod_res1(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.memory_usage = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.memory_usage = pod.memory_usage.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting pod memory usage", e)

    def get_pod_network_rx_bytes(self, pod_data):
        try:
            pod_rx_bytes_res = self._prometheus_query(
                f"(sum(rate(container_network_receive_bytes_total{{namespace!='',pod!='',pod!='POD',instance!=''}}[{self.rate_delta}m]))by (namespace,pod,instance))/1000000")
            for _data in pod_rx_bytes_res:
                namespace, pod_name, node_name = self._parse_pod_res1(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.network_rx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.network_rx_bytes = pod.network_rx_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting pod rx bytes", e)

    def get_pod_network_tx_bytes(self, pod_data):
        try:
            pod_rx_bytes_res = self._prometheus_query(
                f"(sum(rate(container_network_transmit_bytes_total{{namespace!='',pod!='',pod!='POD',instance!=''}}[{self.rate_delta}m]))by (namespace,pod,instance))/1000000")
            for _data in pod_rx_bytes_res:
                namespace, pod_name, node_name = self._parse_pod_res1(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.network_tx_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.network_tx_bytes = pod.network_tx_bytes.astype({'timestamp': int, 'values': float})
        except:
            pass

    def get_pod_disk_total_bytes(self, pod_data):
        try:
            pod_disk_total_bytes_res = self._prometheus_query(
                f"(sum(rate(container_fs_reads_bytes_total{{namespace!='',pod!='',pod!='POD',instance!=''}}[{self.rate_delta}m])+rate(container_fs_writes_bytes_total{{namespace!='',pod!='',instance!=''}}[{self.rate_delta}m]))by (pod,namespace,instance))/1000000")
            for _data in pod_disk_total_bytes_res:
                namespace, pod_name, node_name = self._parse_pod_res1(_data)
                if namespace is None:
                    continue
                if (namespace, pod_name) not in pod_data:
                    pod_data[(namespace, pod_name)] = PodData(pod_name, namespace, node_name)
                pod = pod_data[(namespace, pod_name)]
                pod.disk_total_bytes = pd.DataFrame(_data['values'], columns=['timestamp', 'values'])
                pod.disk_total_bytes = pod.disk_total_bytes.astype({'timestamp': int, 'values': float})
        except Exception as e:
            self.logger.error("Error getting pod disk total bytes", e)
