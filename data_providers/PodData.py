class PodData:
    def __init__(self,
                 pod_name=None,
                 namespace=None,
                 node_name=None,
                 memory_request=0,
                 cpu_request=0,
                 cpu_limit=float('inf'),
                 memory_limit=float("inf")):

        self.pod_name = pod_name
        self.namespace = namespace
        self.node_name = node_name

        self.cpu_request = cpu_request
        self.memory_request = memory_request
        self.cpu_usage = None
        self.memory_usage = None
        self.network_rx_bytes = None
        self.network_tx_bytes = None
        self.disk_total_bytes = None
        self.cpu_limit = cpu_limit
        self.memory_limit = memory_limit
