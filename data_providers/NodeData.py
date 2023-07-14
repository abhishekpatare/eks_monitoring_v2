class NodeData:
    def __init__(self, node_name):
        self.node_name = node_name
        self.instance_type = None
        self.cpu_limit = None
        self.memory_limit = None
        self.cpu_usage = None
        self.memory_usage = None
        self.network_rx_bytes = None
        self.network_tx_bytes = None
        self.disk_total_bytes = None
        self.network_bandwidth_limit = None
        self.ebs_baseline_bandwidth = None
