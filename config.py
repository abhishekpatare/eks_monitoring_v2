# Prometheus server endpoint
PROMETHEUS_URL = ''
AWS_REGION = 'us-east-1'
CLUSTER_NAME = ""
# time delta in minutes
TIMEDELTA = 30
# time delta for rate queries in promql
# it is time over which rate is calculated in minutes
RATE_DELTA = 5

# minimum duration between two timestamps in seconds
STEP = 30
# Multiplication factor of stdev used to calculate expected cpu and memory request
# expected request = avg usage + POD_REQUEST_MARGIN_FACTOR*stdev usage
POD_REQUEST_MARGIN_FACTOR = 1

# Threshold on how much the cpu request/memory request ratio of pod differ from the cpu/memory ratio of the node on
# which it is scheduled
# lower the value tighter the bound
POD_SKEWNESS_THRESHOLD = 0.4

# threshold for how much the actual request differ compared to the suggested or expected request
REQUEST_DIFFERENCE_THRESHOLD = 0.5

NODE_CPU_UTILIZATION_THRESHOLD = 0.5  # Node cpu
NODE_MEMORY_UTILIZATION_THRESHOLD = 0.7  # Node memory
NODE_RX_BYTES_USAGE_THRESHOLD = 0  # Node network received bytes
NODE_TX_BYTES_USAGE_THRESHOLD = 0  # Node network transmitted bytes
NODE_DISK_BYTES_USAGE_THRESHOLD = 0

# Threshold limits on occurrences of high usage of respective resources
NODE_CPU_HIGH_UTIL_EXP_PROB = 0.05
NODE_MEMORY_HIGH_UTIL_EXP_PROB = 0.05
NODE_NETWORK_BYTES_PROB_LIMIT = 0.005
NODE_DISK_TOTAL_BYTES_PROB_LIMIT = 0.005

MIN_WINDOW_DIFF = 300  # Minimum difference between two consecutive time windows of high usage in seconds
MAX_WINDOW_SIZE = 30  # 30 min

NETWORK_BANDWIDTH_FILE_PATH = 'data_providers/network_bandwidths'  # Path to file containing
OUTPUT_FILE_PATH = './report.xlsx'

LOG_FILE_PATH = "./logs.log"
