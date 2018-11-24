# This file is used inside docker to run the kademlia node
# This must be placed in the same directory as the binary

echo 'Starting a new Kademlia node'

# RPC_PORT = Port used by this node to talk to other nodes
# REST_PORT = Port on which the REST server is listening
# CONCURRENCY = Concurrency factor of the node
# REPLICATION = Replication factor of the node
# JOIN_ADDR = Address of another node through which to join
# BOOTSTRAP = Start node in bootstrap node.
# NODE_BIN_PATH = Path of the node binary
# NODE_ID = NodeID of this node

# If any of the required environment variables are not specified
# then default them to certain values as much as possible.
DEFAULT_NODE_BIN_PATH=.
DEFAULT_RPC_PORT=6666
DEFAULT_REST_PORT=6667

DEFAULT_REPLICATION_LEVEL=10
DEFAULT_CONCURRENCY_LEVEL=4

if [[ -z "${NODE_BIN_PATH}" ]]; then
    echo "NODE_BIN_PATH is empty. Setting it to ${DEFAULT_NODE_BIN_PATH}"
    NODE_BIN_PATH=$DEFAULT_NODE_BIN_PATH
fi

if [[ -z "${RPC_PORT}" ]]; then
    echo "RPC_PORT is not specified. Setting it to ${DEFAULT_RPC_PORT}"
    RPC_PORT=$DEFAULT_RPC_PORT
fi

if [[ -z "${REST_PORT}" ]]; then
    echo "REST_PORT is not specified. Setting it to ${DEFAULT_REST_PORT}"
    REST_PORT=$DEFAULT_REST_PORT
fi

if [[ -z "${REPLICATION}" ]]; then
    echo "REPLICATION level is not specified. Setting it to ${DEFAULT_REPLICATION_LEVEL}"
    REPLICATION=$DEFAULT_REPLICATION_LEVEL
fi

if [[ -z "${CONCURRENCY}" ]]; then
    echo "CONCURRENCY level is not specified. Defaulting it to ${DEFAULT_CONCURRENCY_LEVEL}"
    CONCURRENCY=$DEFAULT_CONCURRENCY_LEVEL
fi

NODE_ARGS="--bootstrap --port=$RPC_PORT --rest-port=$REST_PORT"
NODE_ARGS="$NODE_ARGS --concurrency=$CONCURRENCY"
NODE_ARGS="$NODE_ARGS --replication=$REPLICATION"

if ! [[ -z "${K8S_FLAG}" ]]; then
    echo "Running in Kubernetes environment. So join address if specified is not necessary"
    NODE_ARGS="$NODE_ARGS --k8s"

    # Check if the Pod IP is specified through K8S_POD_IP environment variable
    # If it is not specified then it is an error
    if ! [[ -z "${K8S_POD_IP}" ]]; then
        echo 'Pod IP must be specified in K8S_POD_IP'
        exit 1
    else
        NODE_ARGS="$NODE_ARGS --k8s-pod-ip=$K8S_POD_IP"
    fi
else
    echo "Node is not running in Kubernetes environment"
    if [[ -z $JOIN_ADDR ]]; then
        echo "Join address is not specified. So starting in bootstrap mode"
        NODE_ARGS="$NODE_ARGS --bootstrap"
    else
        echo "Join address is specified as $JOIN_ADDR. Appending it to args"
        NODE_ARGS="$NODE_ARGS --join=$JOIN_ADDR"
    fi
fi

# Launch the node
echo "Launcing the node with parameters $NODE_ARGS"
$NODE_BIN_PATH/server $NODE_ARGS