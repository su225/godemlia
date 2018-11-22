# This file is used inside docker to run the kademlia node
# This must be placed in the same directory as the binary

# RPC_PORT = Port used by this node to talk to other nodes
# REST_PORT = Port on which the REST server is listening
# CONCURRENCY = Concurrency factor of the node
# REPLICATION = Replication factor of the node
# LISTEN_ADDR = Listening address of this node
# JOIN_ADDR = Address of another node through which to join
# BOOTSTRAP = Start node in bootstrap node.
# NODE_BIN_PATH = Path of the node binary
# NODE_ID = NodeID of this node

if [ $BOOTSTRAP = true ]; then
    echo 'Starting node in bootstrap mode'
    $NODE_BIN_PATH/server --bootstrap --id=$NODE_ID --ip=$IP \
        --port=$RPC_PORT --rest-port=$REST_PORT \
        --concurrency=$CONCURRENCY --replication=$REPLICATION
else
    echo 'Starting node in non-bootstrap mode'
    $NODE_BIN_PATH/server --id=$NODE_ID --ip=$IP \
        --port=$RPC_PORT --rest-port=$REST_PORT \
        --concurrency=$CONCURRENCY --replication=$REPLICATION \
        --join=$JOIN_ADDR
fi