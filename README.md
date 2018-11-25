# Godemlia
The Distributed Hash Table based on Kademlia.

You can read the original paper [here](https://pdos.csail.mit.edu/~petar/papers/maymounkov-kademlia-lncs.pdf)

There are deviations from the original paper. TCP is used instead of UDP. Further, the republish timeout has been tweaked
and in future it will be made configurable at the start of the node. It will be part of the node configuration received by
the joining node.

I wanted to implement peer auto-discovery on Kubernetes environment. That is still incomplete.

## Future improvements and enhancements
* Deploy on Kubernetes cluster with peer auto-discovery.
* Implement delete functionality.
* Implement graceful shutdown of the node.
* Make it more testable and improve test coverage.
* Make GC and key republication intervals configurable.
* Make number of retry attempts configurable.
* Optimize data republishing functionality.
* Optimize garbage collection of stale data.
* Optimize data storage and optionally allow writing to disk.
* Refactor - put all configuration within a single configuration object instead of throwing them around all over the place.
* Break up the service package in src/server into smaller pieces.


## Building and testing
The following series of commands can be used to start a cluster locally. This will start a cluster with port 6666 being used for communication between various nodes and 6667 is for the client to get/put the data.
```shell
# Build the binary
make build-server
cd src/server

# Start the node in bootstrap mode.
./server --bootstrap --port=6666 --rest-port=6667 --concurrency=5 --replication=3
```

This builds the server part and all tests are run
```
make server-test
```

Building and running a Docker image
```shell
make build-docker-container

# Run docker container in bootstrap mode
docker run kademlia:local

# Run docker container in non-bootstrap mode. Make sure join address is specified. Address of other node must be the external IP of that node. For now, only IPv4 addresses are supported.
docker run -e JOIN_ADDR=<hostport-other-node> kademlia:local
```

How to obtain the value of a key? Have a look at the below example where the value of key 123 is queried.
```
curl http://node-ip:rest-port/data/123
```

How to store a key value pair? Send a POST on /data endpoint in any of the cluster nodes. The below example sends a request to the node running on node-ip:port to store the key-value pair 123:sample
```
curl -v --header "Content-Type: application/json" --request POST --data '{"key": 123, "value": "sample"}' http://node-ip:rest-port/data

```