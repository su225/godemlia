package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
)

// KademliaClient is used by client programs to connect
// to the cluster and get/put key-value pairs to them
type KademliaClient struct {
	// ClusterAddresses is the list of addresses that
	// the client can connect to
	ClusterAddresses []string
}

// CreateKademliaClient creates a new instance of Kademlia client.
// clusterNodeAddresses is the address of some of the nodes in the
// cluster to which the client application can connect to.
func CreateKademliaClient(clusterNodeAddresses []string) *KademliaClient {
	return &KademliaClient{ClusterAddresses: clusterNodeAddresses}
}

var (
	// ErrorKeyNotFound is thrown when the key is not present
	// in the Kademlia cluster.
	ErrorKeyNotFound = errors.New("Key not found")
)

// GetData gets the data for the given key from the cluster.
func (kc *KademliaClient) GetData(key uint64) (string, error) {
	var geterr error
	for _, address := range kc.ClusterAddresses {
		queryURL := fmt.Sprintf("%s/data/%d", address, key)
		req, err := http.NewRequest("GET", queryURL, nil)
		if err != nil {
			geterr = err
			continue
		}
		value, err := kc.doRequest(req)
		if err != nil {
			geterr = err
			continue
		}
		return value, nil
	}
	return "", fmt.Errorf("Cannot contact any nodes. Error=%s", geterr.Error())
}

// PostData is used to store a key-value pair in the cluster.
func (kc *KademliaClient) PostData(key uint64, value string) error {
	var posterr error
	for _, address := range kc.ClusterAddresses {
		keyValuePair := map[string]interface{}{
			"key":   key,
			"value": value,
		}
		json, err := json.Marshal(keyValuePair)
		if err != nil {
			posterr = err
			continue
		}
		postURL := fmt.Sprintf("%s/data", address)
		request, err := http.NewRequest("POST", postURL, bytes.NewBuffer(json))
		if err != nil {
			posterr = err
			continue
		}
		_, err = kc.doRequest(request)
		if err != nil {
			posterr = err
			continue
		}
		return nil
	}
	return posterr
}

// doRequest is responsible for sending the request to the address specified.
func (kc *KademliaClient) doRequest(r *http.Request) (string, error) {
	client := &http.Client{}
	response, err := client.Do(r)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return "", err
	}
	if http.StatusOK != response.StatusCode {
		return "", fmt.Errorf("Status=%d", response.StatusCode)
	}
	return string(body), nil
}
