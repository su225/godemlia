package service

import (
	"log"
	"net/http"
	"strconv"

	"github.com/go-chi/chi"
	"github.com/go-chi/render"
)

// RESTHandler is responsible for handling requests to the REST
// service sent by the client.
type RESTHandler interface {
	// GetData gets the data for the given key. If the data is found
	// returns the obtained value with status 200. Otherwise it returns
	// status 404 indicating that the value for the key is not found
	GetData(w http.ResponseWriter, r *http.Request)

	// PutData puts the given key-value pair into the Kademlia network.
	// If it is successful, status 200 OK is returned. Otherwise it can
	// be 500 if there is an error on the server side or 400 Bad Request
	// in case of a malformed request.
	PutData(w http.ResponseWriter, r *http.Request)
}

// KademliaRESTHandler is responsible for providing the functionalities
// defined in the RESTHandler interface - that is get/put key-value
// pairs into the network.
type KademliaRESTHandler struct {
	nodeCtx *NodeContext
}

// CreateKademliaRESTHandler creates a new instance which is responsible
// for handling incoming requests from the client. It allows a client
// to get or put a key-value pair to the cluster.
func CreateKademliaRESTHandler(ctx *NodeContext) *KademliaRESTHandler {
	return &KademliaRESTHandler{nodeCtx: ctx}
}

// GetData gets the stored data from the Kademlia key-value store. If data is found
// then status 200 is returned with the value is returned. If the key is not in
// decimal and hence not parseable then 400 is returned. If there is some problem
// on the cluster-side because of which the request times out then 500 is returned
func (h *KademliaRESTHandler) GetData(w http.ResponseWriter, r *http.Request) {
	if key, keyParseErr := strconv.ParseUint(chi.URLParam(r, "key"), 10, 64); keyParseErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	} else {
		log.Printf("Request to GET key %d", key)
		if retrieved, retrieveErr := h.nodeCtx.NodeDataContext.DataRetriever.RetrieveKVPair(key); retrieveErr != nil {
			w.WriteHeader(http.StatusNotFound)
			return
		} else {
			w.WriteHeader(http.StatusOK)
			render.JSON(w, r, string(retrieved))
		}
	}
}

// PutData stores a key-value pair in the Kademlia store. If the key-value pair already
// exists then it is overwritten and last-write win semantics are followed for resolving
// conflicts among write of different values.
func (h *KademliaRESTHandler) PutData(w http.ResponseWriter, r *http.Request) {
	var keyValuePair struct {
		Key   uint64
		Value string
	}
	if jsonDecodeErr := render.DecodeJSON(r.Body, &keyValuePair); jsonDecodeErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	log.Printf("Request to POST key-value pair %d:%v", keyValuePair.Key, keyValuePair.Value)
	if storeErr := h.nodeCtx.NodeDataContext.DataStorer.StoreKVPair(keyValuePair.Key, []byte(keyValuePair.Value)); storeErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Printf("Successfully saved key %d", keyValuePair.Key)
	w.WriteHeader(http.StatusOK)
}
