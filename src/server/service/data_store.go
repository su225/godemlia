package service

import (
	"fmt"
	"sync"
	"time"
)

// DataStore stores the key-value pairs and retrieves the key-value pairs
// currently stored in this node in a thread-safe manner.
type DataStore struct {
	// dataStoreLock is used to make concurrent access
	// to the store thread-safe
	dataStoreLock sync.RWMutex

	// kvSet is responsible for holding key-value pairs.
	kvSet  map[uint64][]byte
	kvTime map[uint64]int64
}

// CreateDataStore creates a new instance of a data store
func CreateDataStore() *DataStore {
	return &DataStore{
		dataStoreLock: sync.RWMutex{},
		kvSet:         make(map[uint64][]byte),
		kvTime:        make(map[uint64]int64),
	}
}

// Add adds a key-value pair to the data store. If the key already
// exists then returns complaining that it already exists
func (ds *DataStore) Add(key uint64, value []byte) error {
	ds.dataStoreLock.Lock()
	defer ds.dataStoreLock.Unlock()
	if _, present := ds.kvSet[key]; present {
		ds.kvTime[key] = time.Now().UnixNano()
		return fmt.Errorf("Key %d already exists", key)
	}
	ds.kvSet[key] = value
	return nil
}

// AddOrReplace is like Add. But if the key already exists, then its
// value is overwritten.
func (ds *DataStore) AddOrReplace(key uint64, value []byte) error {
	ds.dataStoreLock.Lock()
	defer ds.dataStoreLock.Unlock()
	ds.kvSet[key] = value
	ds.kvTime[key] = time.Now().UnixNano()
	return nil
}

// Remove removes a key-value pair from the store if it exists
// Otherwise it returns an error complaining that the key doesn't exist.
func (ds *DataStore) Remove(key uint64) error {
	ds.dataStoreLock.Lock()
	defer ds.dataStoreLock.Unlock()
	if _, present := ds.kvSet[key]; !present {
		return fmt.Errorf("Key %d not present", key)
	}
	delete(ds.kvTime, key)
	delete(ds.kvSet, key)
	return nil
}

// Get gets a value from the store if it exists. Otherwise it
// complains that the mapping for the key doesn't exist.
func (ds *DataStore) Get(key uint64) ([]byte, error) {
	ds.dataStoreLock.RLock()
	defer ds.dataStoreLock.RUnlock()
	if value, present := ds.kvSet[key]; !present {
		return []byte{}, fmt.Errorf("Key %d not present", key)
	} else {
		return value, nil
	}
}

// GetAllKeys is used by data republisher. Note that this must be removed while
// optimizing republishing as it is not scalable to pull all the keys and
// store it in a slice
func (ds *DataStore) GetAllKeys() []uint64 {
	ds.dataStoreLock.RLock()
	defer ds.dataStoreLock.RUnlock()
	keys := make([]uint64, 0)
	for k := range ds.kvSet {
		keys = append(keys, k)
	}
	return keys
}

// GetRefreshTime returns the last refreshed time of the key.
func (ds *DataStore) GetRefreshTime(k uint64) (int64, error) {
	ds.dataStoreLock.RLock()
	defer ds.dataStoreLock.RUnlock()
	if refTime, present := ds.kvTime[k]; !present {
		return 0, fmt.Errorf("Key %d not found", k)
	} else {
		return refTime, nil
	}
}
