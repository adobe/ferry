/*
Copyright 2021 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

package finder

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type FinderOption func(exp *Finder)

type Finder struct {
	// Must have properties
	db fdb.Database

	// Optional, but defaults if not set
	logger *zap.Logger
}

// storageGroup represents a set of ranges hosted by a single node
// A slice of storageGroup representing entire cluster will have many ranges
// with overlapping contents - since same range will be stored in multiple nodes.
type StorageGroup struct {
	kranges []fdb.KeyRange
	// host    string
}

// rangeLocation represents a set of hosts holding the given range.
type RangeLocation struct {
	Krange fdb.KeyRange
	Hosts  []string
}

type PartitionMap struct {
	Nodes  map[string]StorageGroup
	Ranges []RangeLocation
}

func NewFinder(db fdb.Database,
	opts ...FinderOption,
) (exp *Finder, err error) {

	exp = &Finder{db: db}
	for _, opt := range opts {
		opt(exp)
	}
	// if logger is not set, we must set one
	if exp.logger == nil {
		exp.logger, err = zap.NewProduction()
		if err != nil {
			return nil, errors.Wrapf(err, "Logger not supplied. Can't initialize one either")
		}
	}
	return exp, nil
}

func Logger(logger *zap.Logger) FinderOption {
	return func(exp *Finder) {
		exp.logger = logger
	}

}

func (exp *Finder) GetLocations(boundaryKeys []fdb.Key) (pmap *PartitionMap, err error) {

	// rangeLocation represents a set of hosts holding the given range.
	type rangeLocationTemp struct {
		krange fdb.KeyRange
		hosts  fdb.FutureStringSlice
	}
	var locationsTemp []rangeLocationTemp

	txn, err := exp.db.CreateTransaction()
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to create fdb transaction")
	}

	for i, beginKey := range boundaryKeys {
		var endKey fdb.Key
		if i == len(boundaryKeys)-1 { // are we on last key?
			endKey = fdb.Key("\xFF")
		} else {
			endKey = boundaryKeys[i+1]
		}
		locationsTemp = append(locationsTemp, rangeLocationTemp{
			krange: fdb.KeyRange{Begin: beginKey, End: endKey},
			hosts:  txn.LocalityGetAddressesForKey(beginKey),
		})
	}

	var ranges []RangeLocation
	var nodes = map[string]StorageGroup{}

	for _, v := range locationsTemp {
		v2, err := v.hosts.Get() // blocking now is OK
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create fdb transaction")
		}

		ranges = append(ranges, RangeLocation{
			Krange: v.krange,
			Hosts:  v2,
		})
		for _, host := range v2 {
			node := nodes[host]
			node.kranges = append(nodes[host].kranges, v.krange)
			nodes[host] = node
		}
	}

	return &PartitionMap{Ranges: ranges, Nodes: nodes}, nil
}

func (exp *Finder) GetBoundaryKeys() (boundaryKeys []fdb.Key, err error) {

	beginKey := fdb.Key("")

	exp.logger.Debug("Checking boundary keys")

	for {
		bKeys, err := exp.db.LocalityGetBoundaryKeys(fdb.KeyRange{Begin: beginKey, End: fdb.Key("\xFF")},
			1000, 0)
		if err != nil {
			return nil, errors.Wrapf(err, "Error querying LocalityGetBoundaryKeys")
		}
		// we must get at least one additional key than what we passed in
		// only keys from position 1 and later is really new
		// except for the boundary case when we first pass in '' as beginKey
		if len(bKeys) > 1 {

			exp.logger.Debug("Boundary keys", zap.String("keys", fmt.Sprintf("%+v", bKeys)))
			beginKey = bKeys[len(bKeys)-1].FDBKey()
			exp.logger.Debug("Last key", zap.ByteString("key", beginKey))
			boundaryKeys = append(boundaryKeys, bKeys[1:]...)
		} else {
			// In that rare case the DB only has one key in total, a single key
			// would return to us and we should still consider it a valid one to
			// save. That boundary case is the expression below.
			if len(boundaryKeys) == 0 && len(bKeys) == 1 {
				boundaryKeys = append(boundaryKeys, bKeys[0])
			}
			break
		}
	}
	exp.logger.Debug("All keys", zap.String("keys", fmt.Sprintf("%+v", boundaryKeys)))
	return boundaryKeys, nil
}
