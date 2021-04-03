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

package client

import (
	"context"
	"fmt"
	"io"
	"sync"

	ferry "github.com/adobe/ferry/rpc"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Exporter struct {
	db       fdb.Database
	grpcPort int
	//bindPort  int
	caFile string
	//certFile  string
	//keyFile   string
	targetURL string
	logger    *zap.Logger
	dryRun    bool
}

// storageGroup represents a set of ranges hosted by a single node
// A slice of storageGroup representing entire cluster will have many ranges
// with overlapping contents - since same range will be stored in multiple nodes.
type storageGroup struct {
	kranges []fdb.KeyRange
	// host    string
}

// rangeLocation represents a set of hosts holding the given range.
type rangeLocation struct {
	krange fdb.KeyRange
	hosts  []string
}

type partitionMap struct {
	nodes  map[string]storageGroup
	ranges []rangeLocation
}

// exportGroup is a dynamic data derived from []storageGroup
// for the current export. It represents the ranges planned to be
// extracted from the given host. It will be a subset of ranges
// hosted by the given host
type exportGroup struct {
	kranges []fdb.KeyRange
	host    string
	conn    ferry.FerryClient // Not exclusive to this
}

func NewExporter(db fdb.Database, targetURL string, grpcPort int, logger *zap.Logger, dryRun bool, caFile string) *Exporter {

	exp := &Exporter{
		db:        db,
		grpcPort:  grpcPort,
		targetURL: targetURL,
		dryRun:    dryRun,
		caFile:    caFile,
		logger:    logger,
	}
	return exp
}

func (exp *Exporter) AssignSources(pmap *partitionMap) (exportPlan map[string]exportGroup, err error) {

	exportPlan = make(map[string]exportGroup) // initialize return struct

	busy := map[string]int{}
	creds, err := credentials.NewClientTLSFromFile(exp.caFile, "")
	if err != nil {
		exp.logger.Warn("Failed to read TLS credentials", zap.String("ca-file", exp.caFile))
		return nil, errors.Wrapf(err, "Failed to read TLS credentials from %s", exp.caFile)
	}

	for _, x := range pmap.ranges {
		// find the least busy (alloted) host
		least_busy_host := ""
		current_load := -1
		for _, host := range x.hosts {
			if current_load < busy[host] {
				least_busy_host = host
				current_load = busy[host]
			}
		}
		exp.logger.Debug("Range->Host mapping",
			zap.ByteString("begin", x.krange.Begin.FDBKey()),
			zap.ByteString("end", x.krange.End.FDBKey()),
			zap.String("host", least_busy_host))

		busy[least_busy_host]++
		if _, ok := exportPlan[least_busy_host]; !ok {
			conn, err := grpc.Dial(fmt.Sprintf("%s:%d", least_busy_host, exp.grpcPort),
				grpc.WithTransportCredentials(creds))
			if err != nil {
				exp.logger.Warn("Failed to dail", zap.String("host", least_busy_host))
				return nil, errors.Wrapf(err, "Fail to dial: %s", least_busy_host)
			}
			kranges := []fdb.KeyRange{x.krange}
			exportPlan[least_busy_host] = exportGroup{
				kranges: kranges,
				conn:    ferry.NewFerryClient(conn),
				host:    least_busy_host}
		} else {
			eg := exportPlan[least_busy_host]
			eg.kranges = append(eg.kranges, x.krange)
			exportPlan[least_busy_host] = eg
		}
	}

	return exportPlan, err
}

func (exp *Exporter) GetLocations(boundaryKeys []fdb.Key) (pmap *partitionMap, err error) {

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

	var ranges []rangeLocation
	var nodes = map[string]storageGroup{}

	for _, v := range locationsTemp {
		v2, err := v.hosts.Get() // blocking now is OK
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to create fdb transaction")
		}

		ranges = append(ranges, rangeLocation{
			krange: v.krange,
			hosts:  v2,
		})
		for _, host := range v2 {
			node := nodes[host]
			node.kranges = append(nodes[host].kranges, v.krange)
			nodes[host] = node
		}
	}

	return &partitionMap{ranges: ranges, nodes: nodes}, nil
}

func (exp *Exporter) GetBoundaryKeys() (boundaryKeys []fdb.Key, err error) {

	beginKey := fdb.Key("")

	for {
		bKeys, err := exp.db.LocalityGetBoundaryKeys(fdb.KeyRange{Begin: beginKey, End: fdb.Key("\xFF")},
			1000, 0)
		if err != nil {
			return nil, errors.Wrapf(err, "Error querying LocalityGetBoundaryKeys")
		}
		if len(bKeys) > 1 ||
			// we must get at least one additional key than what we passed in
			// only keys from position 1 and later is really new
			// except for the boundary case when we first pass in '' as beginKey
			// In that rare case the DB only has one key in total, a single key
			// would return to us and we should still consider it a valid one to
			// save. That boundary case is the expression below.
			(len(boundaryKeys) == 0 && len(bKeys) == 1) {

			exp.logger.Debug("Boundary keys", zap.String("keys", fmt.Sprintf("%+v", bKeys)))
			beginKey = bKeys[len(bKeys)-1].FDBKey()
			exp.logger.Debug("Lasy key", zap.ByteString("key", beginKey))
			boundaryKeys = append(boundaryKeys, bKeys...)
		} else {
			break
		}
	}
	exp.logger.Debug("All keys", zap.String("keys", fmt.Sprintf("%+v", boundaryKeys)))
	return boundaryKeys, nil
}

func (exp *Exporter) ScheduleFetchByNode(eg exportGroup, dryRun bool) (err error) {

	exp.logger.Info("Starting session to", zap.Int("ranges", len(eg.kranges)), zap.String("host", eg.host))
	resp, err := eg.conn.StartSession(context.Background(), &ferry.Target{TargetUrl: exp.targetURL})
	if err != nil {
		return errors.Wrapf(err, "Unable to initiate session with peer")
	}
	sessionID := resp.SessionId

	if !dryRun {
		exportClient, err := eg.conn.Export(context.Background())
		if err != nil {
			return errors.Wrapf(err, "Unable to initiate export session with peer")
		}

		for _, krange := range eg.kranges {
			err = exportClient.Send(&ferry.KeyRequest{
				Begin:     krange.Begin.FDBKey(),
				End:       krange.End.FDBKey(),
				SessionId: sessionID,
			})
			if err != nil {
				return errors.Wrapf(err, "Unable to send key via export client")
			}
		}
		resp, err = exportClient.CloseAndRecv()
		if err != nil && err != io.EOF {
			return errors.Wrapf(err, "Unable to flush queue on export client")
		}
		exp.logger.Info(fmt.Sprintf("%+v", resp))

	} else {
		exp.logger.Info("DRYRUN", zap.Int("ranges", len(eg.kranges)), zap.String("host", eg.host))
	}
	exp.logger.Info("Closing session to", zap.Int("ranges", len(eg.kranges)), zap.String("host", eg.host))
	_, err = eg.conn.EndSession(context.Background(), &ferry.Session{SessionId: sessionID})
	if err != nil {
		return errors.Wrapf(err, "Error from EndSession")
	}

	return nil
}

func (exp *Exporter) ScheduleFetch(exportPlan map[string]exportGroup) (err error) {

	var wg sync.WaitGroup
	for _, plan := range exportPlan {
		wg.Add(1)
		go func(plan exportGroup, wg *sync.WaitGroup) {
			defer wg.Done()
			err = exp.ScheduleFetchByNode(plan, exp.dryRun)
			if err != nil {
				exp.logger.Error("Error from worker thread", zap.Error(err))
			}
		}(plan, &wg)
	}
	wg.Wait()
	return err
}
