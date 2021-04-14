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
	ferry "github.com/adobe/ferry/rpc"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ImporterOption func(exp *ImporterClient)

type ImporterClient struct {
	// Must have properties
	db        fdb.Database
	grpcPort  int
	caFile    string
	targetURL string

	// Optional, but defaults if not set
	logger *zap.Logger

	// Optional, set via ExporterOptions
	dryRun        bool
	samplingMode  bool
	writerThreads int
}

/*
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
*/

// importGroup is a dynamic data derived from manifest file
// for the current import. It represents the files planned to be
// imported to the given host.
type importGroup struct {
	files []string
	host  string
	conn  ferry.FerryClient // Not exclusive to this
}

func NewImporter(db fdb.Database,
	targetURL string,
	grpcPort int,
	caFile string,
	opts ...ImporterOption,
) (exp *ImporterClient, err error) {

	exp = &ImporterClient{
		db:        db,
		grpcPort:  grpcPort,
		targetURL: targetURL,
		caFile:    caFile,
	}
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

func Logger(logger *zap.Logger) ImporterOption {
	return func(exp *ImporterClient) {
		exp.logger = logger
	}

}
func Dryrun(dryRun bool) ImporterOption {
	return func(exp *ImporterClient) {
		exp.dryRun = dryRun
	}
}

func WriterThreads(writerThreads int) ImporterOption {
	return func(exp *ImporterClient) {
		exp.writerThreads = writerThreads
	}
}

func Sample(sample bool) ImporterOption {
	return func(exp *ImporterClient) {
		exp.samplingMode = sample
	}
}
