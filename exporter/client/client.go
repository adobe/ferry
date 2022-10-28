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

type ExporterOption func(exp *ExporterClient)

type ExporterClient struct {
	// Must have properties
	db        fdb.Database
	grpcPort  int
	caFile    string
	targetURL string

	// Optional, but defaults if not set
	logger *zap.Logger

	// Optional, set via ExporterOptions
	dryRun        bool
	readPercent   int
	compress      bool
	readerThreads int
	collectDir    string
	exportFormat  string
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

func NewExporter(db fdb.Database,
	targetURL string,
	grpcPort int,
	caFile string,
	opts ...ExporterOption,
) (exp *ExporterClient, err error) {

	exp = &ExporterClient{
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

func Logger(logger *zap.Logger) ExporterOption {
	return func(exp *ExporterClient) {
		exp.logger = logger
	}

}
func Dryrun(dryRun bool) ExporterOption {
	return func(exp *ExporterClient) {
		exp.dryRun = dryRun
	}
}

func Compress(compress bool) ExporterOption {
	return func(exp *ExporterClient) {
		exp.compress = compress
	}
}

func ReaderThreads(readerThreads int) ExporterOption {
	return func(exp *ExporterClient) {
		exp.readerThreads = readerThreads
	}
}

func Collect(collectDir string) ExporterOption {
	return func(exp *ExporterClient) {
		exp.collectDir = collectDir
	}
}

func Sample(sample int) ExporterOption {
	return func(exp *ExporterClient) {
		exp.readPercent = sample
	}
}

func ExportFormat(format string) ExporterOption {
	return func(exp *ExporterClient) {
		exp.exportFormat = format
	}
}
