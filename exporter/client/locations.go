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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math"
	"os"

	"github.com/adobe/ferry/finder"
	ferry "github.com/adobe/ferry/rpc"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func (exp *ExporterClient) AssignSources(pmap *finder.PartitionMap) (exportPlan map[string]exportGroup, err error) {

	exportPlan = make(map[string]exportGroup) // initialize return struct

	busy := map[string]int{}

	b, err := os.ReadFile(exp.caFile)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to read CA file: >%s<", exp.caFile)
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return nil, errors.New("credentials: failed to append certificates")
	}
	config := &tls.Config{
		InsecureSkipVerify: true, // TODO: Fix the TLS issue
		RootCAs:            cp,
		ServerName:         "adobe.net",
	}
	/*
		creds, err := credentials.NewClientTLSFromFile(exp.caFile, "adobe.net")
		if err != nil {
			exp.logger.Warn("Failed to read TLS credentials", zap.String("ca-file", exp.caFile))
			return nil, errors.Wrapf(err, "Failed to read TLS credentials from %s", exp.caFile)
		}
	*/

	for _, x := range pmap.Ranges {
		// find the least busy (alloted) host
		least_busy_host := ""
		current_load := math.MaxInt
		all_options_for_range := []string{}

		for _, host := range x.Hosts {
			all_options_for_range = append(all_options_for_range,
				fmt.Sprintf("%s=%d", host, busy[host]))
		}

		for _, host := range x.Hosts {
			if current_load > busy[host] {
				least_busy_host = host
				current_load = busy[host]
			}
		}

		exp.logger.Debug("Range->Host mapping",
			zap.ByteString("begin", x.Krange.Begin.FDBKey()),
			zap.ByteString("end", x.Krange.End.FDBKey()),
			zap.String("host", least_busy_host),
			zap.Int("current_load", current_load),
			zap.Any("others", all_options_for_range))

		busy[least_busy_host]++

		if _, ok := exportPlan[least_busy_host]; !ok {
			conn, err := grpc.Dial(fmt.Sprintf("%s:%d", least_busy_host, exp.grpcPort),
				//grpc.WithTransportCredentials(creds))
				grpc.WithTransportCredentials(credentials.NewTLS(config)))

			if err != nil {
				exp.logger.Warn("Failed to dail", zap.String("host", least_busy_host))
				return nil, errors.Wrapf(err, "Fail to dial: %s", least_busy_host)
			}
			kranges := []fdb.KeyRange{x.Krange}
			exportPlan[least_busy_host] = exportGroup{
				kranges: kranges,
				conn:    ferry.NewFerryClient(conn),
				host:    least_busy_host}
		} else {
			eg := exportPlan[least_busy_host]
			eg.kranges = append(eg.kranges, x.Krange)
			exportPlan[least_busy_host] = eg
		}
	}

	for k, v := range exportPlan {
		exp.logger.Debug("EXPORT-PLAN", zap.String("host", k), zap.Int("ranges", len(v.kranges)))
	}
	return exportPlan, err
}
