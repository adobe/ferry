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
	"fmt"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/adobe/ferry/fdbstat"
	ferry "github.com/adobe/ferry/rpc"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func (exp *ImporterClient) AssignTargets() (importPlan map[string]importGroup, err error) {

	importPlan = make(map[string]importGroup) // initialize return struct

	busy := map[string]int{}
	creds, err := credentials.NewClientTLSFromFile(exp.caFile, "")
	if err != nil {
		exp.logger.Warn("Failed to read TLS credentials", zap.String("ca-file", exp.caFile))
		return nil, errors.Wrapf(err, "Failed to read TLS credentials from %s", exp.caFile)
	}

	fileList, err := archive.List(exp.targetURL)
	if err != nil {
		exp.logger.Warn("Unable to list files", zap.Error(err), zap.String("source", exp.targetURL))
		return nil, errors.Wrapf(err, "Unable to list files from %s", exp.targetURL)
	}
	all_hosts, err := fdbstat.GetAllNodes(exp.db)

	for _, fileName := range fileList {
		// find the least busy (alloted) host
		// **NOTE** This logic is temporary and
		// will change once we get a MANIFEST file with
		// range and host mappings
		least_busy_host := ""
		current_load := -1
		for _, host := range all_hosts {
			if current_load < busy[host] {
				least_busy_host = host
				current_load = busy[host]
			}
		}
		exp.logger.Debug("File->Host mapping",
			zap.String("fileName", fileName),
			zap.String("host", least_busy_host))

		busy[least_busy_host]++
		if _, ok := importPlan[least_busy_host]; !ok {
			conn, err := grpc.Dial(fmt.Sprintf("%s:%d", least_busy_host, exp.grpcPort),
				grpc.WithTransportCredentials(creds))
			if err != nil {
				exp.logger.Warn("Failed to dail", zap.String("host", least_busy_host))
				return nil, errors.Wrapf(err, "Fail to dial: %s", least_busy_host)
			}
			files := []string{fileName}
			importPlan[least_busy_host] = importGroup{
				files: files,
				conn:  ferry.NewFerryClient(conn),
				host:  least_busy_host}
		} else {
			eg := importPlan[least_busy_host]
			eg.files = append(eg.files, fileName)
			importPlan[least_busy_host] = eg
		}
	}

	return importPlan, err
}
