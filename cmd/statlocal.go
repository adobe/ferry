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

package cmd

import (
	"fmt"

	"github.com/adobe/ferry/exporter/session"
	"github.com/adobe/ferry/finder"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// statLocalCmd represents the export command
var statLocalCmd = &cobra.Command{
	Use:   "statlocal",
	Short: "Print stats about the current DB",
	Run: func(cmd *cobra.Command, args []string) {
		finder, err := finder.NewFinder(gFDB, finder.Logger(gLogger))
		if err != nil {
			gLogger.Fatal("Error initializing finder", zap.Error(err))
		}

		bKeys, err := finder.GetBoundaryKeys()
		if err != nil {
			gLogger.Fatal("Error fetching boundary keys", zap.Error(err))
		}
		pmap, err := finder.GetLocations(bKeys)
		if err != nil {
			gLogger.Fatal("Error fetching locations", zap.Error(err))
		}
		dbSize, err := calculateRowCount(gFDB, pmap)
		if err != nil {
			gLogger.Fatal("Error fetching estimated size", zap.Error(err))
		}
		gLogger.Info("Total DB size", zap.Int64("size", dbSize))
	},
}

func calculateDBSize(db fdb.Database, pmap *finder.PartitionMap) (totalSize int64, err error) {

	txn, err := db.CreateTransaction()
	for _, v := range pmap.Ranges {
		if err != nil {
			return 0, errors.Wrapf(err, "Unable to create fdb transaction")
		}
		gLogger.Info("Attempt", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
			zap.ByteString("end", v.Krange.End.FDBKey()),
			zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
		size, err := txn.GetEstimatedRangeSizeBytes(v.Krange).Get()
		if err != nil {
			return 0, errors.Wrapf(err, "Unable to create fdb transaction")
		}
		totalSize += size
		gLogger.Info("Range", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
			zap.ByteString("end", v.Krange.End.FDBKey()),
			zap.Int64("size", size),
			zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
	}
	txn.Commit()
	return totalSize, nil
}

func calculateRowCount(db fdb.Database, pmap *finder.PartitionMap) (totalRows int64, err error) {

	es, err := session.NewSession(db,
		"",
		40,
		false,
		gLogger,
		false)
	if err != nil {
		gLogger.Warn("Failed to create a session ID", zap.Error(err))
		return 0, errors.Wrap(err, "Failed to create a session ID")
	}

	for _, v := range pmap.Ranges {
		//gLogger.Info("Attempt", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
		//	zap.ByteString("end", v.Krange.End.FDBKey()),
		//	zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
		es.Send(v.Krange)
	}
	es.Finalize()
	return 0, nil
}

func init() {
	rootCmd.AddCommand(statLocalCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
	statLocalCmd.Flags().BoolP("sample", "m", false, "Sample - fetch only 1000 keys per range")
	statLocalCmd.Flags().IntP("threads", "t", 0, "How many threads per range")
}
