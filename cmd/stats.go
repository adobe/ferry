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
	"github.com/adobe/ferry/fdbstat"
	"github.com/adobe/ferry/finder"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var summary bool

// statsCmd represents the export command
var statsCmd = &cobra.Command{
	Use:   "stats",
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

		srvy, err := fdbstat.NewSurveyor(gFDB, fdbstat.Logger(gLogger))
		if err != nil {
			gLogger.Fatal("Error initializing finder", zap.Error(err))
		}

		sizeByRange, err := srvy.CalculateDBSize(pmap)
		if err != nil {
			gLogger.Fatal("Error fetching estimated size", zap.Error(err))
		}

		totalSize := int64(0)

		var smallestPartitionRange, biggestPartitionRange fdbstat.HashableKeyRange
		smallestPartitionSize, biggestPartitionSize := int64(1<<62), int64(0)

		for sbr, size := range sizeByRange {
			if size > biggestPartitionSize {
				biggestPartitionRange = sbr
				biggestPartitionSize = size
			}
			if size < smallestPartitionSize {
				smallestPartitionRange = sbr
				smallestPartitionSize = size
			}
			totalSize += size
			if !summary {
				gLogger.Info("By range",
					zap.String("Begin", sbr.Begin),
					zap.String("End", sbr.End),
					zap.Int64("Size", size),
				)
			}
		}

		gLogger.Info("Total",
			zap.Int64("size", totalSize),
			zap.Any("Smallest", smallestPartitionRange),
			zap.Int64("Smallest partition", smallestPartitionSize),
			zap.Any("Biggest", biggestPartitionRange),
			zap.Int64("Biggest partition", biggestPartitionSize),
		)
	},
}

func init() {
	rootCmd.AddCommand(statsCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
	statsCmd.Flags().BoolVarP(&summary, "summary", "s", false, "Report only final db level summary")
}
