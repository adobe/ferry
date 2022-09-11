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
	"github.com/spf13/viper"
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

		srvy, err := fdbstat.NewSurveyor(gFDB, fdbstat.Logger(gLogger))
		if err != nil {
			gLogger.Fatal("Error initializing finder", zap.Error(err))
		}

		gLogger.Info("threads", zap.Int("threads", viper.GetInt("threads")))
		dbSize, err := srvy.CalculateRowCount(pmap, viper.GetInt("threads"))
		if err != nil {
			gLogger.Fatal("Error fetching estimated size", zap.Error(err))
		}
		gLogger.Info("Total DB size", zap.Int64("size", dbSize))
	},
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
	statLocalCmd.Flags().IntP("threads", "t", 0, "How many threads to read in parallel")
	statLocalCmd.Flags().IntP("checksum", "c", 0, "Checksum all data")
	statLocalCmd.Flags().IntP("summary", "s", 0, "Report only final db level summary")
}
