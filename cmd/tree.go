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
	"log"

	"github.com/adobe/ferry/fdbstat"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

// statusCmd represents the manage command
var treeCmd = &cobra.Command{
	Use:   "tree",
	Short: "Print tree of directories",
	Long:  `Print tree of directories (like unix tree command)`,

	Run: func(cmd *cobra.Command, args []string) {

		srvy, err := fdbstat.NewSurveyor(gFDB, fdbstat.Logger(gLogger))
		if err != nil {
			gLogger.Fatal("Error initializing finder", zap.Error(err))
		}

		dirs, err := srvy.GetAllDirectories()
		if err != nil {
			log.Fatalf("GetAllDirectories errored: %+v", err)
		}
		for _, dir := range dirs {
			gLogger.Info("Directory", zap.String("path", dir))
		}
	},
}

func init() {
	rootCmd.AddCommand(treeCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
}
