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
	"github.com/adobe/ferry/importer/client"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import all (or filtered set of) keys and values from an export",
	Long: `Import data from the set of files created via the 'export' sub-command earlier.
Import all data or a subset of keys to a target FoundationDB instance 
`,
	Run: func(cmd *cobra.Command, args []string) {
		fdb.MustAPIVersion(620)
		// Open the default database from the system cluster
		db := fdb.MustOpenDefault()
		exp, err := client.NewImporter(db,
			storeURL, viper.GetInt("port"),
			viper.GetString("tls.cert"),
			client.Logger(gLogger),
			client.Dryrun(viper.GetBool("dryrun")),
			client.Sample(viper.GetBool("sample")),
			client.WriterThreads(viper.GetInt("threads")),
		)
		if err != nil {
			gLogger.Fatal("Error initializing exporter", zap.Error(err))
		}
		importPlan, err := exp.AssignTargets()
		if err != nil {
			gLogger.Fatal("Error assigning export nodes", zap.Error(err))
		}

		err = exp.ScheduleImport(importPlan)
		if err != nil {
			gLogger.Fatal("Error scheduling exports", zap.Error(err))
		}
	},
}

func init() {
	rootCmd.AddCommand(importCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
	importCmd.Flags().StringVarP(&storeURL, "store-url", "s", "/tmp/", "Source/target for export/import/manage")
}
