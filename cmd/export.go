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
	"github.com/adobe/ferry/exporter/client"
	"github.com/adobe/ferry/finder"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export all (or filtered set of) keys and values from FoundationDB",
	Long: `This utility will export all (or filtered) data from FoundationDB 
to one of the possible stores - a local file-system folder, Azure blobstore or Amazon S3
Export is not done in a single transaction and that implies you should only do this
if your data is static or you don't care for it being a point-in-time snapshot`,
	Run: func(cmd *cobra.Command, args []string) {

		finder, err := finder.NewFinder(gFDB, finder.Logger(gLogger))
		if err != nil {
			gLogger.Fatal("Error initializing finder", zap.Error(err))
		}

		exp, err := client.NewExporter(gFDB,
			storeURL, viper.GetInt("port"),
			viper.GetString("tls.cert"),
			client.Logger(gLogger),
			client.Dryrun(viper.GetBool("dryrun")),
			client.Sample(viper.GetBool("sample")),
			client.Compress(viper.GetBool("compress")),
			client.ReaderThreads(viper.GetInt("threads")),
			client.Collect(viper.GetString("collect")),
		)
		if err != nil {
			gLogger.Fatal("Error initializing exporter", zap.Error(err))
		}
		bKeys, err := finder.GetBoundaryKeys()
		if err != nil {
			gLogger.Fatal("Error fetching boundary keys", zap.Error(err))
		}
		partitionMap, err := finder.GetLocations(bKeys)
		if err != nil {
			gLogger.Fatal("Error fetching locations", zap.Error(err))
		}

		exportPlan, err := exp.AssignSources(partitionMap)
		if err != nil {
			gLogger.Fatal("Error assigning export nodes", zap.Error(err))
		}

		err = exp.ScheduleFetch(exportPlan)
		if err != nil {
			gLogger.Fatal("Error scheduling exports", zap.Error(err))
		}
	},
}

func init() {
	rootCmd.AddCommand(exportCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
	exportCmd.Flags().BoolP("dryrun", "n", false, "Dryrun connectivity check")
	exportCmd.Flags().BoolP("sample", "m", false, "Sample - fetch only 1000 keys per range")
	exportCmd.Flags().BoolP("compress", "c", false, "Compress export files (.lz4)")
	exportCmd.Flags().IntP("threads", "t", 0, "How many threads per range")
	exportCmd.Flags().StringP("collect", "", "", "Bring exported files to this host at this directory. Only applies to file:// targets")
	exportCmd.Flags().StringVarP(&storeURL, "store-url", "s", "/tmp/", "Source/target for export/import/manage")
}
