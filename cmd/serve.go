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
	"github.com/adobe/ferry/exporter/server"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

// manageCmd represents the manage command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Serve exporter grpc server",
	Long:  `Serve exporter grpc server`,

	Run: func(cmd *cobra.Command, args []string) {

		fdb.MustAPIVersion(620)
		// Open the default database from the system cluster
		db := fdb.MustOpenDefault()
		srv := server.NewServer(db,
			viper.GetInt("port"),
			viper.GetString("tls.cert"),
			viper.GetString("tls.privKey"),
			gLogger)
		err := srv.ServeExport()
		if err != nil {
			gLogger.Fatal("Server failed to start", zap.Error(err))
		}
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
}
