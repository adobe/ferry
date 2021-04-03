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

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// manageCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	serveCmd.Flags().StringVarP(&manageAction, "action", "a", "list", "Action - list|delete")
}
