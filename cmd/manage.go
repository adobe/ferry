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

	"github.com/adobe/blackhole/lib/archive"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var manageAction string

// manageCmd represents the manage command
var manageCmd = &cobra.Command{
	Use:   "manage",
	Short: "manages blobstore directory (list or delete recursively)",
	Long:  `This is used to cleanup blobstore directory structure. Use with caution`,

	Run: func(cmd *cobra.Command, args []string) {

		switch manageAction {
		case "list":
			list()
		case "delete":
			delete()
		default:
			gLogger.Fatal("Unknown action", zap.String("action", manageAction))
		}
	},
}

func list() {
	fileList, err := archive.List(storeURL)
	if err != nil {
		gLogger.Fatal("List failed", zap.String("url", storeURL))
	}
	for _, file := range fileList {
		fmt.Printf("%s\n", file)
	}
}

func delete() {
	fileList, err := archive.List(storeURL)
	if err != nil {
		gLogger.Fatal("List failed", zap.String("url", storeURL))
	}
	err = archive.Delete(storeURL, fileList)
	if err != nil {
		gLogger.Fatal("Delete failed", zap.String("url", storeURL))
	}
}

func init() {
	//rootCmd.AddCommand(manageCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
	manageCmd.Flags().StringVarP(&manageAction, "action", "a", "list", "Action - list|delete")
	manageCmd.Flags().StringVarP(&storeURL, "store-url", "s", "/tmp/", "Source/target for export/import/manage")
}
