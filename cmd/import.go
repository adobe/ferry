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
	"github.com/spf13/cobra"
)

// importCmd represents the import command
var importCmd = &cobra.Command{
	Use:   "import",
	Short: "Import all (or filtered set of) keys and values from an export",
	Long: `Import data from the set of files created via the 'export' sub-command earlier.
Import all data or a subset of keys to a target FoundationDB instance 
`,
	Run: func(cmd *cobra.Command, args []string) {
		gLogger.Fatal("import is not implemented yet")
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
