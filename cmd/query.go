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
	"log"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var base64Output bool
var dirPath string
var key string

// queryCmd represents the export command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query for a given key",
	Run: func(cmd *cobra.Command, args []string) {

		path := strings.Split(dirPath, "/")
		_, err := gFDB.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {

			subSpace, err := directory.Open(rt, path, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "path=%+v", path)
			}
			fmt.Printf("Subspace key prefix = %+v\n", subSpace.Bytes())

			value, err := rt.Get(subSpace.FDBKey()).Get()
			if err != nil {
				return nil, errors.Wrapf(err, "path=%+v, key=%+v", path, key)
			}
			fmt.Printf("Value = %+v\n", value)

			return nil, nil
		})
		if err != nil {
			log.Fatalf("Error: %+v", err)
		}

	},
}

func init() {
	rootCmd.AddCommand(queryCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
	queryCmd.Flags().BoolVarP(&base64Output, "base64", "", false, "Print value as base64")
	queryCmd.Flags().StringVarP(&dirPath, "path", "", "", "Directory path to open")
}
