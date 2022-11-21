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
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var base64Output bool
var dirPathInput string
var subSpacePathInput string
var key string

// queryCmd represents the export command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "Query for a given key",
	Run: func(cmd *cobra.Command, args []string) {

		dirPath := strings.Split(dirPathInput, "/")
		subSpacePath := strings.Split(subSpacePathInput, "/")

		_, err := gFDB.ReadTransact(func(rt fdb.ReadTransaction) (interface{}, error) {

			subSpace, err := directory.Open(rt, dirPath, nil)
			if err != nil {
				return nil, errors.Wrapf(err, "path=%+v", dirPath)
			}
			fmt.Printf("Directory key prefix = %s\n", subSpace.FDBKey())

			subSpaceTuples := []tuple.TupleElement{}
			for _, ss := range subSpacePath {
				subSpaceTuples = append(subSpaceTuples, tuple.TupleElement(ss))
			}
			keySpace := subSpace.Sub(subSpaceTuples...)
			fmt.Printf("Subspace key prefix = %s\n", keySpace.FDBKey())

			if len(key) > 0 {
				fKey := keySpace.Pack(tuple.Tuple{[]byte(key)})
				fmt.Printf("Final key = %s\n", fKey)
				value, err := rt.Get(fKey).Get()
				if err != nil {
					return nil, errors.Wrapf(err, "path=%+v, key=%+v", dirPath, key)
				}
				fmt.Printf("Value = %+v\n", fdb.Printable(value))
			} else {
				var er fdb.ExactRange = keySpace.(fdb.ExactRange)
				var bk, ek fdb.KeyConvertible
				bk, ek = er.FDBRangeKeys()
				var fr fdb.KeyRange = fdb.KeyRange{Begin: bk, End: ek}
				fmt.Printf("Subspace key range = %+v\n", fr)
				fKey := rt.GetRange(fr, fdb.RangeOptions{Limit: 100, Mode: fdb.StreamingModeSerial})
				it := fKey.Iterator()
				for it.Advance() {
					// ---------------------------------------------------------
					// uncomment line below for testing only
					// time.Sleep(time.Millisecond * 1)
					// This is to artifically create the 5 second txn limit test
					// ---------------------------------------------------------
					kv, err := it.Get()
					if err != nil {
						return nil, errors.Wrapf(err, "path=%+v, key=%+v", dirPath, subSpace.FDBKey())
					}
					fmt.Printf("Key = %+v, Value = %+v\n", fdb.Printable(kv.Key), fdb.Printable(kv.Value))
				}
			}

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
	queryCmd.Flags().StringVarP(&dirPathInput, "directory", "", "", "Directory path to open")
	queryCmd.Flags().StringVarP(&subSpacePathInput, "subspace", "", "", "Subspace path (inside directory) to open")
	queryCmd.Flags().StringVarP(&key, "key", "", "", "Key to query")
}
