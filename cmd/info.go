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
	"encoding/json"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// infoCmd represents the manage command
var infoCmd = &cobra.Command{
	Use:   "info",
	Short: "Print info on effective config",
	Long:  `Print info on effective config`,

	Run: func(cmd *cobra.Command, args []string) {
		b, err := json.MarshalIndent(viper.AllSettings(), "", "\t")
		if err != nil {
			fmt.Printf("%+v", err)
			os.Exit(1)
		}
		fmt.Println(string(b))
	},
}

func init() {
	rootCmd.AddCommand(infoCmd)
}
