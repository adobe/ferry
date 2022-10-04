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

	"github.com/adobe/ferry/fdbstat"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var print_hosts bool

// statusCmd represents the manage command
var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Print fdb status [ DO NOT USE: Incomplete ]",
	Long:  `Print fdb status [ DO NOT USE: Incomplete ]`,

	Run: func(cmd *cobra.Command, args []string) {
		status, err := fdbstat.GetStatus(gFDB)
		if err != nil {
			gLogger.Fatal("Status failed", zap.Error(err))
		}
		if !print_hosts {
			fmt.Println(status)
		} else {
			hosts, err := fdbstat.GetNodesFromStatus(status)
			if err != nil {
				gLogger.Fatal("Status failed", zap.Error(err))
			}
			for _, host := range hosts {
				fmt.Println(host)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------

	statusCmd.Flags().BoolVarP(&print_hosts, "hosts", "", false, "Print only hosts")
}
