/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"log"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/spf13/cobra"
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
			log.Printf("Unknown action: %s", manageAction)
		}
	},
}

func list() {
	fileList, err := archive.List(storeURL)
	if err != nil {
		log.Printf("Export failed: %+v", err)
	}
	for _, file := range fileList {
		log.Printf("%s", file)
	}
}

func delete() {
	fileList, err := archive.List(storeURL)
	if err != nil {
		log.Printf("Export failed: %+v", err)
	}
	err = archive.Delete(storeURL, fileList)
	if err != nil {
		log.Printf("manage failed: %+v", err)
	}
}

func init() {
	rootCmd.AddCommand(manageCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// manageCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	manageCmd.Flags().StringVarP(&manageAction, "action", "a", "list", "Action - list|delete")
}
