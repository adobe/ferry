/*
Copyright 2021 Adobe. All rights reserved.
*/
package cmd

import (
	"log"

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
		log.Printf("import is not implemented yet")
	},
}

func init() {
	rootCmd.AddCommand(importCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// importCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// importCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
