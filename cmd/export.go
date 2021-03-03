/*
Copyright 2021 Adobe. All rights reserved.
*/
package cmd

import (
	"github.com/spf13/cobra"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"git.corp.adobe.com/hbhaskar/ferry/lib/exporter"
	"log"
)

// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export all (or filtered set of) keys and values from FoundationDB",
	Long: `This utility will export all (or filtered) data from FoundationDB 
to one of the possible stores - a local file-system folder, Azure blobstore or Amazon S3
Export is not done in a single transaction and that implies you should only do this
if your data is static or you don't care for it being a point-in-time snapshot`,
	Run: func(cmd *cobra.Command, args []string) {
		fdb.MustAPIVersion(620)
		// Open the default database from the system cluster
		db := fdb.MustOpenDefault()
		exp := exporter.NewExporter(db, "")
		err := exp.Export()
		if err != nil {
			log.Printf("Export failed: %+v", err)
		}
	},
}

func init() {
	rootCmd.AddCommand(exportCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// exportCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// exportCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
