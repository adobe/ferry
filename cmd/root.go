/*
Copyright 2021 Adobe. All rights reserved.
*/
package cmd

import (
	"fmt"
	"log"
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/profile"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var profilingRequested string // see github.com/pkg/profile
var profilesAvailable = map[string]func(*profile.Profile){
	"mem": 	profile.MemProfile,
	"cpu": profile.CPUProfile,
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "ferry",
	Short: "Set of utilities to export data from or import data into FoundationDB",
	Long: `This utility will export all (or filtered) data from FoundationDB 
to one of the possible stores - a local file-system folder, Azure blobstore or Amazon S3
Export is not done in a single transaction and that implies you should only do this
if your data is static or you don't care for it being a point-in-time snapshot`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage() // When run without any sub-commands, print help
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config",
		"", "config file (default is $HOME/.ferry.yaml)")

	rootCmd.PersistentFlags().StringVar(&profilingRequested, "profile",
		"", "mem|cpu (Go performance profiling)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolP("verbose", "v", false, "Verbose logging")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".ferry" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".ferry")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}

	if profilingRequested != "" {
		if p, ok := profilesAvailable[profilingRequested]; ok {
			profile.Start(p, profile.ProfilePath("."))
		} else {
			log.Fatalf("Unknown profiling mode '%s' requested", profilingRequested)
		}
	}
}