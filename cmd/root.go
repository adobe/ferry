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
	"os"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/profile"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var cfgFile string
var storeURL string
var gLogger *zap.Logger
var verbose bool

var profilingRequested string // see github.com/pkg/profile
var profilesAvailable = map[string]func(*profile.Profile){
	"mem": profile.MemProfile,
	"cpu": profile.CPUProfile,
}
var ProfileStarted interface{ Stop() }

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

	rootCmd.PersistentFlags().StringVarP(&storeURL, "store-url", "s", "/tmp/", "Source/target for export/import/manage")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")
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
			ProfileStarted = profile.Start(p, profile.ProfilePath("."))
		} else {
			log.Fatalf("Unknown profiling mode '%s' requested", profilingRequested)
		}
	}

	zapLevel := zapcore.InfoLevel
	if verbose {
		zapLevel = zapcore.DebugLevel
	}
	zapConfig := zap.Config{
		Level:             zap.NewAtomicLevelAt(zapLevel),
		DisableCaller:     true,
		DisableStacktrace: true,
		Development:       verbose,
		Encoding:          "console",
		EncoderConfig:     zap.NewDevelopmentEncoderConfig(),
		OutputPaths:       []string{"stderr"},
		ErrorOutputPaths:  []string{"stderr"},
	}
	var err error
	gLogger, err = zapConfig.Build()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
