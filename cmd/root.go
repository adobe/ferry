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

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
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
		err := cmd.Usage() // When run without any sub-commands, print help
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
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
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Verbose logging")
	rootCmd.PersistentFlags().IntP("port", "p", 0, "Port to bind to (applies to `serve` and `export` commands")

}

var gFDB fdb.Database
var gFDBinitalized bool

func initFDB() {
	var err error

	if !gFDBinitalized {

		fdb.MustAPIVersion(710)

		tlsConfig := viper.Get("tls_fdb")
		if v, ok := tlsConfig.(map[string]interface{}); ok && v != nil {
			no := fdb.Options()
			// verifyOptions := "O=Adobe, Inc., CN=fdb.adobe.net"
			verifyOptions := "Check.Valid=0"
			err = no.SetTLSVerifyPeers([]byte(verifyOptions))
			if err != nil {
				log.Fatalf("%+v", fmt.Errorf("unable to set verify options to >%s<: %w", verifyOptions, err))
			}
			certFile, ok := v["cert"].(string)
			if !ok || certFile == "" {
				log.Fatalf("%+v", errors.New("\"tls\" key must include a string subkey \"cert\""))
			}
			err = no.SetTLSCertPath(certFile)
			if err != nil {
				log.Fatalf("%+v", fmt.Errorf("unable to set cert path to %s: %w", certFile, err))
			}
			gLogger.Debug("Setting cert file to", zap.String("file", certFile))
			privKeyFile, ok := v["privkey"].(string)
			if !ok || privKeyFile == "" {
				log.Fatalf("%+v", errors.New("\"tls\" key must include a string subkey \"privkey\""))
			}
			err = no.SetTLSKeyPath(privKeyFile)
			if err != nil {
				log.Fatalf("%+v", fmt.Errorf("unable to set private key path to %s: %w", privKeyFile, err))
			}
			gLogger.Debug("Setting private key file to", zap.String("file", privKeyFile))

			caFile, ok := v["ca"].(string)
			if !ok || caFile == "" {
				log.Fatalf("%+v", errors.New("\"tls\" key must include a string subkey \"ca\""))
			}
			err = no.SetTLSCaPath(caFile)
			if err != nil {
				log.Fatalf("%+v", fmt.Errorf("unable to set ca file path to %s: %w", caFile, err))
			}
			gLogger.Debug("Setting CA file to", zap.String("file", caFile))
		}
		gFDB = fdb.MustOpenDefault()
		gFDBinitalized = true
	}
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
			// CAN'T USE ZAP - Logger not initilized yet
			fmt.Printf("Error finding home directory: %+v\n", err)
			os.Exit(1)
		}

		// Search config in home directory with name ".ferry" (without extension).
		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigName(".ferry")
	}
	viper.SetDefault("port", 8001)
	viper.SetDefault("threads", 10)

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		// CAN'T USE ZAP - Logger not initilized yet
		fmt.Printf("Using config file: %+v\n", viper.ConfigFileUsed())
	}

	for _, v := range []string{"port"} { // PERSISTENT FLAGS SET AT ROOT
		if pf := rootCmd.PersistentFlags().Lookup(v); pf != nil {
			err := viper.BindPFlag(v, pf)
			if err != nil {
				// CAN'T USE ZAP - Logger not initilized yet
				fmt.Printf("Error from BindPFlag (rootCmd): %+v\n", err)
				os.Exit(1)
			}
		} else {
			// CAN'T USE ZAP - Logger not initilized yet
			fmt.Println("Unknown flag ", v)
			os.Exit(1)
		}
	}

	// FLAGS SPECIFIC TO EXPORT
	for _, v := range []string{"dryrun", "sample", "compress", "threads", "collect"} {
		if pf := exportCmd.Flags().Lookup(v); pf != nil {
			err := viper.BindPFlag(v, pf)
			if err != nil {
				// CAN'T USE ZAP - Logger not initilized yet
				fmt.Printf("Error from BindPFlag (exportCmd): %+v\n", err)
				os.Exit(1)
			}
		} else {
			// CAN'T USE ZAP - Logger not initilized yet
			fmt.Println("Unknown flag ", v)
			os.Exit(1)
		}
	}
	// FLAGS SPECIFIC TO STATLOCAL
	for _, v := range []string{"checksum", "threads"} {
		if pf := statLocalCmd.Flags().Lookup(v); pf != nil {
			err := viper.BindPFlag(v, pf)
			if err != nil {
				// CAN'T USE ZAP - Logger not initilized yet
				fmt.Printf("Error from BindPFlag (statLocalCmd): %+v\n", err)
				os.Exit(1)
			}
		} else {
			// CAN'T USE ZAP - Logger not initilized yet
			fmt.Println("Unknown flag ", v)
			os.Exit(1)
		}
	}

	if profilingRequested != "" {
		if p, ok := profilesAvailable[profilingRequested]; ok {
			fmt.Printf("Starting profile >%s<\n", profilingRequested)
			ProfileStarted = profile.Start(p, profile.ProfilePath("."))
		} else {
			// CAN'T USE ZAP - Logger not initilized yet
			fmt.Printf("Unknown profiling mode: %s\n", profilingRequested)
			os.Exit(1)
		}
	}

	zapLevel := zapcore.InfoLevel
	if verbose {
		fmt.Println("Verbose logging")
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
		// CAN'T USE ZAP - Logger not initilized yet
		fmt.Println(err)
		os.Exit(1)
	}

	initFDB()
}
