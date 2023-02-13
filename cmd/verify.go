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
	"encoding/binary"
	"io"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var fileName string

// statusCmd represents the manage command
var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "❌ Verify single archive file",
	Long:  `Verify single archive file`,

	Run: func(cmd *cobra.Command, args []string) {

		ar, err := archive.OpenArchive(fileName, 4_000_000)
		if err != nil {
			gLogger.Fatal("Error",
				zap.Error(errors.Wrapf(
					err, "Unable to open export file %s", fileName)))
		}

		for {
			key, value, err := readRecord(ar)
			if err == io.EOF {
				gLogger.Info("End of file")
				break
			}
			if err != nil {
				gLogger.Error("Verification error",
					zap.Error(err))
				break
			}
			if viper.GetBool("verbose") {
				gLogger.Info("KV", zap.ByteString("key", key),
					zap.ByteString("value", value))
			}
		}
	},
}

const MAX_KEY_LEN = (1 << 14) - 1   // Max 14 bits for its length. We only need 10k. Buffer till 16k
const MAX_VALUE_LEN = (1 << 18) - 1 // Max 18 bits for its length. We only need 100k. Buffer till 260k

func readRecord(ar archive.Archive) (key, value []byte, err error) {
	const UINT32LEN = 4
	var lbuf = make([]byte, UINT32LEN)

	n, err := io.ReadFull(ar, lbuf)
	if err != nil {
		if err == io.EOF {
			return nil, nil, err // io.EOF - Not an error
		}
		return nil, nil, errors.Wrapf(err, "[1] Error reading archive file")
	}
	if n != len(lbuf) {
		gLogger.Error("short read",
			zap.Int("got", n),
			zap.Int("expected", len(lbuf)),
		)
		return nil, nil, errors.New("[1] Unexpected EOF for archive file")
	}
	recordLen := binary.LittleEndian.Uint32(lbuf)
	keyLen := recordLen >> 18
	valueLen := recordLen & MAX_KEY_LEN

	if keyLen > 1000 {
		gLogger.Error("Long key",
			zap.Uint32("key", keyLen),
			zap.Uint32("valueLen", valueLen),
		)
		return key, value, nil
	}
	key = make([]byte, keyLen)
	value = make([]byte, valueLen)
	n, err = io.ReadFull(ar, key)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "[2] Error reading archive file")
	}
	if n != len(key) {
		gLogger.Error("short read",
			zap.Int("got", n),
			zap.Int("expected", len(key)),
		)
		return nil, nil, errors.New("[2] Unexpected EOF for archive file")
	}
	n, err = io.ReadFull(ar, value)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "[3] Error reading archive file")
	}
	if n != len(value) {
		gLogger.Error("short read",
			zap.Int("got", n),
			zap.Int("expected", len(value)),
		)
		return nil, nil, errors.New("[3] Unexpected EOF for archive file")
	}
	return key, value, nil
}

func init() {
	//rootCmd.AddCommand(verifyCmd)

	// ------------------------------------------------------------------------
	// PLEASE DO NOT SET ANY "DEFAULTS" for CLI arguments. Set them instead as
	// viper.SetDefault() in root.go. Then it will apply to both paths. If you
	// set them here, it will always override what is in .ferry.yaml (making the
	// config file useless)
	// ------------------------------------------------------------------------
	verifyCmd.Flags().StringVarP(&fileName, "file", "f", "", "File to check")
}
