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

package session

import (
	"encoding/binary"
	"fmt"
	"io"
	"sync"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const MAX_KEY_LEN = (1 << 14) - 1   // Max 14 bits for its length. We only need 10k. Buffer till 16k
const MAX_VALUE_LEN = (1 << 18) - 1 // Max 18 bits for its length. We only need 100k. Buffer till 260k

func (es *ImporterSession) readRecord(ar archive.Archive) (key, value []byte, err error) {
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
		es.logger.Error("short read",
			zap.Int("got", n),
			zap.Int("expected", len(lbuf)),
		)
		return nil, nil, errors.New("[1] Unexpected EOF for archive file")
	}
	recordLen := binary.LittleEndian.Uint32(lbuf)
	keyLen := recordLen >> 18
	valueLen := recordLen & MAX_KEY_LEN

	if keyLen > 1000 {
		return nil, nil, errors.Errorf("Long key (%d, %d, %d)",
			recordLen, keyLen, valueLen)
	}
	key = make([]byte, keyLen)
	value = make([]byte, valueLen)
	n, err = io.ReadFull(ar, key)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "[2] Error reading archive file")
	}
	if n != len(key) {
		es.logger.Error("short read",
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
		es.logger.Error("short read",
			zap.Int("got", n),
			zap.Int("expected", len(value)),
		)
		return nil, nil, errors.New("[3] Unexpected EOF for archive file")
	}
	return key, value, nil
}

func (es *ImporterSession) printStats(wg *sync.WaitGroup) {
	defer wg.Done()

	var totalKeysRead, totalBytesRead int64
	var totalKeysLastPrinted int64
	for stat := range es.writerStatChan {
		totalBytesRead += stat.bytesRead
		totalKeysRead += stat.keysRead
		if totalKeysRead-totalKeysLastPrinted > 1_000_000 {
			es.logger.Info("Progress", zap.Int64("keys", totalKeysRead), zap.Int64("bytes", totalBytesRead))
			totalKeysLastPrinted = totalKeysRead
		}
	}
	es.logger.Info("Session total", zap.Int64("keys", totalKeysRead), zap.Int64("bytes", totalBytesRead))
}

func (es *ImporterSession) dbWriter(thread int) (err error) {

	totalKeysWritten := int64(0)
	es.logger.Info("Importing from", zap.String("targetURL", es.targetURL))

	for fileName := range es.writerFilesChan {
		fqfn := fmt.Sprintf("%s/%s", es.targetURL, fileName)
		ar, err := archive.OpenArchive(fqfn, 4_000_000)
		if err != nil {
			return errors.Wrapf(err, "Unable to open export file %s", fqfn)
		}

		keysRead := 0
		bytesWritten := int64(0)
		var key, value []byte
		for {
			_, err = es.db.Transact(func(txn fdb.Transaction) (ret interface{}, e error) {
				for {
					key, value, err = es.readRecord(ar)
					if err == io.EOF {
						return nil, io.EOF
					}
					if err != nil {
						return nil, err
					}
					// txn.Set(fdb.Key(key), value)

					keysRead++
					bytesWritten += int64(len(key) + len(value))
					totalKeysWritten++
					if bytesWritten > 4_000_000 {
						return nil, nil
					}
				}
			})
			if err != io.EOF && err != nil {
				es.logger.Error("transaction error",
					zap.String("file", fqfn),
					zap.ByteString("key", key),
					zap.ByteString("value", value),
					zap.Int("keysRead", keysRead),
					zap.Int64("bytesWriten", bytesWritten),
				)
				return errors.Wrapf(err, "Write transaction error")
			}

			// log.Printf("NEXT: Read %d keys %d bytes", keysRead, bytesRead)
			es.writerStatChan <- writerStat{keysRead: int64(keysRead), bytesRead: int64(bytesWritten)}
			keysRead, bytesWritten = 0, 0 // reset to avoid double counting
			if err == io.EOF {
				break
			}
		}
		err = ar.Close()
		if err != nil {
			return errors.Wrapf(err, "Unable to close archive file")
		}
	}
	es.logger.Info("Importing from",
		zap.String("targetURL", es.targetURL),
		zap.Int64("totalKeysWritten", totalKeysWritten),
	)

	return err
}
