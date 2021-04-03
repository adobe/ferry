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
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ExporterSession struct {
	db             fdb.Database
	readerThreads  int
	targetURL      string
	sessionID      string
	readerKeysChan chan fdb.KeyRange
	readerStatChan chan readerStat
	wg             *sync.WaitGroup
	logger         *zap.Logger
	sync.Mutex     // Need a mutex to prevent thie session (and hence channel)
	inUse          bool
	// from being closed midway streaming.
}

type readerStat struct {
	keysRead  int64
	bytesRead int64
}

func NewSession(db fdb.Database, targetURL string, logger *zap.Logger) (es *ExporterSession, err error) {

	var wg = &sync.WaitGroup{}
	sessionID, err := uuid.NewRandom()
	if err != nil {
		es.logger.Warn("Failed to create a session ID", zap.Error(err))
		return nil, errors.Wrap(err, "Failed to create a session ID")
	}
	sessionIDstr := sessionID.String()
	es = &ExporterSession{
		db:             db,
		readerThreads:  10,
		logger:         logger,
		targetURL:      targetURL,
		sessionID:      sessionIDstr,
		readerKeysChan: make(chan fdb.KeyRange),
		readerStatChan: make(chan readerStat),
		wg:             wg,
	}

	es.logger.Info("Starting", zap.Int("reader threads", es.readerThreads))
	for i := 0; i < es.readerThreads; i++ {
		wg.Add(1)
		go func(threadNum int, wg *sync.WaitGroup) {
			defer wg.Done()
			err := es.dbReader(threadNum)
			if err != nil {
				es.logger.Error("Error in background thread",
					zap.Int("thread", threadNum),
					zap.Error(err))
			}
		}(i, wg)
	}

	go es.printStats()
	return es, nil
}

func (es *ExporterSession) GetSessionID() string {
	return es.sessionID
}

func (es *ExporterSession) Send(krange fdb.KeyRange) {
	es.readerKeysChan <- krange
}

func (exp *ExporterSession) MarkInUse(inUse bool) {
	exp.inUse = inUse
}

func (exp *ExporterSession) InUse() (inUse bool) {
	return exp.inUse
}

func (es *ExporterSession) Close() {
	close(es.readerKeysChan)
	es.wg.Wait()
}

const MAX_KEY_LEN = (1 << 14) - 1   // Max 14 bits for its length. We only need 10k. Buffer till 16k
const MAX_VALUE_LEN = (1 << 18) - 1 // Max 18 bits for its length. We only need 100k. Buffer till 260k

func (es *ExporterSession) saveRecord(ar archive.Archive, key, value []byte) (err error) {
	const UINT32LEN = 4
	var lbuf = make([]byte, UINT32LEN)

	keyLen := len(key)
	valueLen := len(value)
	if keyLen > MAX_KEY_LEN {
		return errors.Errorf("Sorry we only support key length up to %d bytes", MAX_KEY_LEN)
	}
	if valueLen > MAX_VALUE_LEN {
		return errors.Errorf("Sorry we only support value length up to %d bytes", MAX_VALUE_LEN)
	}
	recordLen := uint32(keyLen<<18 | valueLen)

	binary.LittleEndian.PutUint32(lbuf, recordLen)

	n, err := ar.Write(lbuf)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, UINT32LEN)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", UINT32LEN))
		return errors.Wrap(err, msg)
	}

	n, err = ar.Write(key)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, keyLen)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", keyLen))
		return errors.Wrap(err, msg)
	}

	n, err = ar.Write(value)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, valueLen)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", valueLen))
		return errors.Wrap(err, msg)
	}
	return err
}

func (es *ExporterSession) printStats() {
	var totalKeysRead, totalBytesRead int64
	var totalKeysLastPrinted int64
	for stat := range es.readerStatChan {
		totalBytesRead += stat.bytesRead
		totalKeysRead += stat.keysRead
		if totalKeysRead-totalKeysLastPrinted > 1_000_000 {
			es.logger.Debug("Progress", zap.Int64("keys", totalKeysRead), zap.Int64("bytes", totalBytesRead))
			totalKeysLastPrinted = totalKeysRead
		}
	}
	es.logger.Debug("Session total", zap.Int64("keys", totalKeysRead), zap.Int64("bytes", totalBytesRead))
}

func (es *ExporterSession) dbReader(thread int) (err error) {

	totalKeysRead := 0
	totalKeysArchived := 0
	es.logger.Info("Exporting to", zap.String("targetURL", es.targetURL))
	ar, err := archive.NewArchive(es.targetURL, "fdb", ".records",
		common.Compress(false),
		common.BufferSize(0),
		common.Logger(es.logger))
	if err != nil {
		return errors.Wrapf(err, "Unable to create archive file")
	}
	defer ar.Close()

	for keyRange := range es.readerKeysChan {
		txn, err := es.db.CreateTransaction()
		if err != nil {
			return errors.Wrapf(err, "Unable to create fdb transaction")
		}

		keysRead := 0
		keysReadInOneTxn := 0
		keysReadInOneBatch := 0
		bytesRead := int64(0)
		lastReadKey, endKey := keyRange.FDBRangeKeys()
		batchReadLimit := 1_000_000
	Fetch:
		for {
			fKey := txn.GetRange(keyRange, fdb.RangeOptions{Limit: batchReadLimit, Mode: fdb.StreamingModeSerial})
			it := fKey.Iterator()
			for it.Advance() {
				// ---------------------------------------------------------
				// uncomment line below for testing only
				// time.Sleep(time.Millisecond * 1)
				// This is to artifically create the 5 second txn limit test
				// ---------------------------------------------------------
				kv, err := it.Get()
				if err != nil {
					txn.Commit()
					if errFDB, ok := err.(fdb.Error); ok && errFDB.Code == 1007 { // txn too old

						es.logger.Info("Txn limit hit, restarting txn",
							zap.Int("after", keysReadInOneTxn))
						// don't print key. Don't assume key is utf8 string
						// zap.String("key", lastReadKey.FDBKey().String()))

						txn, err = es.db.CreateTransaction()
						if err != nil {
							return errors.Wrapf(err, "Unable to create fdb transaction")
						}
						keyRange = fdb.KeyRange{Begin: lastReadKey, End: endKey}
						keysReadInOneTxn = 0
						continue Fetch
						// continue from where we last received
					}
					return errors.Wrapf(err, "Unable to create fdb transaction")
				}
				if keysReadInOneTxn == 0 && keysRead != 0 && bytes.Equal(lastReadKey.FDBKey(), kv.Key) {
					// When retrying transactions, we don't have a way to ask for
					// starting from the 'next' key because we don't know what the next key is.
					// We will need to give the same key as the beginKey for next try,
					// and skip that first row when we get it back. beginKey is inclusive.
					continue
				}

				keysRead++
				keysReadInOneTxn++
				keysReadInOneBatch++
				bytesRead += int64(len(kv.Key) + len(kv.Value))
				es.saveRecord(ar, kv.Key, kv.Value)
				lastReadKey = kv.Key
				totalKeysRead++
			}

			if keysReadInOneBatch >= batchReadLimit {
				// there might be more left.
				es.logger.Info("Batch limit hit, starting another batch",
					zap.Int("after", keysReadInOneBatch),
					zap.Int("total", keysRead))
				keysReadInOneBatch = 0
				keyRange = fdb.KeyRange{Begin: lastReadKey, End: endKey}
				continue
			}

			break // we are really done
		}
		// log.Printf("NEXT: Read %d keys %d bytes", keysRead, bytesRead)
		txn.Commit()
		es.readerStatChan <- readerStat{keysRead: int64(keysRead), bytesRead: int64(bytesRead)}
		keysRead, bytesRead = 0, 0 // reset to avoid double counting
		if totalKeysRead-totalKeysArchived > 1_000_000 {
			// Rotate every 1 million keys
			totalKeysArchived = totalKeysRead
			err = ar.Rotate()
			if err != nil {
				return errors.Wrapf(err, "Unable to rotate archive file")
			}
		}
	}
	return err
}
