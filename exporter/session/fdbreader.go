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
	"math/rand"
	"sync"
	"time"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

const MAX_KEY_LEN = (1 << 14) - 1   // Max 14 bits for its length. We only need 10k. Buffer till 16k
const MAX_VALUE_LEN = (1 << 18) - 1 // Max 18 bits for its length. We only need 100k. Buffer till 260k

func (es *ExporterSession) saveRecord(ar archive.Archive, key, value []byte) (bytesTotal int, err error) {
	const UINT32LEN = 4
	var lbuf = make([]byte, UINT32LEN)
	var n int

	keyLen := len(key)
	valueLen := len(value)
	if keyLen > MAX_KEY_LEN {
		return 0, errors.Errorf("Sorry we only support key length up to %d bytes", MAX_KEY_LEN)
	}
	if valueLen > MAX_VALUE_LEN {
		return 0, errors.Errorf("Sorry we only support value length up to %d bytes", MAX_VALUE_LEN)
	}
	recordLen := uint32(keyLen<<18 | valueLen)

	binary.LittleEndian.PutUint32(lbuf, recordLen)

	n, err = ar.Write(lbuf)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, UINT32LEN)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", UINT32LEN))
		return 0, errors.Wrap(err, msg)
	}
	bytesTotal += n

	n, err = ar.Write(key)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, keyLen)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", keyLen))
		return 0, errors.Wrap(err, msg)
	}
	bytesTotal += n

	n, err = ar.Write(value)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, valueLen)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", valueLen))
		return 0, errors.Wrap(err, msg)
	}
	bytesTotal += n

	return bytesTotal, err
}

func (es *ExporterSession) saveKeysPlainText(ar archive.Archive, key []byte) (bytesTotal int, err error) {
	var n int

	keyLen := len(key)
	n, err = ar.Write(key)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, keyLen)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", keyLen))
		return 0, errors.Wrap(err, msg)
	}
	bytesTotal += n

	n, err = ar.Write([]byte("\n"))
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, 2 expected.", n)
		es.logger.Error(msg, zap.Int("wrote", n), zap.Int("expected", 2))
		return 0, errors.Wrap(err, msg)
	}
	bytesTotal += n

	return bytesTotal, err
}

func (es *ExporterSession) printStats(wg *sync.WaitGroup) {
	defer wg.Done()

	var totalKeysRead, totalBytesRead int64
	var totalKeysLastPrinted int64
	var startTime = time.Now()
	for stat := range es.readerStatChan {
		totalBytesRead += stat.bytesSaved
		totalKeysRead += stat.keysRead
		if totalKeysRead-totalKeysLastPrinted > 1_000_000 {
			seconds := time.Since(startTime).Seconds()
			es.logger.Info("Progress",
				zap.Int64("keys", totalKeysRead),
				zap.Int64("bytes", totalBytesRead),
				zap.Int64("keys/s", int64((float64(totalKeysRead)/seconds))),
				zap.Int64("bps", int64((float64(totalBytesRead)/seconds))),
			)

			totalKeysLastPrinted = totalKeysRead
		}
	}

	es.results.Lock()
	es.logger.Info("Session total", zap.Int64("keys", totalKeysRead), zap.Int64("bytes", totalBytesRead))
	for k, v := range es.results.finalizedDetails {
		es.logger.Info("SUMMARY",
			zap.Any("range", k),
			zap.Int("content-length", int(v.ChunksWritten)))
	}
	es.results.Unlock()

}

func (es *ExporterSession) dbReader(thread int) (err error) {

	es.logger.Info("Exporting to", zap.String("targetURL", es.targetURL))

	for keyRange := range es.readerKeysChan {
		err := es.rangeReader(thread, keyRange)
		if err != nil {
			return errors.Wrap(err, "error from rangeReader")
		}
	}
	return err
}

func (es *ExporterSession) rangeReader(thread int, keyRange fdb.KeyRange) (err error) {

	ar, err := archive.NewArchive(es.targetURL, "fdb", ".records",
		common.Compress(es.compress),
		common.BufferSize(4096),
		common.Logger(es.logger))
	if err != nil {
		return errors.Wrapf(err, "Unable to create archive file")
	}
	defer ar.Close()

	txn, err := es.db.CreateTransaction()
	if err != nil {
		return errors.Wrapf(err, "Unable to create fdb transaction")
	}

	err = txn.Options().SetReadYourWritesDisable()
	if err != nil {
		return errors.Wrapf(err, "Unable to set transaction option")
	}

	rangeIdentifier := fmt.Sprintf("%s-%s",
		fdb.Printable(keyRange.Begin.FDBKey()),
		fdb.Printable(keyRange.End.FDBKey()))
	keysRead := int64(0)
	keysReadInThisTxn := 0
	bytesSaved := int64(0)
	lastReadKey, endKey := keyRange.FDBRangeKeys()
	batchReadLimit := 100000

Fetch:
	for {
		es.logger.Debug("Querying",
			zap.Int("thread", thread),
			zap.Int("after", keysReadInThisTxn),
			zap.String("begin-key", fdb.Printable(keyRange.Begin.FDBKey())),
			zap.String("end-key", fdb.Printable(keyRange.End.FDBKey())))
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

					// Reduce batchLimit to something smaller
					batchReadLimit = keysReadInThisTxn - keysReadInThisTxn%100
					if batchReadLimit < 100 {
						batchReadLimit = 100
					}

					// Round it down to a multiple of 100
					es.logger.Info("Txn limit hit",
						zap.Int("thread", thread),
						zap.Int("after", keysReadInThisTxn),
						zap.String("key", fdb.Printable(lastReadKey.FDBKey())),
						zap.Int("new batch limit", batchReadLimit))

					txn, err = es.db.CreateTransaction()
					if err != nil {
						return errors.Wrapf(err, "Unable to create fdb transaction")
					}
					keysReadInThisTxn = 0
					err = txn.Options().SetReadYourWritesDisable()
					if err != nil {
						return errors.Wrapf(err, "Unable to set transaction option")
					}
					keyRange = fdb.KeyRange{Begin: lastReadKey, End: endKey}
					continue Fetch
					// continue from where we last received
				}
				return errors.Wrapf(err, "Unable to read key range from fdb")
			}
			if keysReadInThisTxn == 0 && keysRead != 0 && bytes.Equal(lastReadKey.FDBKey(), kv.Key) {
				// When retrying transactions, we don't have a way to ask for
				// starting from the 'next' key because we don't know what the next key is.
				// We will need to give the same key as the beginKey for next try,
				// and skip that first row when we get it back. beginKey is inclusive.
				es.logger.Debug("Skipping repeat read of",
					zap.Int("thread", thread),
					zap.String("key", fdb.Printable(kv.Key.FDBKey())))

				continue
			}

			keysRead++
			keysReadInThisTxn++
			if len(kv.Key) > 2048 {
				es.logger.Warn("Invalid-key", zap.Int("keyLen", len(kv.Key)))
			}
			var n int
			if es.readPercent == 100 || rand.Intn(100) <= es.readPercent {
				if es.exportFormat == "archive" {
					n, err = es.saveRecord(ar, kv.Key, kv.Value)
				} else {
					n, err = es.saveKeysPlainText(ar, kv.Key)
				}
				if err != nil {
					es.logger.Error("saveRecord failed",
						zap.Int("thread", thread),
						zap.Int("after", keysReadInThisTxn),
						zap.Int64("total", keysRead),
						zap.Error(err))
					return errors.Wrapf(err, "Unable to save data locally")
				}
				bytesSaved += int64(n)
			} else {
				bytesSaved += int64(len(kv.Key) + len(kv.Value))
			}
			lastReadKey = kv.Key
		}

		if keysReadInThisTxn >= (batchReadLimit - 1) {
			// Reason to compare against `batchReadLimit-1`
			// We want to keep `keysReadInThisTxn` to be actual useful keys read
			// When we ask for 1000 keys and skip first one, we only get 999
			// there might be more left.
			// in es.samplingMode though, we stop after one "smaller" batch.
			// See override of batchReadLimit above
			es.logger.Debug("Batch limit hit, starting another batch",
				zap.Int("thread", thread),
				zap.Int("after", keysReadInThisTxn),
				zap.Int64("total", keysRead),
				zap.String("key", fdb.Printable(lastReadKey.FDBKey())))

			txn, err = es.db.CreateTransaction()
			if err != nil {
				return errors.Wrapf(err, "Unable to create fdb transaction")
			}
			err = txn.Options().SetReadYourWritesDisable()
			if err != nil {
				return errors.Wrapf(err, "Unable to set transaction option")
			}

			keysReadInThisTxn = 0
			keyRange = fdb.KeyRange{Begin: lastReadKey, End: endKey}
			continue
		}

		es.logger.Debug("Done  with key range",
			zap.Int("thread", thread),
			zap.Int("last-txn-read", keysReadInThisTxn),
			zap.String("range", rangeIdentifier),
			zap.Int64("keys", keysRead))

		break // we are really done
	}
	// log.Printf("NEXT: Read %d keys %d bytes", keysRead, bytesRead)
	txn.Commit()
	// fileName := ar.Name()
	es.readerStatChan <- readerStat{
		keysRead:   int64(keysRead),
		bytesSaved: int64(bytesSaved),
		//fileName:   fileName,
	}

	err = ar.Close()
	if err != nil {
		return errors.Wrapf(err, "Unable to close archive file")
	}
	finalizedDetails := ar.FinalizedFiles()

	es.results.Lock()
	for _, v := range finalizedDetails {
		v.RowsWritten = keysRead
		es.results.finalizedDetails[rangeIdentifier] = v
		es.results.finalizedFiles[v.FileName] = true

	}
	// es.logger.Debug("Results so far",
	//
	//	zap.Any("results", es.results.finalizedDetails))
	es.results.Unlock()
	return nil
}
