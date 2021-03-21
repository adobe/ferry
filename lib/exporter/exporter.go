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

package exporter

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"github.com/adobe/blackhole/lib/archive"
	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Exporter struct {
	db             fdb.Database
	targetURL      string
	readerKeysChan chan fdb.Range
	readerStatChan chan readerStat
	logger         *zap.Logger
}

type readerStat struct {
	keysRead  int64
	bytesRead int64
}

func NewExporter(db fdb.Database, targetURL string, logger *zap.Logger) *Exporter {

	exp := &Exporter{db: db, targetURL: targetURL}
	exp.readerKeysChan = make(chan fdb.Range)
	exp.readerStatChan = make(chan readerStat)
	exp.logger = logger

	go exp.printStats()
	return exp
}

const MAX_KEY_LEN = (1 << 14) - 1   // Max 14 bits for its length. We only need 10k. Buffer till 16k
const MAX_VALUE_LEN = (1 << 18) - 1 // Max 18 bits for its length. We only need 100k. Buffer till 260k

func saveRecord(ar archive.Archive, key, value []byte) (err error) {
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
		log.Printf("%s: Error: %+v", msg, err)
		return errors.Wrap(err, msg)
	}

	n, err = ar.Write(key)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, keyLen)
		log.Printf("%s: Error: %+v", msg, err)
		return errors.Wrap(err, msg)
	}

	n, err = ar.Write(value)
	if err != nil {
		msg := fmt.Sprintf("FATAL: Wrote only %d bytes, %d expected.", n, valueLen)
		log.Printf("%s: Error: %+v", msg, err)
		return errors.Wrap(err, msg)
	}
	return err
}

func (exp *Exporter) printStats() {
	var totalKeysRead, totalBytesRead int64
	var totalKeysLastPrinted int64
	for stat := range exp.readerStatChan {
		totalBytesRead += stat.bytesRead
		totalKeysRead += stat.keysRead
		if totalKeysRead-totalKeysLastPrinted > 1_000_000 {
			log.Printf("PROGRESS: Read %d keys, %d bytes so far", totalKeysRead, totalBytesRead)
			totalKeysLastPrinted = totalKeysRead
		}
	}
	log.Printf("FINAL: Read %d keys, %d bytes so far", totalKeysRead, totalBytesRead)
}

func (exp *Exporter) dbReader(thread int) (err error) {

	totalKeysRead := 0
	totalKeysArchived := 0
	log.Printf("targetURL= %s", exp.targetURL)
	ar, err := archive.NewArchive(exp.targetURL, "fdb", ".records",
		common.Compress(false),
		common.BufferSize(0),
		common.Logger(exp.logger))
	if err != nil {
		return errors.Wrapf(err, "Unable to create archive file")
	}

	for keyRange := range exp.readerKeysChan {
		txn, err := exp.db.CreateTransaction()
		if err != nil {
			return errors.Wrapf(err, "Unable to create fdb transaction")
		}

		fKey := txn.GetRange(keyRange, fdb.RangeOptions{Limit: 1_000_000, Mode: fdb.StreamingModeSerial})
		it := fKey.Iterator()
		keysRead := 0
		bytesRead := 0
		for it.Advance() {
			kv, err := it.Get()
			if err != nil {
				txn.Commit()
				return errors.Wrapf(err, "Unable to create fdb transaction")
			}
			keysRead++
			bytesRead += len(kv.Key) + len(kv.Value)
			saveRecord(ar, kv.Key, kv.Value)
			totalKeysRead++
		}
		// log.Printf("NEXT: Read %d keys %d bytes", keysRead, bytesRead)
		txn.Commit()
		exp.readerStatChan <- readerStat{keysRead: int64(keysRead), bytesRead: int64(bytesRead)}
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

func (exp *Exporter) Export() error {

	var boundaryKeys []fdb.Key
	beginKey := fdb.Key("")

	for {
		bKeys, err := exp.db.LocalityGetBoundaryKeys(fdb.KeyRange{Begin: beginKey, End: fdb.Key("\xFF")},
			1000, 0)
		if err != nil {
			return errors.Wrapf(err, "Error querying LocalityGetBoundaryKeys")
		}
		if len(bKeys) > 1 ||
			// we must get at least one additional key than what we passed in
			// only keys from position 1 and later is really new
			// except for the boundary case when we first pass in '' as beginKey
			// In that rare case the DB only has one key in total, a single key
			// would return to us and we should still consider it a valid one to
			// save. That boundary case is the expression below.
			(len(boundaryKeys) == 0 && len(bKeys) == 1) {

			log.Printf("%+v", bKeys)
			beginKey = bKeys[len(bKeys)-1].FDBKey()
			log.Printf("Last key is %+v", beginKey)

			boundaryKeys = append(boundaryKeys, bKeys...)
		} else {
			break
		}
	}
	log.Printf("All keys: %+v", boundaryKeys)

	var wg sync.WaitGroup
	readerThreads := 10

	log.Printf("Starting %d reader threads", readerThreads)
	for i := 0; i < readerThreads; i++ {
		wg.Add(1)
		go func(threadNum int, wg *sync.WaitGroup) {
			defer wg.Done()
			err := exp.dbReader(threadNum)
			if err != nil {
				log.Printf("ERROR in dbReader thread: %+v", err) // Print before goroutine exists
			}
		}(i, &wg)
	}

	// Start work feeder
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		wg.Done()
		for i, beginKey := range boundaryKeys {
			var endKey fdb.Key
			if i == len(boundaryKeys)-1 { // are we on last key?
				endKey = fdb.Key("\xFF")
			} else {
				endKey = boundaryKeys[i+1]
			}
			exp.readerKeysChan <- fdb.KeyRange{Begin: beginKey, End: endKey}
		}
		close(exp.readerKeysChan)
	}(&wg)

	log.Printf("Waiting for all threads to finish")
	wg.Wait()
	close(exp.readerStatChan)

	// Exporter object is only usable once because of all the goroutine magic
	exp.readerKeysChan, exp.readerStatChan = nil, nil
	return nil
}
