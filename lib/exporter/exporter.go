package exporter

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"log"
	"sync"
)

type Exporter struct {
	db fdb.Database
	readerKeysChan chan fdb.Range
	readerStatChan chan readerStat
}

type readerStat struct {
	keysRead  int64
	bytesRead int64
}

func NewExporter(db fdb.Database, targetURL string) *Exporter {

	exp := &Exporter{db: db}
	exp.readerKeysChan = make(chan fdb.Range)
	exp.readerStatChan = make(chan readerStat)

	go exp.printStats()
	return exp
}

func (exp *Exporter) printStats() {
	var totalKeysRead, totalBytesRead int64
	for stat := range exp.readerStatChan {
		totalBytesRead += stat.bytesRead
		totalKeysRead += stat.keysRead
		log.Printf("PROGRESS: Read %d keys, %d bytes so far", totalKeysRead, totalBytesRead)
	}
	log.Printf("FINAL: Read %d keys, %d bytes so far", totalKeysRead, totalBytesRead)
}

func (exp *Exporter) dbReader(thread int) (err error) {

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
		}
		// log.Printf("NEXT: Read %d keys %d bytes", keysRead, bytesRead)
		txn.Commit()
		exp.readerStatChan <- readerStat{keysRead: int64(keysRead), bytesRead: int64(bytesRead)}
		keysRead, bytesRead = 0, 0 // reset to avoid double counting
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
