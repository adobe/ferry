package fdbstat

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"github.com/adobe/ferry/exporter/session"
	"github.com/adobe/ferry/finder"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type HashableKeyRange struct {
	Begin string
	End   string
}
type KeyRangeStats struct {
	Begin []byte
	End   []byte
	Size  int64
}

type DirSizeGuestimate struct {
	Size       int64
	LowerBound int64
	UpperBound int64
}
type PendingResponse struct {
	Kr HashableKeyRange
	Fi fdb.FutureInt64
}

func NewHashableKeyRange(kr fdb.KeyRange) HashableKeyRange {
	return HashableKeyRange{
		Begin: kr.Begin.FDBKey().String(), // uses fdb.Printable inside
		End:   kr.End.FDBKey().String(),   // uses fdb.Printable inside
	}
}

func NewKeyRangeStats(kr fdb.KeyRange, size int64) KeyRangeStats {
	s := KeyRangeStats{}
	s.Begin = append(s.Begin, kr.Begin.FDBKey()...)
	s.End = append(s.Begin, kr.End.FDBKey()...)
	s.Size = size
	return s
}
func (s *Surveyor) CalculateDBSize(pmap *finder.PartitionMap) (sizeByRange map[HashableKeyRange]KeyRangeStats, err error) {

	txn, err := s.db.CreateTransaction()
	if err != nil {
		return nil, err
	}
	sizeByRange = make(map[HashableKeyRange]KeyRangeStats)

	rangesAnalyzed := 0
	for _, v := range pmap.Ranges {

		for {
			kvKey := NewHashableKeyRange(v.Krange)
			var size int64
			size, err = txn.GetEstimatedRangeSizeBytes(v.Krange).Get()
			if err != nil {
				if errFDB, ok := err.(fdb.Error); ok && errFDB.Code == 1007 { // txn too old
					s.logger.Debug("Txn is old. Restarting")
					txn.Commit()
					txn, err = s.db.CreateTransaction()
					if err != nil {
						return nil, err
					}
					continue
				}
				return nil, err
			}
			sizeByRange[kvKey] = NewKeyRangeStats(v.Krange, size)

			s.logger.Debug("Estimated size",
				zap.String("begin", kvKey.Begin),
				zap.String("end", kvKey.End),
				zap.Int64("size", sizeByRange[kvKey].Size))
			rangesAnalyzed++
			if rangesAnalyzed%10 == 0 {
				s.logger.Info("CalculateDBSize", zap.Int("ranges analyzed", rangesAnalyzed))
			}
			break
		}
	}
	txn.Commit()

	return sizeByRange, nil
}

func (s *Surveyor) EstimateDirectorySize(sizeByRange map[HashableKeyRange]KeyRangeStats, directories DirListing) (err error) {

	dirEstimate := map[string]DirSizeGuestimate{}

	for dirName, dir := range directories {
		// Find out how many ranges does dir.Prefix span

		for _, v := range sizeByRange {

			dirEnd := append([]byte(nil), dir.Prefix...)
			dirEnd = append(dirEnd, byte('\xFF'))

			//s.logger.Info("!!!!DEBUG!!!!", zap.String("debug",
			//	fmt.Sprintf("Comparing %+v with %+v and %+v", dir.Prefix, v.Begin, v.End)))
			count_once_only := 0

			if bytes.Compare(dir.Prefix, v.Begin) >= 0 && bytes.Compare(dirEnd, v.End) < 0 {
				count_once_only++
				// DIRECTORY IS COMPLETELY CONTAINED WITHIN THE PARTITION RANGE
				x := dirEstimate[dirName]
				x.Size += v.Size
				// x.LowerBound += v // DON'T ADD TO LOWERBOUND since directory might just barely be in the range.
				x.UpperBound += v.Size
				dirEstimate[dirName] = x
			} else if bytes.Compare(dir.Prefix, []byte(v.Begin)) >= 0 && bytes.Compare(dir.Prefix, []byte(v.End)) < 0 && bytes.Compare(dirEnd, v.End) >= 0 {
				count_once_only++
				// DIRECTORY OVERLAPS PARTITION - Directory start is between partition BEGIN and END
				// DIRECTORY OVERLAPS PARTITION - Directory end is past partition END
				x := dirEstimate[dirName]
				x.Size += v.Size
				// x.LowerBound += v // DON'T ADD TO LOWERBOUND since directory might just barely be in the range.
				x.UpperBound += v.Size
				dirEstimate[dirName] = x
			} else if bytes.Compare(dir.Prefix, []byte(v.Begin)) < 0 && bytes.Compare(dirEnd, []byte(v.Begin)) >= 0 && bytes.Compare(dirEnd, v.End) < 0 {
				count_once_only++
				// DIRECTORY OVERLAPS PARTITION - Directory start is before BEGIN
				// DIRECTORY OVERLAPS PARTITION - Directory end is between partition BEGIN and END
				x := dirEstimate[dirName]
				x.Size += v.Size
				// x.LowerBound += v // DON'T ADD TO LOWERBOUND since directory might just barely be in the range.
				x.UpperBound += v.Size
				dirEstimate[dirName] = x
			} else if bytes.Compare(dirEnd, []byte(v.Begin)) < 0 || bytes.Compare(dir.Prefix, []byte(v.End)) >= 0 {
				count_once_only++
				// DIRECTORY IS COMPLETELY OUTSIDE THE PARTITION RANGE
			} else if bytes.Compare(dir.Prefix, []byte(v.Begin)) < 0 && bytes.Compare(dirEnd, []byte(v.End)) >= 0 {
				count_once_only++
				// PARTITION RANGE IS COMPLETELY WITHIN THE DIRECTORY RANGE
				x := dirEstimate[dirName]
				x.Size += v.Size
				x.LowerBound += v.Size // Range is well within directory range.
				x.UpperBound += v.Size
				dirEstimate[dirName] = x
			}
			if count_once_only != 1 {
				s.logger.Fatal("Logic bug!", zap.Int("count", count_once_only),
					zap.String("dirPrefix", fmt.Sprintf("%+v", dir.Prefix)),
					zap.String("dirEnd", fmt.Sprintf("%+v", dirEnd)),
					zap.String("range-begin", fmt.Sprintf("%+v", v.Begin)),
					zap.String("range-end", fmt.Sprintf("%+v", v.End)),
					zap.Int64("range-size", v.Size))
			}
		}
		x := dirEstimate[dirName]
		s.logger.Info("Directory size",
			zap.String("Directory", dirName),
			zap.Int64("Guess", x.Size),
			zap.Int64("Upperbound", x.UpperBound),
			zap.Int64("LowerBound", x.LowerBound))
	}
	return nil
}

func (s *Surveyor) CalculateDBSizeAsync(pmap *finder.PartitionMap) (sizeByRange map[HashableKeyRange]int64, totalSize int64, err error) {

	var numPartitions = len(pmap.Ranges)
	sizeByRange = make(map[HashableKeyRange]int64, numPartitions)
	var futures = make(chan PendingResponse, numPartitions)
	var wgFetcher sync.WaitGroup
	var wgRequester sync.WaitGroup
	var gErr error

	wgFetcher.Add(1)
	go func(f chan PendingResponse, w *sync.WaitGroup) {
		defer w.Done()
		for pr := range f {
			/*
				if !pr.Fi.IsReady() {
					// f <- pr
					// This will deadlock if buffered channel
					// doesn't have enough space.
					// make sure channel is pre-created with max-length
					fmt.Printf("Future for %s-%s not ready, sleeping\n", pr.Kr.Begin, pr.Kr.End)
					time.Sleep(10 * time.Millisecond)
					continue
				}
			*/
			var i64 int64
			t1 := time.Now()
			i64, gErr = pr.Fi.Get()
			if gErr == nil {
				sizeByRange[pr.Kr] = i64
			}
			fmt.Printf("Got result in %+v\n", time.Since(t1))
		}
	}(futures, &wgFetcher)

	txn, err := s.db.CreateTransaction()
	if err != nil {
		return nil, 0, err
	}
	schedule := func(f chan PendingResponse, ranges []finder.RangeLocation, w *sync.WaitGroup) {
		defer w.Done()
		t1 := time.Now()
		for _, v := range ranges {
			/*
				s.logger.Debug("Attempt", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
					zap.ByteString("end", v.Krange.End.FDBKey()),
					zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
			*/
			pr := PendingResponse{Kr: NewHashableKeyRange(v.Krange),
				Fi: txn.GetEstimatedRangeSizeBytes(v.Krange)}
			f <- pr
		}
		fmt.Printf("Scheduled request in %+v\n", time.Since(t1))
	}
	max := len(pmap.Ranges)
	step := 1000
	for i := 0; i < max; i += step {
		j := i + step
		if j > max {
			j = max
		}
		wgRequester.Add(1)
		go schedule(futures, pmap.Ranges[i:j], &wgRequester)
	}
	wgRequester.Wait() // Wait for all results to come in
	close(futures)
	wgFetcher.Wait() // Wait for all results to come in
	txn.Commit()

	return sizeByRange, totalSize, nil
}

func (s *Surveyor) CalculateRowCount(pmap *finder.PartitionMap, readerThreads int) (totalRows int64, err error) {

	es, err := session.NewSession(s.db,
		"",
		readerThreads,
		false,
		s.logger,
		100,
		"archive")
	if err != nil {
		s.logger.Warn("Failed to create a session ID", zap.Error(err))
		return 0, errors.Wrap(err, "Failed to create a session ID")
	}

	for _, v := range pmap.Ranges {
		//s.logger.Info("Attempt", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
		//	zap.ByteString("end", v.Krange.End.FDBKey()),
		//	zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
		es.Send(v.Krange)
	}
	es.Finalize()
	return 0, nil
}
