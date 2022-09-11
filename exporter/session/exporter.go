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
	"sync"

	"github.com/adobe/blackhole/lib/archive/common"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type ExporterSession struct {
	db             fdb.Database
	readerThreads  int
	compress       bool
	targetURL      string
	sessionID      string
	readerKeysChan chan fdb.KeyRange
	readerStatChan chan readerStat
	wgReaders      *sync.WaitGroup
	wgStaters      *sync.WaitGroup
	logger         *zap.Logger
	samplingMode   bool
	results        Results
	// state          SessionState
}

type Results struct {
	finalizedFiles   map[string]bool
	finalizedDetails map[string]common.ArchiveFileDetails
	sync.Mutex
	// To facilitate concurrent access to slice above
	// since slice is updated at end-of-run only, the
	// performance penalty is OK.
}

type readerStat struct {
	keysRead   int64
	bytesSaved int64
	//fileName   string
}

func NewSession(db fdb.Database, targetURL string, readerThreads int, compress bool, logger *zap.Logger, samplingMode bool) (es *ExporterSession, err error) {

	sessionID, err := uuid.NewRandom()
	if err != nil {
		es.logger.Warn("Failed to create a session ID", zap.Error(err))
		return nil, errors.Wrap(err, "Failed to create a session ID")
	}
	sessionIDstr := sessionID.String()
	es = &ExporterSession{
		db:             db,
		readerThreads:  readerThreads,
		compress:       compress,
		logger:         logger,
		targetURL:      targetURL,
		sessionID:      sessionIDstr,
		readerKeysChan: make(chan fdb.KeyRange),
		readerStatChan: make(chan readerStat),
		wgReaders:      &sync.WaitGroup{},
		wgStaters:      &sync.WaitGroup{},
		samplingMode:   samplingMode,
	}

	es.results.finalizedDetails = make(map[string]common.ArchiveFileDetails)
	es.results.finalizedFiles = make(map[string]bool)
	if es.readerThreads <= 0 {
		es.readerThreads = 1
	}

	es.logger.Info("Starting", zap.Int("reader threads", es.readerThreads))
	for i := 0; i < es.readerThreads; i++ {
		es.wgReaders.Add(1)
		go func(threadNum int, wg *sync.WaitGroup) {
			defer wg.Done()
			err := es.dbReader(threadNum)
			if err != nil {
				es.logger.Error("Error in background thread",
					zap.Int("thread", threadNum),
					zap.Error(err))
			}
		}(i, es.wgReaders)
	}

	es.wgStaters.Add(1)
	go es.printStats(es.wgStaters)
	return es, nil
}

func (es *ExporterSession) GetSessionID() string {
	return es.sessionID
}

func (es *ExporterSession) IsResultFile(targetURL, fileName string) bool {
	_, ok := es.results.finalizedFiles[fileName]
	return ok && targetURL == es.targetURL
}

func (es *ExporterSession) Send(krange fdb.KeyRange) {
	es.readerKeysChan <- krange
}

func (es *ExporterSession) Finalize() (finalFiles []common.ArchiveFileDetails) {

	// ---------------------------------------------------
	// WARNING: Order of channel close and .Wait()s are
	// important
	// ---------------------------------------------------
	// first make sure workers are finished
	// ---------------------------------------------------
	if es.readerKeysChan != nil {
		close(es.readerKeysChan)
		es.wgReaders.Wait()
		es.readerKeysChan = nil
	}

	// ---------------------------------------------------
	// then make sure results/stats from workers
	// are processed and printed
	// ---------------------------------------------------
	if es.readerStatChan != nil {
		close(es.readerStatChan)
		es.wgStaters.Wait()
		es.readerStatChan = nil
	}

	for _, v := range es.results.finalizedDetails {
		finalFiles = append(finalFiles, v)
	}

	es.logger.Warn("Finalize()", zap.Any("files", finalFiles))
	return finalFiles
}
