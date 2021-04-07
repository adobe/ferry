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
	wg             *sync.WaitGroup
	logger         *zap.Logger
	samplingMode   bool
	results        Results
}

type Results struct {
	finalizedFiles []string
	sync.Mutex
	// To facilitate concurrent access to slice above
	// since slice is updated at end-of-run only, the
	// performance penalty is OK.
}

type readerStat struct {
	keysRead  int64
	bytesRead int64
}

func NewSession(db fdb.Database, targetURL string, readerThreads int, compress bool, logger *zap.Logger, samplingMode bool) (es *ExporterSession, err error) {

	var wg = &sync.WaitGroup{}
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
		wg:             wg,
		samplingMode:   samplingMode,
	}

	if es.readerThreads <= 0 {
		es.readerThreads = 1
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

	wg.Add(1)
	go es.printStats(wg)
	return es, nil
}

func (es *ExporterSession) GetSessionID() string {
	return es.sessionID
}

func (es *ExporterSession) Send(krange fdb.KeyRange) {
	es.readerKeysChan <- krange
}

func (es *ExporterSession) Finalize() (finalPaths []string) {
	close(es.readerKeysChan)
	es.wg.Wait()
	es.logger.Warn("Finalize()", zap.Strings("files", es.results.finalizedFiles))
	return es.results.finalizedFiles
}
