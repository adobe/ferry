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

type ImporterSession struct {
	db              fdb.Database
	writerThreads   int
	targetURL       string
	sessionID       string
	writerFilesChan chan string
	writerStatChan  chan writerStat
	wgWriters       *sync.WaitGroup
	wgStaters       *sync.WaitGroup
	logger          *zap.Logger
	samplingMode    bool
}

type writerStat struct {
	keysRead  int64
	bytesRead int64
}

func NewSession(db fdb.Database, targetURL string, writerThreads int, logger *zap.Logger, samplingMode bool) (es *ImporterSession, err error) {

	sessionID, err := uuid.NewRandom()
	if err != nil {
		es.logger.Warn("Failed to create a session ID", zap.Error(err))
		return nil, errors.Wrap(err, "Failed to create a session ID")
	}
	sessionIDstr := sessionID.String()
	es = &ImporterSession{
		db:              db,
		writerThreads:   writerThreads,
		logger:          logger,
		targetURL:       targetURL,
		sessionID:       sessionIDstr,
		writerFilesChan: make(chan string),
		writerStatChan:  make(chan writerStat),
		wgWriters:       &sync.WaitGroup{},
		wgStaters:       &sync.WaitGroup{},
		samplingMode:    samplingMode,
	}

	if es.writerThreads <= 0 {
		es.writerThreads = 1
	}

	es.logger.Info("Starting", zap.Int("reader threads", es.writerThreads))
	for i := 0; i < es.writerThreads; i++ {
		es.wgWriters.Add(1)
		go func(threadNum int, wg *sync.WaitGroup) {
			defer wg.Done()
			err := es.dbWriter(threadNum)
			if err != nil {
				es.logger.Error("Error in background thread",
					zap.Int("thread", threadNum),
					zap.Error(err))
			}
		}(i, es.wgWriters)
	}

	es.wgStaters.Add(1)
	go es.printStats(es.wgStaters)
	return es, nil
}

func (es *ImporterSession) GetSessionID() string {
	return es.sessionID
}

func (es *ImporterSession) Send(fileName string) {
	es.writerFilesChan <- fileName
}

func (es *ImporterSession) Finalize() {

	// ---------------------------------------------------
	// WARNING: Order of channel close and .Wait()s are
	// important
	// ---------------------------------------------------
	// first make sure workers are finished
	// ---------------------------------------------------
	if es.writerFilesChan != nil {
		close(es.writerFilesChan)
		es.wgWriters.Wait()
		es.writerFilesChan = nil
	}

	// ---------------------------------------------------
	// then make sure results/stats from workers
	// are processed and printed
	// ---------------------------------------------------
	if es.writerStatChan != nil {
		close(es.writerStatChan)
		es.wgStaters.Wait()
		es.writerStatChan = nil
	}
}
