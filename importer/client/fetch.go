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

package client

import (
	"context"
	"fmt"
	"io"
	"sync"

	ferry "github.com/adobe/ferry/rpc"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (exp *ImporterClient) ScheduleImportByNode(eg importGroup, dryRun bool) (err error) {

	exp.logger.Info("Starting session to",
		zap.Int("files", len(eg.files)),
		zap.String("host", eg.host))
	resp, err := eg.conn.StartImportSession(context.Background(), &ferry.Target{
		TargetUrl:     exp.targetURL,
		SamplingMode:  exp.samplingMode,
		ReaderThreads: int32(exp.writerThreads),
	})
	if err != nil {
		return errors.Wrapf(err, "Unable to initiate session with peer")
	}
	sessionID := resp.SessionId

	if !dryRun {
		importClient, err := eg.conn.Import(context.Background())
		if err != nil {
			return errors.Wrapf(err, "Unable to initiate export session with peer")
		}

		for _, fileName := range eg.files {
			err = importClient.Send(
				&ferry.ImportRequest{
					FileName:  fileName,
					SessionId: sessionID,
				})
			if err != nil {
				return errors.Wrapf(err, "Unable to start import via import client")
			}
		}

		resp, err = importClient.CloseAndRecv()
		if err != nil && err != io.EOF {
			return errors.Wrapf(err, "Unable to flush queue on export client")
		}
		exp.logger.Info(fmt.Sprintf("%+v", resp))

	} else {
		exp.logger.Info("DRYRUN",
			zap.Int("files", len(eg.files)),
			zap.String("host", eg.host))
	}

	exp.logger.Info("Closing session to",
		zap.Int("files", len(eg.files)),
		zap.String("host", eg.host))

	resp, err = eg.conn.StopImportSession(context.Background(),
		&ferry.Session{SessionId: sessionID})
	if err != nil {
		return errors.Wrapf(err, "Error from StopSession")
	}
	exp.logger.Info("Export saved", zap.Int("files", len(resp.FinalizedFiles)))

	_, err = eg.conn.EndExportSession(context.Background(),
		&ferry.Session{SessionId: sessionID})
	if err != nil {
		return errors.Wrapf(err, "Error from EndSession")
	}

	return nil
}

func (exp *ImporterClient) ScheduleImport(importPlan map[string]importGroup) (err error) {

	var wg sync.WaitGroup
	for _, plan := range importPlan {
		wg.Add(1)
		go func(plan importGroup, wg *sync.WaitGroup) {
			defer wg.Done()
			err = exp.ScheduleImportByNode(plan, exp.dryRun)
			if err != nil {
				exp.logger.Error("Error from worker thread", zap.Error(err))
			}
		}(plan, &wg)
	}
	wg.Wait()
	return err
}
