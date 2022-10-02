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
	"os"
	"path"
	"strings"
	"sync"
	"time"

	ferry "github.com/adobe/ferry/rpc"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (exp *ExporterClient) ScheduleFetchByNode(eg exportGroup, dryRun bool) (err error) {

	exp.logger.Info("Starting session to",
		zap.Int("ranges", len(eg.kranges)),
		zap.String("host", eg.host))
	resp, err := eg.conn.StartExportSession(context.Background(), &ferry.Target{
		TargetUrl:     exp.targetURL,
		ReadPercent:   int32(exp.readPercent),
		ExportFormat:  exp.exportFormat,
		ReaderThreads: int32(exp.readerThreads),
		Compress:      exp.compress,
	})
	if err != nil {
		return errors.Wrapf(err, "Unable to initiate session with peer")
	}
	sessionID := resp.SessionId

	if !dryRun {
		exportClient, err := eg.conn.Export(context.Background())
		if err != nil {
			return errors.Wrapf(err, "Unable to initiate export session with peer")
		}

		for _, krange := range eg.kranges {
			err = exportClient.Send(&ferry.KeyRequest{
				Begin:     krange.Begin.FDBKey(),
				End:       krange.End.FDBKey(),
				SessionId: sessionID,
			})
			if err != nil {
				return errors.Wrapf(err, "Unable to send key via export client")
			}
		}
		resp, err = exportClient.CloseAndRecv()
		if err != nil && err != io.EOF {
			return errors.Wrapf(err, "Unable to flush queue on export client")
		}
		exp.logger.Info(fmt.Sprintf("%+v", resp))

	} else {
		exp.logger.Info("DRYRUN",
			zap.Int("ranges", len(eg.kranges)),
			zap.String("host", eg.host))
	}

	exp.logger.Info("Closing session to",
		zap.Int("ranges", len(eg.kranges)),
		zap.String("host", eg.host))

	resp, err = eg.conn.StopExportSession(context.Background(),
		&ferry.Session{SessionId: sessionID})
	if err != nil {
		return errors.Wrapf(err, "Error from StopSession")
	}
	exp.logger.Info("Export saved", zap.Int("files", len(resp.FinalizedFiles)))
	err = saveArchiveSummary(eg.host, resp.FinalizedFiles)
	if err != nil {
		return errors.Wrapf(err, "Error from saveArchiveSummary")
	}

	if exp.collectDir != "" && // --collect /foo/bar argument exists
		(!strings.Contains(exp.targetURL, "://") || // and it is a raw-path (not a s3:// type URL)
			strings.HasPrefix(exp.targetURL, "file://")) { // OR it is a file:// URL

		exp.logger.Info("Bringing files from each node",
			zap.String("dest", exp.collectDir))

		for _, finalFile := range resp.FinalizedFiles {
			if finalFile.ShellOnly {
				exp.logger.Info("Skipping meta-data-only file (CAN'T DOWNLOAD!)", zap.String("file", finalFile.FileName))
			} else {
				exp.logger.Info("Downloading", zap.String("file", finalFile.FileName))
				st := time.Now()
				fileSize := int64(0)
				gc, err := eg.conn.GetExportedFile(context.Background(),
					&ferry.FileRequest{
						SessionId: sessionID,
						TargetUrl: exp.targetURL,
						FileName:  finalFile.FileName,
					}) // Max 1 MB chunk. GRPC hard limit is 4 MB
				if err != nil {
					return errors.Wrapf(err, "Error from EndSession")
				}
				localPath := path.Join(exp.collectDir, finalFile.FileName)
				fp, err := os.OpenFile(localPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
				if err != nil {
					return errors.Wrapf(err, "Create of local file failed: %s", localPath)
				}
				for {
					block, err := gc.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						return errors.Wrapf(err, "Recv on block of file %s failed", finalFile)
					}
					fileSize += int64(len(block.BlockData))
					n, err := fp.Write(block.BlockData)
					if err != nil || n != len(block.BlockData) {
						return errors.Wrapf(err, "Write on block of file %s failed", localPath)
					}
				}
				err = fp.Close()
				if err != nil {
					return errors.Wrapf(err, "Write on block of file %s failed", localPath)
				}
				exp.logger.Info("Downloaded",
					zap.String("file", finalFile.FileName),
					zap.Int64("file-size", fileSize),
					zap.String("local-path", localPath),
					zap.Duration("duration", time.Since(st)),
				)
				_, err = eg.conn.RemoveExportedFile(context.Background(),
					&ferry.FileRequest{
						SessionId: sessionID,
						TargetUrl: exp.targetURL,
						FileName:  finalFile.FileName,
					})
				if err != nil {
					return errors.Wrapf(err, "Delete of source file %s failed", finalFile)
				}
			}
		}
	}

	_, err = eg.conn.EndExportSession(context.Background(),
		&ferry.Session{SessionId: sessionID})
	if err != nil {
		return errors.Wrapf(err, "Error from EndSession")
	}

	return nil
}
func saveArchiveSummary(host string, finalizedFiles []*ferry.FinalizedFile) (err error) {
	fileName := fmt.Sprintf("%s.out", host)
	fp, err := os.Create(fileName)
	if err != nil {
		return errors.Wrapf(err, "unable to save results files to >%s<", fileName)
	}
	for _, ff := range finalizedFiles {
		fmt.Fprintf(fp, "%s\t%s\t%d\t%d\t%s\t%t\n",
			ff.FileName, ff.KeyRange, ff.ContentSize,
			ff.RowCount, ff.Checksum, ff.ShellOnly)
	}
	fp.Close()
	return nil
}

func (exp *ExporterClient) ScheduleFetch(exportPlan map[string]exportGroup) (err error) {

	var wg sync.WaitGroup
	for _, plan := range exportPlan {
		wg.Add(1)
		go func(plan exportGroup, wg *sync.WaitGroup) {
			defer wg.Done()
			err = exp.ScheduleFetchByNode(plan, exp.dryRun)
			if err != nil {
				exp.logger.Error("Error from worker thread", zap.Error(err))
			}
		}(plan, &wg)
	}
	wg.Wait()
	return err
}
