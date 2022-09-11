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

package server

import (
	"context"
	"io"
	"os"
	"path"
	"strings"

	"github.com/adobe/ferry/exporter/session"
	ferry "github.com/adobe/ferry/rpc"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (exp *Server) StartExportSession(ctx context.Context, tgt *ferry.Target) (*ferry.SessionResponse, error) {

	es, err := session.NewSession(exp.db,
		tgt.TargetUrl,
		int(tgt.ReaderThreads),
		tgt.Compress,
		exp.logger,
		tgt.SamplingMode)
	if err != nil {
		exp.logger.Warn("Failed to create a session ID", zap.Error(err))
		return nil, errors.Wrap(err, "Failed to create a session ID")
	}

	sessionID := es.GetSessionID()
	exp.exportSessions.Store(sessionID, es)
	exp.logger.Info("Created session", zap.String("sessionID", sessionID))

	return &ferry.SessionResponse{SessionId: sessionID, Status: ferry.SessionResponse_SUCCESS}, err
}

func (exp *Server) Export(srv ferry.Ferry_ExportServer) error {
	currentSessionID := ""
	var es *session.ExporterSession

	for {

		req, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			exp.logger.Error("Stream receive failed",
				zap.String("last-known-sessionID", currentSessionID))
			if es != nil {
				// If `es` is set, it is assumed
				// to be pop-ed - cleanup resources
				es.Finalize()
			}
			return errors.Errorf("Single stream cannot have multiple session ids %s", currentSessionID)
		}
		if req.SessionId != currentSessionID {
			if currentSessionID != "" {
				exp.logger.Error("Single stream cannot have multiple session ids",
					zap.String("current-sessionID", currentSessionID),
					zap.String("new-sessionID", req.SessionId))
				if es != nil {
					// If `es` is set, it is assumed
					// to be pop-ed - cleanup resources
					es.Finalize()
				}
				return errors.Errorf("Single stream cannot have multiple session ids %s", currentSessionID)
			}
			currentSessionID = req.SessionId

			es, err = exp.popExportSession(currentSessionID)
			if err != nil {
				return err
			}
			exp.logger.Info("Start export stream", zap.String("sessionID", currentSessionID))
		}
		// At this point es will point to ExpoterSession we can use
		exp.logger.Debug("Sending to worker",
			zap.ByteString("begin", req.Begin),
			zap.ByteString("end", req.End),
		)
		es.Send(fdb.KeyRange{Begin: fdb.Key(req.Begin), End: fdb.Key(req.End)})
	}

	if es != nil {
		// Store it back. This is critical, we had *deleted* it earlier
		// Storing it back is how we indicate it is now free to be acquired
		// for cleanup
		exp.exportSessions.Store(currentSessionID, es)
	}

	return nil
}

func (exp *Server) popExportSession(sessionID string) (es *session.ExporterSession, err error) {

	var esi interface{}
	var ok bool

	esi, ok = exp.exportSessions.LoadAndDelete(sessionID)
	if !ok {
		return nil, errors.Errorf("Invalid session id OR Session is in use - %s", sessionID)
	}
	es, ok = esi.(*session.ExporterSession)
	if !ok {
		return nil, errors.Errorf("Corrupted tracker for session id %s", sessionID)
	}
	return es, nil
}
func (exp *Server) StopExportSession(ctx context.Context, fs *ferry.Session) (*ferry.SessionResponse, error) {

	var es *session.ExporterSession

	exp.logger.Info("Received StopSession", zap.String("sessionID", fs.SessionId))

	es, err := exp.popExportSession(fs.SessionId)
	if err != nil {
		return nil, err
	}
	// Very Import: Release session after use
	// "release" is done by putting it back in map
	// else the session will be GC-ed.
	defer exp.exportSessions.Store(fs.SessionId, es)

	exp.logger.Debug("Releasing resources", zap.String("sessionID", fs.SessionId))
	finalPaths := es.Finalize()
	exp.logger.Info("Released resources", zap.String("sessionID", fs.SessionId))

	var ferryFinalizedFiles []*ferry.FinalizedFile
	for _, v := range finalPaths {
		x := &ferry.FinalizedFile{}
		x.Checksum = v.Checksum
		x.ChunksCount = v.ChunksWritten
		x.ContentSize = v.BytesWritten
		x.FileName = v.FileName
		ferryFinalizedFiles = append(ferryFinalizedFiles, x)
	}

	return &ferry.SessionResponse{
		SessionId:      fs.SessionId,
		Status:         ferry.SessionResponse_SUCCESS,
		FinalizedFiles: ferryFinalizedFiles,
	}, nil
}

func (exp *Server) EndExportSession(ctx context.Context, fs *ferry.Session) (*ferry.SessionResponse, error) {

	var es *session.ExporterSession

	exp.logger.Info("Received EndSession", zap.String("sessionID", fs.SessionId))

	es, err := exp.popExportSession(fs.SessionId)
	if err != nil {
		return nil, err
	}
	// Very Import: this (EndSession) is the only situation
	// a 'pop' is not followed by a
	// 	"defer exp.sessions.Store(fr.SessionId, es)"

	exp.logger.Debug("Releasing resources", zap.String("sessionID", fs.SessionId))
	finalPaths := es.Finalize()
	exp.logger.Info("Released resources", zap.String("sessionID", fs.SessionId))

	var ferryFinalizedFiles []*ferry.FinalizedFile
	for _, v := range finalPaths {
		x := &ferry.FinalizedFile{}
		x.Checksum = v.Checksum
		x.ChunksCount = v.ChunksWritten
		x.ContentSize = v.BytesWritten
		x.FileName = v.FileName
		ferryFinalizedFiles = append(ferryFinalizedFiles, x)
	}

	return &ferry.SessionResponse{
		SessionId:      fs.SessionId,
		Status:         ferry.SessionResponse_SUCCESS,
		FinalizedFiles: ferryFinalizedFiles,
	}, nil
}

func (exp *Server) GetExportedFile(fr *ferry.FileRequest, resp ferry.Ferry_GetExportedFileServer) (err error) {

	var es *session.ExporterSession
	es, err = exp.popExportSession(fr.SessionId)
	if err != nil {
		return err
	}
	// Very Import: Release session after use
	// "release" is done by putting it back in map
	// else the session will be GC-ed.
	defer exp.exportSessions.Store(fr.SessionId, es)

	if !es.IsResultFile(fr.TargetUrl, fr.FileName) {
		return errors.Errorf("The tuple (%s, %s) is not part of the result set",
			fr.TargetUrl, fr.FileName)
	}

	if strings.Contains(fr.TargetUrl, "://") && !strings.HasPrefix(fr.TargetUrl, "file://") {
		return errors.New("This method is only implemented for targets of file://")
	}
	fr.TargetUrl = strings.TrimPrefix(fr.TargetUrl, "file://")
	fp, err := os.Open(path.Join(fr.TargetUrl, fr.FileName))
	if err != nil {
		return errors.Wrapf(err, "Error opening node-local file: %s", fr.FileName)
	}
	blockSize := 1_000_000
	buf := make([]byte, blockSize)

	blockNum := 0
	eof := false
	for {
		n, err := fp.Read(buf)
		if err != nil {
			if err == io.EOF {
				eof = true // don't bail yet. Have data to process
			} else {
				return errors.Wrapf(err, "Error reading file: %s", fr.FileName)
			}
		}
		if n != 0 { // note: `err` could still has unhandled `io.EOF` value
			// there is data to send
			blockNum++
			errGrpc := resp.Send(&ferry.FileRequestResponse{
				FileName:  fr.FileName,
				BlockNum:  int32(blockNum),
				BlockData: buf[:n], // IMPORTANT: use only up to n bytes
			})
			if errGrpc != nil {
				return errors.Wrapf(errGrpc, "Error from grpc stream send for %s", fr.FileName)
			}
		}
		if eof {
			break
		}
	}
	return nil
}

func (exp *Server) RemoveExportedFile(ctx context.Context, fr *ferry.FileRequest) (resp *ferry.FileRequest, err error) {

	var es *session.ExporterSession
	es, err = exp.popExportSession(fr.SessionId)
	if err != nil {
		return nil, err
	}
	// Very Import: Release session after use
	// "release" is done by putting it back in map
	// else the session will be GC-ed.
	defer exp.exportSessions.Store(fr.SessionId, es)

	if !es.IsResultFile(fr.TargetUrl, fr.FileName) {
		return nil, errors.Errorf("The tuple (%s, %s) is not part of the result set",
			fr.TargetUrl, fr.FileName)
	}

	if strings.Contains(fr.TargetUrl, "://") && !strings.HasPrefix(fr.TargetUrl, "file://") {
		return nil, errors.New("This method is only implemented for targets of file://")
	}
	fr.TargetUrl = strings.TrimPrefix(fr.TargetUrl, "file://")
	fullPath := path.Join(fr.TargetUrl, fr.FileName)
	err = os.Remove(fullPath)
	if err != nil {
		return nil, errors.Wrapf(err, "Error removing file: %s", fullPath)
	}
	return fr, nil
}
