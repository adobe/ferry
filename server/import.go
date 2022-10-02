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

	"github.com/adobe/ferry/importer/session"
	ferry "github.com/adobe/ferry/rpc"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (exp *Server) StartImportSession(ctx context.Context, tgt *ferry.Target) (*ferry.SessionResponse, error) {

	es, err := session.NewSession(exp.db,
		tgt.TargetUrl,
		int(tgt.ReaderThreads),
		exp.logger,
		false)
	if err != nil {
		exp.logger.Warn("Failed to create a session ID", zap.Error(err))
		return nil, errors.Wrap(err, "Failed to create a session ID")
	}

	sessionID := es.GetSessionID()
	exp.importSessions.Store(sessionID, es)
	exp.logger.Info("Created session", zap.String("sessionID", sessionID))

	return &ferry.SessionResponse{SessionId: sessionID, Status: ferry.SessionResponse_SUCCESS}, err
}

func (exp *Server) Import(srv ferry.Ferry_ImportServer) (err error) {
	currentSessionID := ""
	var es *session.ImporterSession

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

			es, err = exp.popImportSession(currentSessionID)
			if err != nil {
				return err
			}
			exp.logger.Info("Start import stream", zap.String("sessionID", currentSessionID))
		}
		// At this point es will point to ImporterSession we can use
		exp.logger.Debug("Sending to worker",
			zap.String("file", req.FileName),
		)
		es.Send(req.FileName)
	}

	exp.logger.Info("Wrapping up Import", zap.String("sessionID", currentSessionID))

	if es != nil {
		// Store it back. This is critical, we had *deleted* it earlier
		// Storing it back is how we indicate it is now free to be acquired
		// for cleanup
		exp.importSessions.Store(currentSessionID, es)
	}

	return err
}

func (exp *Server) popImportSession(sessionID string) (es *session.ImporterSession, err error) {

	var esi interface{}
	var ok bool

	esi, ok = exp.importSessions.LoadAndDelete(sessionID)
	if !ok {
		return nil, errors.Errorf("Invalid session id OR Session is in use - %s", sessionID)
	}
	es, ok = esi.(*session.ImporterSession)
	if !ok {
		return nil, errors.Errorf("Corrupted tracker for session id %s", sessionID)
	}
	return es, nil
}
func (exp *Server) StopImportSession(ctx context.Context, fs *ferry.Session) (*ferry.SessionResponse, error) {

	var es *session.ImporterSession

	exp.logger.Info("Received StopSession", zap.String("sessionID", fs.SessionId))

	es, err := exp.popImportSession(fs.SessionId)
	if err != nil {
		return nil, err
	}
	// Very Import: Release session after use
	// "release" is done by putting it back in map
	// else the session will be GC-ed.
	defer exp.importSessions.Store(fs.SessionId, es)

	exp.logger.Debug("Releasing resources", zap.String("sessionID", fs.SessionId))
	es.Finalize()
	exp.logger.Info("Released resources", zap.String("sessionID", fs.SessionId))

	return &ferry.SessionResponse{
		SessionId: fs.SessionId,
		Status:    ferry.SessionResponse_SUCCESS,
	}, nil
}

func (exp *Server) EndImportSession(ctx context.Context, fs *ferry.Session) (*ferry.SessionResponse, error) {

	var es *session.ImporterSession

	exp.logger.Info("Received EndSession", zap.String("sessionID", fs.SessionId))

	es, err := exp.popImportSession(fs.SessionId)
	if err != nil {
		return nil, err
	}
	// Very Import: this (EndSession) is the only situation
	// a 'pop' is not followed by a
	// 	"defer exp.sessions.Store(fr.SessionId, es)"

	exp.logger.Debug("Releasing resources", zap.String("sessionID", fs.SessionId))
	es.Finalize()
	exp.logger.Info("Released resources", zap.String("sessionID", fs.SessionId))

	return &ferry.SessionResponse{
		SessionId: fs.SessionId,
		Status:    ferry.SessionResponse_SUCCESS,
	}, nil
}
