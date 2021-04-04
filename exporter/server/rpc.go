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
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/adobe/ferry/exporter/session"
	ferry "github.com/adobe/ferry/rpc"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Server struct {
	logger   *zap.Logger
	db       fdb.Database
	sessions sync.Map
	bindPort int
	certFile string
	keyFile  string
	ferry.UnimplementedFerryServer
}

func NewServer(db fdb.Database, bindPort int, certFile, keyFile string, logger *zap.Logger) *Server {
	return &Server{
		logger:   logger,
		db:       db,
		bindPort: bindPort,
		certFile: certFile,
		keyFile:  keyFile,
	}
}

func (exp *Server) ServeExport() (err error) {

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", exp.bindPort))
	if err != nil {
		exp.logger.Warn("Failed to listed on port", zap.Int("ca-file", exp.bindPort))
		return errors.Wrapf(err, "Failed to listed on port %s", exp.bindPort)
	}

	creds, err := credentials.NewServerTLSFromFile(exp.certFile, exp.keyFile)
	if err != nil {
		exp.logger.Warn("Failed to read TLS credentials", zap.String("ca-file", exp.certFile))
		return errors.Wrapf(err, "Failed to read TLS credentials from %s", exp.certFile)
	}

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	ferry.RegisterFerryServer(grpcServer, exp)
	exp.logger.Info("Listening", zap.String("port", fmt.Sprintf("%+v", exp.bindPort)))
	grpcServer.Serve(listener)
	return nil
}

func (exp *Server) StartSession(ctx context.Context, tgt *ferry.Target) (*ferry.SessionResponse, error) {

	es, err := session.NewSession(exp.db, tgt.TargetUrl, exp.logger)
	if err != nil {
		exp.logger.Warn("Failed to create a session ID", zap.Error(err))
		return nil, errors.Wrap(err, "Failed to create a session ID")
	}

	sessionID := es.GetSessionID()
	exp.sessions.Store(sessionID, es)
	exp.logger.Info("Created session", zap.String("sessionID", sessionID))

	return &ferry.SessionResponse{SessionId: sessionID, Status: ferry.SessionResponse_SUCCESS}, err
}

func (exp *Server) Export(srv ferry.Ferry_ExportServer) error {
	currentSessionID := ""
	var es *session.ExporterSession
	var ok bool

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
				es.Close()
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
					es.Close()
				}
				return errors.Errorf("Single stream cannot have multiple session ids %s", currentSessionID)
			}
			currentSessionID = req.SessionId

			var esi interface{}
			esi, ok = exp.sessions.LoadAndDelete(currentSessionID)
			if !ok {
				// Can't free `es`, not set yet
				return errors.Errorf("Invalid session id %s", currentSessionID)
			}
			es, ok = esi.(*session.ExporterSession)
			if !ok {
				// Can't free `es`, not set yet
				return errors.Errorf("Invalid session id %s", currentSessionID)
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
		exp.sessions.Store(currentSessionID, es)
	}

	return nil
}
func (exp *Server) EndSession(ctx context.Context, fs *ferry.Session) (*ferry.SessionResponse, error) {

	var es *session.ExporterSession
	var ok bool

	exp.logger.Info("Received EndSession", zap.String("sessionID", fs.SessionId))

	var esi interface{}
	esi, ok = exp.sessions.LoadAndDelete(fs.SessionId)
	if !ok {
		return nil, errors.Errorf("Invalid session id OR Session is in use - %s", fs.SessionId)
	}
	es, ok = esi.(*session.ExporterSession)
	if !ok {
		return nil, errors.Errorf("Corrupted tracker for session id %s", fs.SessionId)
	}

	exp.logger.Debug("Releasing resources", zap.String("sessionID", fs.SessionId))
	es.Close()
	exp.logger.Info("Released resources", zap.String("sessionID", fs.SessionId))
	return &ferry.SessionResponse{SessionId: fs.SessionId, Status: ferry.SessionResponse_SUCCESS}, nil
}

func (exp *Server) GiveTimeOfTheDay(ctx context.Context, clientTime *ferry.Time) (*ferry.Time, error) {

	return &ferry.Time{Ts: time.Now().UnixNano()}, nil
}
