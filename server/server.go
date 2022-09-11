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
	"fmt"
	"net"
	"sync"

	ferry "github.com/adobe/ferry/rpc"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	logger         *zap.Logger
	db             fdb.Database
	importSessions sync.Map
	exportSessions sync.Map
	bindPort       int
	certFile       string
	keyFile        string

	// comment-out line below (temporarily) to
	// see what methods the interface doesn't
	// satisfy yet. UnimplementedFerryServer
	// is a catch-all
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

func (exp *Server) ServeImportExport() (err error) {

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", exp.bindPort))
	if err != nil {
		exp.logger.Warn("Failed to listed on port", zap.Int("port", exp.bindPort))
		return errors.Wrapf(err, "Failed to listed on port %d", exp.bindPort)
	}

	creds, err := credentials.NewServerTLSFromFile(exp.certFile, exp.keyFile)
	if err != nil {
		exp.logger.Warn("Failed to read TLS credentials", zap.String("cert-file", exp.certFile))
		return errors.Wrapf(err, "Failed to read TLS credentials from %s", exp.certFile)
	}

	grpcServer := grpc.NewServer(grpc.Creds(creds))
	ferry.RegisterFerryServer(grpcServer, exp)
	reflection.Register(grpcServer)
	exp.logger.Info("Listening", zap.String("port", fmt.Sprintf("%+v", exp.bindPort)))
	err = grpcServer.Serve(listener)
	return err
}
