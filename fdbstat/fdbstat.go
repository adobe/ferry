package fdbstat

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type SurveyorOption func(exp *Surveyor)

type Surveyor struct {
	// Must have properties
	db fdb.Database

	// Optional, but defaults if not set
	logger *zap.Logger
}

func NewSurveyor(db fdb.Database, opts ...SurveyorOption) (s *Surveyor, err error) {

	s = &Surveyor{db: db}
	for _, opt := range opts {
		opt(s)
	}
	// if logger is not set, we must set one
	if s.logger == nil {
		s.logger, err = zap.NewProduction()
		if err != nil {
			return nil, errors.Wrapf(err, "Logger not supplied. Can't initialize one either")
		}
	}
	return s, nil

}
func Logger(logger *zap.Logger) SurveyorOption {
	return func(s *Surveyor) {
		s.logger = logger
	}

}
