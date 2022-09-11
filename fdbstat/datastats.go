package fdbstat

import (
	"fmt"

	"github.com/adobe/ferry/exporter/session"
	"github.com/adobe/ferry/finder"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (s *Surveyor) CalculateDBSize(pmap *finder.PartitionMap) (totalSize int64, err error) {

	txn, err := s.db.CreateTransaction()
	for _, v := range pmap.Ranges {
		if err != nil {
			return 0, errors.Wrapf(err, "Unable to create fdb transaction")
		}
		s.logger.Debug("Attempt", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
			zap.ByteString("end", v.Krange.End.FDBKey()),
			zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
		size, err := txn.GetEstimatedRangeSizeBytes(v.Krange).Get()
		if err != nil {
			return 0, errors.Wrapf(err, "Unable to create fdb transaction")
		}
		totalSize += size
		s.logger.Debug("Range", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
			zap.ByteString("end", v.Krange.End.FDBKey()),
			zap.Int64("size", size),
			zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
	}
	txn.Commit()
	return totalSize, nil
}

func (s *Surveyor) CalculateRowCount(pmap *finder.PartitionMap, readerThreads int) (totalRows int64, err error) {

	es, err := session.NewSession(s.db,
		"",
		readerThreads,
		false,
		s.logger,
		false)
	if err != nil {
		s.logger.Warn("Failed to create a session ID", zap.Error(err))
		return 0, errors.Wrap(err, "Failed to create a session ID")
	}

	for _, v := range pmap.Ranges {
		//s.logger.Info("Attempt", zap.ByteString("begin", v.Krange.Begin.FDBKey()),
		//	zap.ByteString("end", v.Krange.End.FDBKey()),
		//	zap.String("hosts", fmt.Sprintf("%+v", v.Hosts)))
		es.Send(v.Krange)
	}
	es.Finalize()
	return 0, nil
}
