package fdbstat

import (
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

func (s *Surveyor) dir(path []string) (children []string, err error) {
	var dirs, subDirs []string

	var subSpace subspace.Subspace

	if path != nil { // root directory cannot be "opened"
		subSpace, err = directory.Open(s.db, path, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "directory.List failed for %s", path)
		}
	} else {
		subSpace = subspace.AllKeys()
	}
	krB, krE := subSpace.FDBRangeKeys()
	s.logger.Debug("directory.List()",
		zap.Strings("path", path),
		zap.String("prefix", fdb.Printable(subSpace.Bytes())),
		// TODO: there must be a better way to write the statement below.
		// subSpace already contains the 'Range'. We get it as tuple via FDBRangeKeys
		// above ^^ and then convert to a KeyRange again. Seems convoluted.
		zap.Any("range", NewHashableKeyRange(fdb.KeyRange{Begin: krB, End: krE})))

	dirs, err = directory.List(s.db, path)
	if err != nil {
		return nil, errors.Wrapf(err, "directory.List failed for %s", path)
	}
	for _, dr := range dirs {
		children = append(children, fmt.Sprintf("%s/%s", strings.Join(path, "/"), dr))
		p := path[:] // clone
		p = append(p, dr)
		subDirs, err = s.dir(p)
		if err != nil {
			return nil, errors.Wrapf(err, "directory.List failed for %+v", p)
		}
		children = append(children, subDirs...)
	}
	return children, nil
}

func (s *Surveyor) GetAllDirectories() (directories []string, err error) {

	directories, err = s.dir(nil)
	if err != nil {
		return directories, errors.Wrapf(err, "List failed")
	}
	/*
		subspace := subspace.AllKeys()
		if err != nil {
			return directories, errors.Wrapf(err, "AllKeys failed")
		}
		b, e := subspace.FDBRangeKeys()
		fmt.Printf("AllKeys(): %s-%s", b, e)

		b2 := subspace.Bytes()
		fmt.Printf("Bytes(): %s", b2)
	*/
	return directories, nil
}
