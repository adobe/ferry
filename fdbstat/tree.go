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

type DirNode struct {
	FlattenedPath   string // Also used as 'key' to index into DirListing below
	Path            []string
	Prefix          fdb.Key // raw key in original form
	PrefixPrintable string
	Children        []string // Immediate children only, slice of Flattened Path
}

type DirListing map[string]DirNode

func (s *Surveyor) dir(path []string) (desc []DirNode, err error) {
	var dirs []string
	var subDirs []DirNode

	var subSpace subspace.Subspace
	var node DirNode

	if path != nil { // root directory cannot be "opened"
		node.Path = path
		node.FlattenedPath = strings.Join(path, "/")
		subSpace, err = directory.Open(s.db, path, nil)
		if err != nil {
			return nil, errors.Wrapf(err, "directory.List failed for %s", path)
		}
	} else {
		subSpace = subspace.AllKeys()
	}
	node.Prefix = subSpace.FDBKey()
	node.PrefixPrintable = fdb.Printable(subSpace.Bytes())

	s.logger.Debug("directory.List()",
		zap.Strings("path", path),
		zap.Any("prefix-pritable", node.PrefixPrintable),
		zap.Any("prefix", node.Prefix))

	dirs, err = directory.List(s.db, path)
	if err != nil {
		return nil, errors.Wrapf(err, "directory.List failed for %s", path)
	}
	for _, dr := range dirs {
		node.Children = append(node.Children, fmt.Sprintf("%s/%s", strings.Join(path, "/"), dr))
		p := path[:] // clone
		p = append(p, dr)
		subDirs, err = s.dir(p)
		if err != nil {
			return nil, errors.Wrapf(err, "directory.List failed for %+v", p)
		}
		desc = append(desc, subDirs...)
	}
	desc = append(desc, node)
	return desc, nil
}

func (s *Surveyor) GetAllDirectories() (directories DirListing, err error) {

	dirs, err := s.dir(nil)
	if err != nil {
		return directories, errors.Wrapf(err, "List failed")
	}

	directories = make(DirListing, len(dirs))
	for _, dir := range dirs {
		directories[dir.FlattenedPath] = dir
	}
	return directories, nil
}
