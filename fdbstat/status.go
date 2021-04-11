package fdbstat

import (
	"encoding/json"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/pkg/errors"
)

func GetNodesFromStatus(status string) (hosts []string, err error) {
	v := map[string]interface{}{}
	err = json.Unmarshal([]byte(status), &v)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to parse /status/json output")
	}
	v1, ok := v["cluster"]
	if !ok {
		return nil, errors.New("[1] Unexpected format. No 'cluster' key")
	}
	v1a, ok := v1.(map[string]interface{})
	if !ok {
		return nil, errors.New("[2] Unexpected format. 'cluster' value is not a map")
	}
	v2, ok := v1a["machines"]
	if !ok {
		return nil, errors.New("[3] Unexpected format. No 'machines' sub-key")

	}
	v2a, ok := v2.(map[string]interface{})
	if !ok {
		return nil, errors.New("[4] Unexpected format. 'machines' values is not a map")
	}
	for _, v3 := range v2a {
		v3a, ok := v3.(map[string]interface{})
		if !ok {
			return nil, errors.New("[5] Unexpected format. Machine specific node is not a map")
		}
		v4, ok := v3a["address"]
		if !ok {
			return nil, errors.New("[6] Unexpected format. No 'address' sub-key")
		}
		v4a, ok := v4.(string)
		if !ok {
			return nil, errors.New("[7] Unexpected format. 'address' sub-key is not a string")
		}
		hosts = append(hosts, v4a)
	}
	return hosts, nil
}

func GetStatus(db fdb.Database) (status string, err error) {

	ret, err := db.Transact(func(tr fdb.Transaction) (v interface{}, err error) {
		fstr := tr.Get(fdb.Key("\xFF\xFF/status/json"))
		v, err = fstr.Get()
		return v, err
	})
	if err != nil {
		return "", errors.Wrapf(err, "Error fetching fdb status")
	}
	statusb, ok := ret.([]byte)
	if !ok {
		return "", errors.New("Error fetching fdb status")
	}
	status = string(statusb)
	return status, err
}

func GetAllNodes(db fdb.Database) (hosts []string, err error) {

	status, err := GetStatus(db)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to fetch status")
	}
	fmt.Println(status)
	hosts, err = GetNodesFromStatus(status)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to fetch status")
	}
	return hosts, err
}
