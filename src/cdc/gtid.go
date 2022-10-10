// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cdc

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// GTID is the Mariadb Global Transaction ID
// https://mariadb.com/kb/en/library/gtid/
type GTID struct {
	Domain   uint32 `json:"domain"`
	ServerId uint32 `json:"server_id"`
	Sequence uint64 `json:"sequence"`
}

// String representation of the gtid. Return empty string if nil.
func (g *GTID) String() string {
	if g == nil {
		return ""
	}
	return fmt.Sprintf("%d-%d-%d", g.Domain, g.ServerId, g.Sequence)
}

// ParseGTID return GTID for the given string. For empty string it returns nil with error
func ParseGTID(gtid string) (*GTID, error) {
	if gtid == "" {
		return nil, nil
	}
	parts := strings.Split(gtid, "-")
	if len(parts) != 3 {
		return nil, errors.Errorf("parse gtid %s failed", gtid)
	}
	domain, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, errors.Errorf("parse domain failed")
	}
	serverId, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, errors.Errorf("parse serverId failed")
	}
	sequence, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return nil, errors.Errorf("parse sequence id failed")
	}
	return &GTID{
		Domain:   uint32(domain),
		ServerId: uint32(serverId),
		Sequence: sequence,
	}, nil
}
