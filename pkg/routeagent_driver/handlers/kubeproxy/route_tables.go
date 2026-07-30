/*
SPDX-License-Identifier: Apache-2.0

Copyright Contributors to the Submariner project.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubeproxy

import (
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/submariner-io/submariner/pkg/routeagent_driver/constants"
	"golang.org/x/sys/unix"
)

const (
	// RouteTablesConfigMap is an optional ConfigMap that lists additional Linux
	// routing tables that should receive the same inter-cluster VxLAN routes
	// programmed into the main table. Useful for CNIs that steer pod traffic into
	// custom PBR tables (for example Amazon VPC CNI on EKS — see issue #3697).
	//
	// Example:
	//
	//	apiVersion: v1
	//	kind: ConfigMap
	//	metadata:
	//	  name: submariner-route-tables
	//	data:
	//	  tables: "2,3,4"
	RouteTablesConfigMap = "submariner-route-tables"

	// RouteTablesKey is the ConfigMap data key holding a comma-separated list of
	// routing table IDs.
	RouteTablesKey = "tables"
)

// ParseRouteTables parses a comma-separated list of routing table IDs.
// Empty input yields a nil slice. Submariner-managed and reserved tables are skipped.
func ParseRouteTables(value string) ([]int, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}

	parts := strings.Split(value, ",")
	tables := make([]int, 0, len(parts))
	seen := map[int]struct{}{}

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		table, err := strconv.Atoi(part)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid routing table %q", part)
		}

		if table <= 0 {
			return nil, errors.Errorf("invalid routing table %d: must be positive", table)
		}

		if isReservedRouteTable(table) {
			logger.Warningf("Ignoring reserved/Submariner-managed routing table %d from %s", table, RouteTablesConfigMap)
			continue
		}

		if _, ok := seen[table]; ok {
			continue
		}

		seen[table] = struct{}{}
		tables = append(tables, table)
	}

	return tables, nil
}

func isReservedRouteTable(table int) bool {
	return table == unix.RT_TABLE_LOCAL ||
		table == unix.RT_TABLE_MAIN ||
		table == unix.RT_TABLE_DEFAULT ||
		table == constants.RouteAgentInterClusterNetworkTableID ||
		table == constants.RouteAgentHostNetworkTableID
}

func isMainRouteTable(table int) bool {
	return table == 0 || table == unix.RT_TABLE_MAIN
}
