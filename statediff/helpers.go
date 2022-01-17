// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Contains a batch of utility type declarations used by the tests. As the node
// operates on unique types, a lot of them are needed to check various features.

package statediff

import (
	"fmt"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
)

func sortKeys(data AccountMap) []string {
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

// findIntersection finds the set of strings from both arrays that are equivalent
// a and b must first be sorted
// this is used to find which keys have been both "deleted" and "created" i.e. they were updated
func findIntersection(a, b []string) []string {
	lenA := len(a)
	lenB := len(b)
	iOfA, iOfB := 0, 0
	updates := make([]string, 0)
	if iOfA >= lenA || iOfB >= lenB {
		return updates
	}
	for {
		switch strings.Compare(a[iOfA], b[iOfB]) {
		// -1 when a[iOfA] < b[iOfB]
		case -1:
			iOfA++
			if iOfA >= lenA {
				return updates
			}
			// 0 when a[iOfA] == b[iOfB]
		case 0:
			updates = append(updates, a[iOfA])
			iOfA++
			iOfB++
			if iOfA >= lenA || iOfB >= lenB {
				return updates
			}
			// 1 when a[iOfA] > b[iOfB]
		case 1:
			iOfB++
			if iOfB >= lenB {
				return updates
			}
		}
	}
}

// loadWatchedAddresses is used to load watched addresses to the in-memory write loop params from the db
func loadWatchedAddresses(db *postgres.DB) error {
	rows, err := db.Query("SELECT address FROM eth.watched_addresses")
	if err != nil {
		return fmt.Errorf("error loading watched addresses: %v", err)
	}

	var watchedAddresses []common.Address
	for rows.Next() {
		var addressHex string
		err := rows.Scan(&addressHex)
		if err != nil {
			return err
		}

		watchedAddresses = append(watchedAddresses, common.HexToAddress(addressHex))
	}

	writeLoopParams.mu.Lock()
	writeLoopParams.WatchedAddresses = watchedAddresses
	writeLoopParams.mu.Unlock()

	return nil
}

// removeAddresses is used to remove given addresses from a list of addresses
func removeAddresses(addresses []common.Address, addressesToRemove []common.Address) []common.Address {
	addressesCopy := make([]common.Address, len(addresses))
	copy(addressesCopy, addresses)

	for _, address := range addressesToRemove {
		if idx := containsAddress(addressesCopy, address); idx != -1 {
			addressesCopy = append(addressesCopy[:idx], addressesCopy[idx+1:]...)
		}
	}

	return addressesCopy
}

// containsAddress is used to check if an address is present in the provided list of addresses
// return the index if found else -1
func containsAddress(addresses []common.Address, address common.Address) int {
	for idx, addr := range addresses {
		if addr == address {
			return idx
		}
	}
	return -1
}
