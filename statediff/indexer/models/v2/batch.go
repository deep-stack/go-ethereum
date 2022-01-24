// VulcanizeDB
// Copyright © 2021 Vulcanize

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package models

import "github.com/lib/pq"

// IPLDBatch holds the arguments for a batch insert of IPLD data
type IPLDBatch struct {
	Keys   []string
	Values [][]byte
}

// UncleBatch is the db model for eth.uncle_cids
type UncleBatch struct {
	IDs          []int64
	HeaderIDs    []int64
	BlockHashes  []string
	ParentHashes []string
	CIDs         []string
	MhKeys       []string
	Rewards      []string
}

// TxBatch is the db model for eth.transaction_cids
type TxBatch struct {
	IDs       []int64
	HeaderIDs []int64
	Indexes   []int64
	TxHashes  []string
	CIDs      []string
	MhKeys    []string
	Dsts      []string
	Srcs      []string
	Data      [][]byte
	Types     []uint8
}

// AccessListElementBatch is the db model for eth.access_list_entry
type AccessListElementBatch struct {
	IDs         []int64
	Indexes     []int64
	TxIDs       []int64
	Addresses   []string
	StorageKeys []pq.StringArray
}

// ReceiptBatch is the db model for eth.receipt_cids
type ReceiptBatch struct {
	IDs            []int64
	TxIDs          []int64
	LeafCIDs       []string
	LeafMhKeys     []string
	PostStatuses   []int64
	PostStates     []string
	Contracts      []string
	ContractHashes []string
	LogRoots       []string
}

// StateNodeBatch is the db model for eth.state_cids
type StateNodeBatch struct {
	IDs       []int64
	HeaderIDs []int64
	Paths     [][]byte
	StateKeys []string
	NodeTypes []int
	CIDs      []string
	MhKeys    []string
	Diffs     []bool
}

// StorageNodeBatch is the db model for eth.storage_cids
type StorageNodeBatch struct {
	IDs         []int64
	StateIDs    []int64
	Paths       [][]byte
	StorageKeys []string
	NodeTypes   []int
	CIDs        []string
	MhKeys      []string
	Diffs       []bool
}

// StorageNodeWithStateKeyBatch is a db model for eth.storage_cids + eth.state_cids.state_key
type StorageNodeWithStateKeyBatch struct {
	IDs         []int64
	StateIDs    []int64
	Paths       [][]byte
	StateKeys   []string
	StorageKeys []string
	NodeTypes   []int
	CIDs        []string
	MhKeys      []string
	Diffs       []bool
}

// StateAccountBatch is a db model for an eth state account (decoded value of state leaf node)
type StateAccountBatch struct {
	IDs          []int64
	StateIDs     []int64
	Balances     []string
	Nonces       []int64
	CodeHashes   [][]byte
	StorageRoots []string
}

// LogsBatch is the db model for eth.logs
type LogsBatch struct {
	IDs        []int64
	LeafCIDs   []string
	LeafMhKeys []string
	ReceiptIDs []int64
	Addresses  []string
	Indexes    []int64
	Data       [][]byte
	Topic0s    []string
	Topic1s    []string
	Topic2s    []string
	Topic3s    []string
}
