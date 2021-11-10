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

// UncleBatch holds the arguments for a batch insert of uncle data
type UncleBatch struct {
	HeaderID     []int64
	BlockHashes  []string
	ParentHashes []string
	CIDs         []string
	MhKeys       []string
	Rewards      []string
}

// TxBatch holds the arguments for a batch insert of tx data
type TxBatch struct {
	HeaderID int64
	Indexes  []int64
	TxHashes []string
	CIDs     []string
	MhKeys   []string
	Dsts     []string
	Srcs     []string
	Datas    [][]byte
	Types    []*uint8
}

// AccessListBatch holds the arguments for a batch insert of access list data
type AccessListBatch struct {
	Indexes         []int64
	TxIDs           []int64
	Addresses       []string
	StorageKeysSets []pq.StringArray
}

// ReceiptBatch holds the arguments for a batch insert of receipt data
type ReceiptBatch struct {
	TxIDs          []int64
	LeafCIDs       []string
	LeafMhKeys     []string
	PostStatuses   []uint64
	PostStates     []string
	Contracts      []string
	ContractHashes []string
	LogRoots       []string
}

// LogBatch holds the arguments for a batch insert of log data
type LogBatch struct {
	LeafCIDs   []string
	LeafMhKeys []string
	ReceiptIDs []int64
	Addresses  []string
	Indexes    []int64
	Datas      [][]byte
	Topic0s    []string
	Topic1s    []string
	Topic2s    []string
	Topic3s    []string
}

// StateBatch holds the arguments for a batch insert of state data
type StateBatch struct {
	ID       int64
	HeaderID int64
	Path     []byte
	StateKey string
	NodeType int
	CID      string
	MhKey    string
	Diff     bool
}

// AccountBatch holds the arguments for a batch insert of account data
type AccountBatch struct {
	ID          int64
	StateID     int64
	Balance     string
	Nonce       uint64
	CodeHash    []byte
	StorageRoot string
}

// StorageBatch holds the arguments for a batch insert of storage data
type StorageBatch struct {
	ID         int64
	StateID    int64
	Path       []byte
	StorageKey string
	NodeType   int
	CID        string
	MhKey      string
	Diff       bool
}
