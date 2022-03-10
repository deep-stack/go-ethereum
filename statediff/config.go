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

package statediff

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
)

// Config contains instantiation parameters for the state diffing service
type Config struct {
	IndexerConfig interfaces.Config
	// A unique ID used for this service
	ID string
	// Name for the client this service is running
	ClientName string
	// Whether to enable writing state diffs directly to track blockchain head
	EnableWriteLoop bool
	// Size of the worker pool
	NumWorkers uint
	// Should the statediff service wait until geth has synced to the head of the blockchain?
	WaitForSync bool
	// Context
	Context context.Context
}

// Params contains config parameters for the state diff builder
type Params struct {
	IntermediateStateNodes   bool
	IntermediateStorageNodes bool
	IncludeBlock             bool
	IncludeReceipts          bool
	IncludeTD                bool
	IncludeCode              bool
	WatchedAddresses         []common.Address
	WatchedStorageSlots      []common.Hash
}

// Args bundles the arguments for the state diff builder
type Args struct {
	OldStateRoot, NewStateRoot, BlockHash common.Hash
	BlockNumber                           *big.Int
}
