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

package interfaces

import (
	"context"
	"io"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

// StateDiffIndexer interface required to index statediff data
type StateDiffIndexer interface {
	PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (Batch, int64, error)
	PushStateNode(tx Batch, stateNode sdtypes.StateNode, headerHash string, headerID int64) error
	PushCodeAndCodeHash(tx Batch, codeAndCodeHash sdtypes.CodeAndCodeHash) error
	ReportOldDBMetrics(delay time.Duration, quit <-chan bool)
	ReportNewDBMetrics(delay time.Duration, quit <-chan bool)
	io.Closer
}

// Batch required for indexing data atomically
type Batch interface {
	Submit(err error) error
}

// Config used to configure different underlying implementations
type Config interface {
	Type() shared.DBType
}

// Database interfaces required by the sql indexer
type Database interface {
	Driver
	Statements
	Version() uint
}

// Driver interface has all the methods required by a driver implementation to support the sql indexer
type Driver interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) ScannableRow
	Exec(ctx context.Context, sql string, args ...interface{}) (Result, error)
	Select(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Get(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	Begin(ctx context.Context) (Tx, error)
	Stats() Stats
	Context() context.Context
	io.Closer
}

// Statements interface to accommodate different SQL query syntax
type Statements interface {
	InsertNodeInfoStm() string
	InsertHeaderStm() string
	InsertUncleStm() string
	InsertTxStm() string
	InsertAccessListElementStm() string
	InsertRctStm() string
	InsertLogStm() string
	InsertStateStm() string
	InsertAccountStm() string
	InsertStorageStm() string
	InsertIPLDStm() string
	InsertIPLDsStm() string
}

// Tx interface to accommodate different concrete SQL transaction types
type Tx interface {
	QueryRow(ctx context.Context, sql string, args ...interface{}) ScannableRow
	Exec(ctx context.Context, sql string, args ...interface{}) (Result, error)
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// ScannableRow interface to accommodate different concrete row types
type ScannableRow interface {
	Scan(dest ...interface{}) error
}

// Result interface to accommodate different concrete result types
type Result interface {
	RowsAffected() (int64, error)
}

// Stats interface to accommodate different concrete sql stats types
type Stats interface {
	MaxOpen() int64
	Open() int64
	InUse() int64
	Idle() int64
	WaitCount() int64
	WaitDuration() time.Duration
	MaxIdleClosed() int64
	MaxLifetimeClosed() int64
}
