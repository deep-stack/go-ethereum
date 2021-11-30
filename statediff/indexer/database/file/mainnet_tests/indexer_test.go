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

package mainnet_tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

var (
	testBlock     *types.Block
	testReceipts  types.Receipts
	testHeaderCID cid.Cid
	sqlxdb        *sqlx.DB
	err           error
	chainConf     = params.MainnetChainConfig
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}
}

func setup(t *testing.T) {
	testBlock, testReceipts, err = TestBlocksAndReceiptsFromEnv()
	require.NoError(t, err)
	headerRLP, err := rlp.EncodeToBytes(testBlock.Header())
	require.NoError(t, err)

	testHeaderCID, _ = ipld.RawdataToCid(ipld.MEthHeader, headerRLP, multihash.KECCAK_256)
	if _, err := os.Stat(file.TestConfig.FilePath); !errors.Is(err, os.ErrNotExist) {
		err := os.Remove(file.TestConfig.FilePath)
		require.NoError(t, err)
	}
	ind, err := file.NewStateDiffIndexer(context.Background(), chainConf, file.TestConfig)
	require.NoError(t, err)
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		testBlock,
		testReceipts,
		testBlock.Difficulty())
	require.NoError(t, err)

	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
		if err := ind.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, testBlock.Hash().String())
		require.NoError(t, err)
	}

	test_helpers.ExpectEqual(t, tx.(*file.BatchTx).BlockNumber, testBlock.Number().Uint64())

	connStr := postgres.DefaultConfig.DbConnectionString()

	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}

func dumpData(t *testing.T) {
	sqlFileBytes, err := os.ReadFile(file.TestConfig.FilePath)
	require.NoError(t, err)

	_, err = sqlxdb.Exec(string(sqlFileBytes))
	require.NoError(t, err)
}

func tearDown(t *testing.T) {
	file.TearDownDB(t, sqlxdb)
	err := os.Remove(file.TestConfig.FilePath)
	require.NoError(t, err)
	err = sqlxdb.Close()
	require.NoError(t, err)
}

func TestPushBlockAndState(t *testing.T) {
	t.Run("Test PushBlock and PushStateNode", func(t *testing.T) {
		setup(t)
		dumpData(t)
		tearDown(t)
	})
}
