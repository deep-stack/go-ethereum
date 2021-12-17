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
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/statediff/indexer"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
)

var (
	err error
	db    *postgres.DB
	chainConf = params.MainnetChainConfig
)

func init() {
	if os.Getenv("MODE") != "statediff" {
		fmt.Println("Skipping statediff test")
		os.Exit(0)
	}
}

func TestPushBlockAndState(t *testing.T) {
	conf := DefaultTestConfig
	rawURL := os.Getenv(TEST_RAW_URL)
	if rawURL == "" {
		fmt.Printf("Warning: no raw url configured for statediffing mainnet tests, will look for local file and"+
			"then try default endpoint (%s)\r\n", DefaultTestConfig.RawURL)
	} else {
		conf.RawURL = rawURL
	}
	for _, blockNumber := range problemBlocks {
		conf.BlockNumber = big.NewInt(blockNumber)
		tb, trs, err := TestBlockAndReceipts(conf)
		require.NoError(t, err)
		testPushBlockAndState(t, tb, trs)
	}
	testBlock, testReceipts, err := TestBlockAndReceiptsFromEnv(conf)
	require.NoError(t, err)
	testPushBlockAndState(t, testBlock, testReceipts)
}

func testPushBlockAndState(t *testing.T, block *types.Block, receipts types.Receipts) {
	t.Run("Test PushBlock and PushStateNode", func(t *testing.T) {
		setup(t, block, receipts)
		tearDown(t)
	})
}

func setup(t *testing.T, testBlock *types.Block, testReceipts types.Receipts) {
	db, err = shared.SetupDB()
	if err != nil {
		t.Fatal(err)
	}
	ind, err := indexer.NewStateDiffIndexer(chainConf, db)
	require.NoError(t, err)
	var tx *indexer.BlockTx
	tx, err = ind.PushBlock(
		testBlock,
		testReceipts,
		testBlock.Difficulty())
	require.NoError(t, err)

	defer func(){
		if err := tx.Close(err); err != nil {
			t.Fatal(err)
		}
	}()

	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node)
		require.NoError(t, err)
	}

	shared.ExpectEqual(t, tx.BlockNumber, testBlock.Number().Uint64())
}

func tearDown(t *testing.T) {
	indexer.TearDownDB(t, db)
	err = db.Close()
	require.NoError(t, err)
}
