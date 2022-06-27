// VulcanizeDB
// Copyright © 2022 Vulcanize

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

package file_test

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/file"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

func setupCSVIndexer(t *testing.T) {
	file.TestConfig.Mode = file.CSV
	file.TestConfig.OutputDir = "./statediffing_test"

	if _, err := os.Stat(file.TestConfig.OutputDir); !errors.Is(err, os.ErrNotExist) {
		err := os.RemoveAll(file.TestConfig.OutputDir)
		require.NoError(t, err)
	}

	ind, err = file.NewStateDiffIndexer(context.Background(), mocks.TestConfig, file.TestConfig)
	require.NoError(t, err)

	connStr := postgres.DefaultConfig.DbConnectionString()
	sqlxdb, err = sqlx.Connect("postgres", connStr)
	if err != nil {
		t.Fatalf("failed to connect to db with connection string: %s err: %v", connStr, err)
	}
}

func setupCSV(t *testing.T) {
	setupCSVIndexer(t)
	var tx interfaces.Batch
	tx, err = ind.PushBlock(
		mockBlock,
		mocks.MockReceipts,
		mocks.MockBlock.Difficulty())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Submit(err); err != nil {
			t.Fatal(err)
		}
		if err := ind.Close(); err != nil {
			t.Fatal(err)
		}
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, mockBlock.Hash().String())
		require.NoError(t, err)
	}

	require.Equal(t, mocks.BlockNumber.String(), tx.(*file.BatchTx).BlockNumber)
}

func XTestCSVFileIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)
		pgStr := `SELECT cid, td, reward, block_hash, coinbase
				FROM eth.header_cids
				WHERE block_number = $1`
		// check header was properly indexed
		type res struct {
			CID       string
			TD        string
			Reward    string
			BlockHash string `db:"block_hash"`
			Coinbase  string `db:"coinbase"`
		}
		header := new(res)
		err = sqlxdb.QueryRowx(pgStr, mocks.BlockNumber.Uint64()).StructScan(header)
		if err != nil {
			t.Fatal(err)
		}

		require.Equal(t, headerCID.String(), header.CID)
		require.Equal(t, mocks.MockBlock.Difficulty().String(), header.TD)
		require.Equal(t, "2000000000000021250", header.Reward)
		require.Equal(t, mocks.MockHeader.Coinbase.String(), header.Coinbase)
		dc, err := cid.Decode(header.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		var data []byte
		err = sqlxdb.Get(&data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, mocks.MockHeaderRlp, data)
	})

	t.Run("Publish and index transaction IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		// check that txs were properly indexed and published
		trxs := make([]string, 0)
		pgStr := `SELECT transaction_cids.cid FROM eth.transaction_cids INNER JOIN eth.header_cids ON (transaction_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1`
		err = sqlxdb.Select(&trxs, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 5, len(trxs))
		expectTrue(t, test_helpers.ListContainsString(trxs, trx1CID.String()))
		expectTrue(t, test_helpers.ListContainsString(trxs, trx2CID.String()))
		expectTrue(t, test_helpers.ListContainsString(trxs, trx3CID.String()))
		expectTrue(t, test_helpers.ListContainsString(trxs, trx4CID.String()))
		expectTrue(t, test_helpers.ListContainsString(trxs, trx5CID.String()))

		transactions := mocks.MockBlock.Transactions()
		type txResult struct {
			TxType uint8 `db:"tx_type"`
			Value  string
		}
		for _, c := range trxs {
			dc, err := cid.Decode(c)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			var data []byte
			err = sqlxdb.Get(&data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
			if err != nil {
				t.Fatal(err)
			}
			txTypeAndValueStr := `SELECT tx_type, value FROM eth.transaction_cids WHERE cid = $1`
			switch c {
			case trx1CID.String():
				require.Equal(t, tx1, data)
				txRes := new(txResult)
				err = sqlxdb.QueryRowx(txTypeAndValueStr, c).StructScan(txRes)
				if err != nil {
					t.Fatal(err)
				}
				if txRes.TxType != 0 {
					t.Fatalf("expected LegacyTxType (0), got %d", txRes.TxType)
				}
				if txRes.Value != transactions[0].Value().String() {
					t.Fatalf("expected tx value %s got %s", transactions[0].Value().String(), txRes.Value)
				}
			case trx2CID.String():
				require.Equal(t, tx2, data)
				txRes := new(txResult)
				err = sqlxdb.QueryRowx(txTypeAndValueStr, c).StructScan(txRes)
				if err != nil {
					t.Fatal(err)
				}
				if txRes.TxType != 0 {
					t.Fatalf("expected LegacyTxType (0), got %d", txRes.TxType)
				}
				if txRes.Value != transactions[1].Value().String() {
					t.Fatalf("expected tx value %s got %s", transactions[1].Value().String(), txRes.Value)
				}
			case trx3CID.String():
				require.Equal(t, tx3, data)
				txRes := new(txResult)
				err = sqlxdb.QueryRowx(txTypeAndValueStr, c).StructScan(txRes)
				if err != nil {
					t.Fatal(err)
				}
				if txRes.TxType != 0 {
					t.Fatalf("expected LegacyTxType (0), got %d", txRes.TxType)
				}
				if txRes.Value != transactions[2].Value().String() {
					t.Fatalf("expected tx value %s got %s", transactions[2].Value().String(), txRes.Value)
				}
			case trx4CID.String():
				require.Equal(t, tx4, data)
				txRes := new(txResult)
				err = sqlxdb.QueryRowx(txTypeAndValueStr, c).StructScan(txRes)
				if err != nil {
					t.Fatal(err)
				}
				if txRes.TxType != types.AccessListTxType {
					t.Fatalf("expected AccessListTxType (1), got %d", txRes.TxType)
				}
				if txRes.Value != transactions[3].Value().String() {
					t.Fatalf("expected tx value %s got %s", transactions[3].Value().String(), txRes.Value)
				}
				accessListElementModels := make([]models.AccessListElementModel, 0)
				pgStr = "SELECT cast(access_list_elements.block_number AS TEXT), access_list_elements.index, access_list_elements.tx_id, " +
					"access_list_elements.address, access_list_elements.storage_keys FROM eth.access_list_elements " +
					"INNER JOIN eth.transaction_cids ON (tx_id = transaction_cids.tx_hash) WHERE cid = $1 ORDER BY access_list_elements.index ASC"
				err = sqlxdb.Select(&accessListElementModels, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if len(accessListElementModels) != 2 {
					t.Fatalf("expected two access list entries, got %d", len(accessListElementModels))
				}
				model1 := models.AccessListElementModel{
					BlockNumber: mocks.BlockNumber.String(),
					Index:       accessListElementModels[0].Index,
					Address:     accessListElementModels[0].Address,
				}
				model2 := models.AccessListElementModel{
					BlockNumber: mocks.BlockNumber.String(),
					Index:       accessListElementModels[1].Index,
					Address:     accessListElementModels[1].Address,
					StorageKeys: accessListElementModels[1].StorageKeys,
				}
				require.Equal(t, mocks.AccessListEntry1Model, model1)
				require.Equal(t, mocks.AccessListEntry2Model, model2)
			case trx5CID.String():
				require.Equal(t, tx5, data)
				txRes := new(txResult)
				err = sqlxdb.QueryRowx(txTypeAndValueStr, c).StructScan(txRes)
				if err != nil {
					t.Fatal(err)
				}
				if txRes.TxType != types.DynamicFeeTxType {
					t.Fatalf("expected DynamicFeeTxType (2), got %d", txRes.TxType)
				}
				if txRes.Value != transactions[4].Value().String() {
					t.Fatalf("expected tx value %s got %s", transactions[4].Value().String(), txRes.Value)
				}
			}
		}
	})

	t.Run("Publish and index log IPLDs for multiple receipt of a specific block", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		rcts := make([]string, 0)
		pgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.tx_hash
				AND transaction_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				ORDER BY transaction_cids.index`
		err = sqlxdb.Select(&rcts, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}

		type logIPLD struct {
			Index   int    `db:"index"`
			Address string `db:"address"`
			Data    []byte `db:"data"`
			Topic0  string `db:"topic0"`
			Topic1  string `db:"topic1"`
		}
		for i := range rcts {
			results := make([]logIPLD, 0)
			pgStr = `SELECT log_cids.index, log_cids.address, log_cids.topic0, log_cids.topic1, data FROM eth.log_cids
    				INNER JOIN eth.receipt_cids ON (log_cids.rct_id = receipt_cids.tx_id)
					INNER JOIN public.blocks ON (log_cids.leaf_mh_key = blocks.key)
					WHERE receipt_cids.leaf_cid = $1 ORDER BY eth.log_cids.index ASC`
			err = sqlxdb.Select(&results, pgStr, rcts[i])
			require.NoError(t, err)

			// expecting MockLog1 and MockLog2 for mockReceipt4
			expectedLogs := mocks.MockReceipts[i].Logs
			require.Equal(t, len(expectedLogs), len(results))

			var nodeElements []interface{}
			for idx, r := range results {
				// Attempt to decode the log leaf node.
				err = rlp.DecodeBytes(r.Data, &nodeElements)
				require.NoError(t, err)
				if len(nodeElements) == 2 {
					logRaw, err := rlp.EncodeToBytes(expectedLogs[idx])
					require.NoError(t, err)
					// 2nd element of the leaf node contains the encoded log data.
					require.Equal(t, nodeElements[1].([]byte), logRaw)
				} else {
					logRaw, err := rlp.EncodeToBytes(expectedLogs[idx])
					require.NoError(t, err)
					// raw log was IPLDized
					require.Equal(t, r.Data, logRaw)
				}
			}
		}
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		// check receipts were properly indexed and published
		rcts := make([]string, 0)
		pgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.tx_hash
				AND transaction_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1 order by transaction_cids.index`
		err = sqlxdb.Select(&rcts, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 5, len(rcts))
		expectTrue(t, test_helpers.ListContainsString(rcts, rct1CID.String()))
		expectTrue(t, test_helpers.ListContainsString(rcts, rct2CID.String()))
		expectTrue(t, test_helpers.ListContainsString(rcts, rct3CID.String()))
		expectTrue(t, test_helpers.ListContainsString(rcts, rct4CID.String()))
		expectTrue(t, test_helpers.ListContainsString(rcts, rct5CID.String()))

		for idx, c := range rcts {
			result := make([]models.IPLDModel, 0)
			pgStr = `SELECT data
					FROM eth.receipt_cids
					INNER JOIN public.blocks ON (receipt_cids.leaf_mh_key = public.blocks.key)
					WHERE receipt_cids.leaf_cid = $1`
			err = sqlxdb.Select(&result, pgStr, c)
			if err != nil {
				t.Fatal(err)
			}

			// Decode the log leaf node.
			var nodeElements []interface{}
			err = rlp.DecodeBytes(result[0].Data, &nodeElements)
			require.NoError(t, err)

			expectedRct, err := mocks.MockReceipts[idx].MarshalBinary()
			require.NoError(t, err)

			require.Equal(t, expectedRct, nodeElements[1].([]byte))

			dc, err := cid.Decode(c)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			var data []byte
			err = sqlxdb.Get(&data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
			if err != nil {
				t.Fatal(err)
			}
			postStatePgStr := `SELECT post_state FROM eth.receipt_cids WHERE leaf_cid = $1`
			switch c {
			case rct1CID.String():
				require.Equal(t, rctLeaf1, data)
				var postStatus uint64
				pgStr = `SELECT post_status FROM eth.receipt_cids WHERE leaf_cid = $1`
				err = sqlxdb.Get(&postStatus, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, mocks.ExpectedPostStatus, postStatus)
			case rct2CID.String():
				require.Equal(t, rctLeaf2, data)
				var postState string
				err = sqlxdb.Get(&postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, mocks.ExpectedPostState1, postState)
			case rct3CID.String():
				require.Equal(t, rctLeaf3, data)
				var postState string
				err = sqlxdb.Get(&postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, mocks.ExpectedPostState2, postState)
			case rct4CID.String():
				require.Equal(t, rctLeaf4, data)
				var postState string
				err = sqlxdb.Get(&postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, mocks.ExpectedPostState3, postState)
			case rct5CID.String():
				require.Equal(t, rctLeaf5, data)
				var postState string
				err = sqlxdb.Get(&postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				require.Equal(t, mocks.ExpectedPostState3, postState)
			}
		}
	})

	t.Run("Publish and index state IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		// check that state nodes were properly indexed and published
		stateNodes := make([]models.StateNodeModel, 0)
		pgStr := `SELECT state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1 AND node_type != 3`
		err = sqlxdb.Select(&stateNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 2, len(stateNodes))
		for _, stateNode := range stateNodes {
			var data []byte
			dc, err := cid.Decode(stateNode.CID)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			err = sqlxdb.Get(&data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
			if err != nil {
				t.Fatal(err)
			}
			pgStr = `SELECT cast(block_number AS TEXT), header_id, state_path, cast(balance AS TEXT), nonce, code_hash, storage_root from eth.state_accounts WHERE header_id = $1 AND state_path = $2`
			var account models.StateAccountModel
			err = sqlxdb.Get(&account, pgStr, stateNode.HeaderID, stateNode.Path)
			if err != nil {
				t.Fatal(err)
			}
			if stateNode.CID == state1CID.String() {
				require.Equal(t, 2, stateNode.NodeType)
				require.Equal(t, common.BytesToHash(mocks.ContractLeafKey).Hex(), stateNode.StateKey)
				require.Equal(t, []byte{'\x06'}, stateNode.Path)
				require.Equal(t, mocks.ContractLeafNode, data)
				require.Equal(t, models.StateAccountModel{
					BlockNumber: mocks.BlockNumber.String(),
					HeaderID:    account.HeaderID,
					StatePath:   stateNode.Path,
					Balance:     "0",
					CodeHash:    mocks.ContractCodeHash.Bytes(),
					StorageRoot: mocks.ContractRoot,
					Nonce:       1,
				}, account)
			}
			if stateNode.CID == state2CID.String() {
				require.Equal(t, 2, stateNode.NodeType)
				require.Equal(t, common.BytesToHash(mocks.AccountLeafKey).Hex(), stateNode.StateKey)
				require.Equal(t, []byte{'\x0c'}, stateNode.Path)
				require.Equal(t, mocks.AccountLeafNode, data)
				require.Equal(t, models.StateAccountModel{
					BlockNumber: mocks.BlockNumber.String(),
					HeaderID:    account.HeaderID,
					StatePath:   stateNode.Path,
					Balance:     "1000",
					CodeHash:    mocks.AccountCodeHash.Bytes(),
					StorageRoot: mocks.AccountRoot,
					Nonce:       0,
				}, account)
			}
		}

		// check that Removed state nodes were properly indexed and published
		stateNodes = make([]models.StateNodeModel, 0)
		pgStr = `SELECT state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1 AND node_type = 3`
		err = sqlxdb.Select(&stateNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 2, len(stateNodes))
		for idx, stateNode := range stateNodes {
			var data []byte
			dc, err := cid.Decode(stateNode.CID)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			require.Equal(t, shared.RemovedNodeMhKey, prefixedKey)
			err = sqlxdb.Get(&data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
			if err != nil {
				t.Fatal(err)
			}

			if idx == 0 {
				require.Equal(t, shared.RemovedNodeStateCID, stateNode.CID)
				require.Equal(t, common.BytesToHash(mocks.RemovedLeafKey).Hex(), stateNode.StateKey)
				require.Equal(t, []byte{'\x02'}, stateNode.Path)
				require.Equal(t, []byte{}, data)
			}
			if idx == 1 {
				require.Equal(t, shared.RemovedNodeStateCID, stateNode.CID)
				require.Equal(t, common.BytesToHash(mocks.Contract2LeafKey).Hex(), stateNode.StateKey)
				require.Equal(t, []byte{'\x07'}, stateNode.Path)
				require.Equal(t, []byte{}, data)
			}
		}
	})

	t.Run("Publish and index storage IPLDs in a single tx", func(t *testing.T) {
		setupCSV(t)
		dumpCSVFileData(t)
		defer tearDownCSV(t)

		// check that storage nodes were properly indexed
		storageNodes := make([]models.StorageNodeWithStateKeyModel, 0)
		pgStr := `SELECT cast(storage_cids.block_number AS TEXT), storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE (storage_cids.state_path, storage_cids.header_id) = (state_cids.state_path, state_cids.header_id)
				AND state_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				AND storage_cids.node_type != 3`
		err = sqlxdb.Select(&storageNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 1, len(storageNodes))
		require.Equal(t, models.StorageNodeWithStateKeyModel{
			BlockNumber: mocks.BlockNumber.String(),
			CID:         storageCID.String(),
			NodeType:    2,
			StorageKey:  common.BytesToHash(mocks.StorageLeafKey).Hex(),
			StateKey:    common.BytesToHash(mocks.ContractLeafKey).Hex(),
			Path:        []byte{},
		}, storageNodes[0])
		var data []byte
		dc, err := cid.Decode(storageNodes[0].CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		err = sqlxdb.Get(&data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, mocks.StorageLeafNode, data)

		// check that Removed storage nodes were properly indexed
		storageNodes = make([]models.StorageNodeWithStateKeyModel, 0)
		pgStr = `SELECT cast(storage_cids.block_number AS TEXT), storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE (storage_cids.state_path, storage_cids.header_id) = (state_cids.state_path, state_cids.header_id)
				AND state_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				AND storage_cids.node_type = 3
				ORDER BY storage_path`
		err = sqlxdb.Select(&storageNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		require.Equal(t, 3, len(storageNodes))
		expectedStorageNodes := []models.StorageNodeWithStateKeyModel{
			{
				BlockNumber: mocks.BlockNumber.String(),
				CID:         shared.RemovedNodeStorageCID,
				NodeType:    3,
				StorageKey:  common.BytesToHash(mocks.RemovedLeafKey).Hex(),
				StateKey:    common.BytesToHash(mocks.ContractLeafKey).Hex(),
				Path:        []byte{'\x03'},
			},
			{
				BlockNumber: mocks.BlockNumber.String(),
				CID:         shared.RemovedNodeStorageCID,
				NodeType:    3,
				StorageKey:  common.BytesToHash(mocks.Storage2LeafKey).Hex(),
				StateKey:    common.BytesToHash(mocks.Contract2LeafKey).Hex(),
				Path:        []byte{'\x0e'},
			},
			{
				BlockNumber: mocks.BlockNumber.String(),
				CID:         shared.RemovedNodeStorageCID,
				NodeType:    3,
				StorageKey:  common.BytesToHash(mocks.Storage3LeafKey).Hex(),
				StateKey:    common.BytesToHash(mocks.Contract2LeafKey).Hex(),
				Path:        []byte{'\x0f'},
			},
		}
		for idx, storageNode := range storageNodes {
			require.Equal(t, expectedStorageNodes[idx], storageNode)
			dc, err = cid.Decode(storageNode.CID)
			if err != nil {
				t.Fatal(err)
			}
			mhKey = dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey = blockstore.BlockPrefix.String() + mhKey.String()
			require.Equal(t, shared.RemovedNodeMhKey, prefixedKey, mocks.BlockNumber.Uint64())
			err = sqlxdb.Get(&data, ipfsPgGet, prefixedKey, mocks.BlockNumber.Uint64())
			if err != nil {
				t.Fatal(err)
			}
			require.Equal(t, []byte{}, data)
		}
	})
}

func TestCSVFileWatchAddressMethods(t *testing.T) {
	setupCSVIndexer(t)
	defer tearDownCSV(t)
	type res struct {
		Address      string `db:"address"`
		CreatedAt    uint64 `db:"created_at"`
		WatchedAt    uint64 `db:"watched_at"`
		LastFilledAt uint64 `db:"last_filled_at"`
	}
	pgStr := "SELECT * FROM eth_meta.watched_addresses"

	t.Run("Load watched addresses (empty table)", func(t *testing.T) {
		expectedData := []common.Address{}

		rows, err := ind.LoadWatchedAddresses()
		require.NoError(t, err)

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Insert watched addresses", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract1Address,
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
		}

		err = ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt1)))
		require.NoError(t, err)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Insert watched addresses (some already watched)", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract1Address,
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract3Address,
				CreatedAt:    contract3CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
		}

		// // Debug using different file
		// srcFile, err := os.Open(file.TableFile(file.TestConfig.OutputDir, sdtypes.TableWatchedAddresses.Name))
		// file.TestConfig.OutputDir = "./watched_addresses_test"
		// ind.Close()
		// ind, err = file.NewStateDiffIndexer(context.Background(), mocks.TestConfig, file.TestConfig)
		// require.NoError(t, err)
		// dstFile, err := os.OpenFile(
		// 	file.TableFile(file.TestConfig.OutputDir, sdtypes.TableWatchedAddresses.Name),
		// 	os.O_WRONLY|os.O_CREATE|os.O_APPEND,
		// 	0644,
		// )
		// written, err := io.Copy(dstFile, srcFile)
		// fmt.Println(written, err)

		err = ind.InsertWatchedAddresses(args, big.NewInt(int64(watchedAt2)))
		require.NoError(t, err)
		addresses, err := ind.LoadWatchedAddresses()
		fmt.Println("after insert", addresses)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		fmt.Println("rows", rows)

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	return

	t.Run("Remove watched addresses", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract1Address,
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt1,
				LastFilledAt: lastFilledAt,
			},
		}

		err = ind.RemoveWatchedAddresses(args)
		require.NoError(t, err)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Remove watched addresses (some non-watched)", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
		}
		expectedData := []res{}

		err = ind.RemoveWatchedAddresses(args)
		require.NoError(t, err)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Set watched addresses", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract1Address,
				CreatedAt: contract1CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract1Address,
				CreatedAt:    contract1CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract3Address,
				CreatedAt:    contract3CreatedAt,
				WatchedAt:    watchedAt2,
				LastFilledAt: lastFilledAt,
			},
		}

		err = ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt2)))
		require.NoError(t, err)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Set watched addresses (some already watched)", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		args := []sdtypes.WatchAddressArg{
			{
				Address:   contract4Address,
				CreatedAt: contract4CreatedAt,
			},
			{
				Address:   contract2Address,
				CreatedAt: contract2CreatedAt,
			},
			{
				Address:   contract3Address,
				CreatedAt: contract3CreatedAt,
			},
		}
		expectedData := []res{
			{
				Address:      contract4Address,
				CreatedAt:    contract4CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract2Address,
				CreatedAt:    contract2CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
			{
				Address:      contract3Address,
				CreatedAt:    contract3CreatedAt,
				WatchedAt:    watchedAt3,
				LastFilledAt: lastFilledAt,
			},
		}

		err = ind.SetWatchedAddresses(args, big.NewInt(int64(watchedAt3)))
		require.NoError(t, err)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Load watched addresses", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		expectedData := []common.Address{
			common.HexToAddress(contract4Address),
			common.HexToAddress(contract2Address),
			common.HexToAddress(contract3Address),
		}

		rows, err := ind.LoadWatchedAddresses()
		require.NoError(t, err)

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Clear watched addresses", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		expectedData := []res{}

		err = ind.ClearWatchedAddresses()
		require.NoError(t, err)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})

	t.Run("Clear watched addresses (empty table)", func(t *testing.T) {
		defer file.TearDownDB(t, sqlxdb)
		expectedData := []res{}

		err = ind.ClearWatchedAddresses()
		require.NoError(t, err)
		dumpCSVFileData(t)

		rows := []res{}
		err = sqlxdb.Select(&rows, pgStr)
		if err != nil {
			t.Fatal(err)
		}

		expectTrue(t, len(rows) == len(expectedData))
		for idx, row := range rows {
			require.Equal(t, expectedData[idx], row)
		}
	})
}
