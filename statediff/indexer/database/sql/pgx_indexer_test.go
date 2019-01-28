// VulcanizeDB
// Copyright © 2019 Vulcanize

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

package sql_test

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/mocks"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	"github.com/ethereum/go-ethereum/statediff/indexer/test_helpers"
)

func setupPGX(t *testing.T) {
	db, err = postgres.SetupPGXDB()
	if err != nil {
		t.Fatal(err)
	}
	ind, err = sql.NewStateDiffIndexer(context.Background(), mocks.TestConfig, db)
	require.NoError(t, err)
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
	}()
	for _, node := range mocks.StateDiffs {
		err = ind.PushStateNode(tx, node, mockBlock.Hash().String())
		require.NoError(t, err)
	}

	test_helpers.ExpectEqual(t, tx.(*sql.BatchTx).BlockNumber, mocks.BlockNumber.Uint64())
}

func TestPGXIndexer(t *testing.T) {
	t.Run("Publish and index header IPLDs in a single tx", func(t *testing.T) {
		setupPGX(t)
		defer tearDown(t)
		pgStr := `SELECT cid, cast(td AS TEXT), cast(reward AS TEXT), block_hash, coinbase
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
		err = db.QueryRow(context.Background(), pgStr, mocks.BlockNumber.Uint64()).Scan(
			&header.CID,
			&header.TD,
			&header.Reward,
			&header.BlockHash,
			&header.Coinbase)
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, header.CID, headerCID.String())
		test_helpers.ExpectEqual(t, header.TD, mocks.MockBlock.Difficulty().String())
		test_helpers.ExpectEqual(t, header.Reward, "2000000000000021250")
		test_helpers.ExpectEqual(t, header.Coinbase, mocks.MockHeader.Coinbase.String())
		dc, err := cid.Decode(header.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		var data []byte
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, data, mocks.MockHeaderRlp)
	})

	t.Run("Publish and index transaction IPLDs in a single tx", func(t *testing.T) {
		setupPGX(t)
		defer tearDown(t)
		// check that txs were properly indexed and published
		trxs := make([]string, 0)
		pgStr := `SELECT transaction_cids.cid FROM eth.transaction_cids INNER JOIN eth.header_cids ON (transaction_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1`
		err = db.Select(context.Background(), &trxs, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, len(trxs), 5)
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
			err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey)
			if err != nil {
				t.Fatal(err)
			}
			txTypeAndValueStr := `SELECT tx_type, CAST(value as TEXT) FROM eth.transaction_cids WHERE cid = $1`
			switch c {
			case trx1CID.String():
				test_helpers.ExpectEqual(t, data, tx1)
				txRes := new(txResult)
				err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
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
				test_helpers.ExpectEqual(t, data, tx2)
				txRes := new(txResult)
				err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
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
				test_helpers.ExpectEqual(t, data, tx3)
				txRes := new(txResult)
				err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
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
				test_helpers.ExpectEqual(t, data, tx4)
				txRes := new(txResult)
				err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
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
				pgStr = `SELECT access_list_elements.* FROM eth.access_list_elements INNER JOIN eth.transaction_cids ON (tx_id = transaction_cids.tx_hash) WHERE cid = $1 ORDER BY access_list_elements.index ASC`
				err = db.Select(context.Background(), &accessListElementModels, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				if len(accessListElementModels) != 2 {
					t.Fatalf("expected two access list entries, got %d", len(accessListElementModels))
				}
				model1 := models.AccessListElementModel{
					Index:   accessListElementModels[0].Index,
					Address: accessListElementModels[0].Address,
				}
				model2 := models.AccessListElementModel{
					Index:       accessListElementModels[1].Index,
					Address:     accessListElementModels[1].Address,
					StorageKeys: accessListElementModels[1].StorageKeys,
				}
				test_helpers.ExpectEqual(t, model1, mocks.AccessListEntry1Model)
				test_helpers.ExpectEqual(t, model2, mocks.AccessListEntry2Model)
			case trx5CID.String():
				test_helpers.ExpectEqual(t, data, tx5)
				txRes := new(txResult)
				err = db.QueryRow(context.Background(), txTypeAndValueStr, c).Scan(&txRes.TxType, &txRes.Value)
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
		setupPGX(t)
		defer tearDown(t)

		rcts := make([]string, 0)
		rctsPgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.tx_hash
				AND transaction_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				ORDER BY transaction_cids.index`
		logsPgStr := `SELECT log_cids.index, log_cids.address, log_cids.topic0, log_cids.topic1, data FROM eth.log_cids
    				INNER JOIN eth.receipt_cids ON (log_cids.rct_id = receipt_cids.tx_id)
					INNER JOIN public.blocks ON (log_cids.leaf_mh_key = blocks.key)
					WHERE receipt_cids.leaf_cid = $1 ORDER BY eth.log_cids.index ASC`
		err = db.Select(context.Background(), &rcts, rctsPgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		if len(rcts) != len(mocks.MockReceipts) {
			t.Fatalf("expected %d receipts, got %d", len(mocks.MockReceipts), len(rcts))
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
			err = db.Select(context.Background(), &results, logsPgStr, rcts[i])
			require.NoError(t, err)

			expectedLogs := mocks.MockReceipts[i].Logs
			test_helpers.ExpectEqual(t, len(results), len(expectedLogs))

			var nodeElements []interface{}
			for idx, r := range results {
				// Attempt to decode the log leaf node.
				err = rlp.DecodeBytes(r.Data, &nodeElements)
				require.NoError(t, err)
				if len(nodeElements) == 2 {
					log := new(types.Log)
					rlp.DecodeBytes(nodeElements[1].([]byte), log)
					logRaw, err := rlp.EncodeToBytes(expectedLogs[idx])
					require.NoError(t, err)
					// 2nd element of the leaf node contains the encoded log data.
					test_helpers.ExpectEqual(t, logRaw, nodeElements[1].([]byte))
				} else {
					log := new(types.Log)
					rlp.DecodeBytes(r.Data, log)
					logRaw, err := rlp.EncodeToBytes(expectedLogs[idx])
					require.NoError(t, err)
					// raw log was IPLDized
					test_helpers.ExpectEqual(t, logRaw, r.Data)
				}
			}
		}
	})

	t.Run("Publish and index receipt IPLDs in a single tx", func(t *testing.T) {
		setupPGX(t)
		defer tearDown(t)

		// check receipts were properly indexed and published
		rcts := make([]string, 0)
		pgStr := `SELECT receipt_cids.leaf_cid FROM eth.receipt_cids, eth.transaction_cids, eth.header_cids
				WHERE receipt_cids.tx_id = transaction_cids.tx_hash
				AND transaction_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1 order by transaction_cids.index`
		err = db.Select(context.Background(), &rcts, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, len(rcts), 5)
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
			err = db.Select(context.Background(), &result, pgStr, c)
			if err != nil {
				t.Fatal(err)
			}

			// Decode the log leaf node.
			var nodeElements []interface{}
			err = rlp.DecodeBytes(result[0].Data, &nodeElements)
			require.NoError(t, err)

			expectedRct, err := mocks.MockReceipts[idx].MarshalBinary()
			require.NoError(t, err)

			test_helpers.ExpectEqual(t, expectedRct, nodeElements[1].([]byte))

			dc, err := cid.Decode(c)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			var data []byte
			err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey)
			if err != nil {
				t.Fatal(err)
			}

			postStatePgStr := `SELECT post_state FROM eth.receipt_cids WHERE leaf_cid = $1`
			switch c {
			case rct1CID.String():
				test_helpers.ExpectEqual(t, data, rctLeaf1)
				var postStatus uint64
				pgStr = `SELECT post_status FROM eth.receipt_cids WHERE leaf_cid = $1`
				err = db.Get(context.Background(), &postStatus, pgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				test_helpers.ExpectEqual(t, postStatus, mocks.ExpectedPostStatus)
			case rct2CID.String():
				test_helpers.ExpectEqual(t, data, rctLeaf2)
				var postState string
				err = db.Get(context.Background(), &postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				test_helpers.ExpectEqual(t, postState, mocks.ExpectedPostState1)
			case rct3CID.String():
				test_helpers.ExpectEqual(t, data, rctLeaf3)
				var postState string
				err = db.Get(context.Background(), &postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				test_helpers.ExpectEqual(t, postState, mocks.ExpectedPostState2)
			case rct4CID.String():
				test_helpers.ExpectEqual(t, data, rctLeaf4)
				var postState string
				err = db.Get(context.Background(), &postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				test_helpers.ExpectEqual(t, postState, mocks.ExpectedPostState3)
			case rct5CID.String():
				test_helpers.ExpectEqual(t, data, rctLeaf5)
				var postState string
				err = db.Get(context.Background(), &postState, postStatePgStr, c)
				if err != nil {
					t.Fatal(err)
				}
				test_helpers.ExpectEqual(t, postState, mocks.ExpectedPostState3)
			}
		}
	})

	t.Run("Publish and index state IPLDs in a single tx", func(t *testing.T) {
		setupPGX(t)
		defer tearDown(t)
		// check that state nodes were properly indexed and published
		stateNodes := make([]models.StateNodeModel, 0)
		pgStr := `SELECT state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1 AND node_type != 3`
		err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, len(stateNodes), 2)
		for _, stateNode := range stateNodes {
			var data []byte
			dc, err := cid.Decode(stateNode.CID)
			if err != nil {
				t.Fatal(err)
			}
			mhKey := dshelp.MultihashToDsKey(dc.Hash())
			prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
			err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey)
			if err != nil {
				t.Fatal(err)
			}
			pgStr = `SELECT header_id, state_path, cast(balance AS TEXT), nonce, code_hash, storage_root from eth.state_accounts WHERE header_id = $1 AND state_path = $2`
			var account models.StateAccountModel
			err = db.Get(context.Background(), &account, pgStr, stateNode.HeaderID, stateNode.Path)
			if err != nil {
				t.Fatal(err)
			}
			if stateNode.CID == state1CID.String() {
				test_helpers.ExpectEqual(t, stateNode.NodeType, 2)
				test_helpers.ExpectEqual(t, stateNode.StateKey, common.BytesToHash(mocks.ContractLeafKey).Hex())
				test_helpers.ExpectEqual(t, stateNode.Path, []byte{'\x06'})
				test_helpers.ExpectEqual(t, data, mocks.ContractLeafNode)
				test_helpers.ExpectEqual(t, account, models.StateAccountModel{
					HeaderID:    account.HeaderID,
					StatePath:   stateNode.Path,
					Balance:     "0",
					CodeHash:    mocks.ContractCodeHash.Bytes(),
					StorageRoot: mocks.ContractRoot,
					Nonce:       1,
				})
			}
			if stateNode.CID == state2CID.String() {
				test_helpers.ExpectEqual(t, stateNode.NodeType, 2)
				test_helpers.ExpectEqual(t, stateNode.StateKey, common.BytesToHash(mocks.AccountLeafKey).Hex())
				test_helpers.ExpectEqual(t, stateNode.Path, []byte{'\x0c'})
				test_helpers.ExpectEqual(t, data, mocks.AccountLeafNode)
				test_helpers.ExpectEqual(t, account, models.StateAccountModel{
					HeaderID:    account.HeaderID,
					StatePath:   stateNode.Path,
					Balance:     "1000",
					CodeHash:    mocks.AccountCodeHash.Bytes(),
					StorageRoot: mocks.AccountRoot,
					Nonce:       0,
				})
			}
		}

		// check that Removed state nodes were properly indexed and published
		stateNodes = make([]models.StateNodeModel, 0)
		pgStr = `SELECT state_cids.cid, state_cids.state_leaf_key, state_cids.node_type, state_cids.state_path, state_cids.header_id
				FROM eth.state_cids INNER JOIN eth.header_cids ON (state_cids.header_id = header_cids.block_hash)
				WHERE header_cids.block_number = $1 AND node_type = 3`
		err = db.Select(context.Background(), &stateNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, len(stateNodes), 1)
		stateNode := stateNodes[0]
		var data []byte
		dc, err := cid.Decode(stateNode.CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		test_helpers.ExpectEqual(t, prefixedKey, shared.RemovedNodeMhKey)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, stateNode.CID, shared.RemovedNodeStateCID)
		test_helpers.ExpectEqual(t, stateNode.Path, []byte{'\x02'})
		test_helpers.ExpectEqual(t, data, []byte{})
	})

	t.Run("Publish and index storage IPLDs in a single tx", func(t *testing.T) {
		setupPGX(t)
		defer tearDown(t)
		// check that storage nodes were properly indexed
		storageNodes := make([]models.StorageNodeWithStateKeyModel, 0)
		pgStr := `SELECT storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE (storage_cids.state_path, storage_cids.header_id) = (state_cids.state_path, state_cids.header_id)
				AND state_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				AND storage_cids.node_type != 3`
		err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, len(storageNodes), 1)
		test_helpers.ExpectEqual(t, storageNodes[0], models.StorageNodeWithStateKeyModel{
			CID:        storageCID.String(),
			NodeType:   2,
			StorageKey: common.BytesToHash(mocks.StorageLeafKey).Hex(),
			StateKey:   common.BytesToHash(mocks.ContractLeafKey).Hex(),
			Path:       []byte{},
		})
		var data []byte
		dc, err := cid.Decode(storageNodes[0].CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey := dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey := blockstore.BlockPrefix.String() + mhKey.String()
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, data, mocks.StorageLeafNode)

		// check that Removed storage nodes were properly indexed
		storageNodes = make([]models.StorageNodeWithStateKeyModel, 0)
		pgStr = `SELECT storage_cids.cid, state_cids.state_leaf_key, storage_cids.storage_leaf_key, storage_cids.node_type, storage_cids.storage_path
				FROM eth.storage_cids, eth.state_cids, eth.header_cids
				WHERE (storage_cids.state_path, storage_cids.header_id) = (state_cids.state_path, state_cids.header_id)
				AND state_cids.header_id = header_cids.block_hash
				AND header_cids.block_number = $1
				AND storage_cids.node_type = 3`
		err = db.Select(context.Background(), &storageNodes, pgStr, mocks.BlockNumber.Uint64())
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, len(storageNodes), 1)
		test_helpers.ExpectEqual(t, storageNodes[0], models.StorageNodeWithStateKeyModel{
			CID:        shared.RemovedNodeStorageCID,
			NodeType:   3,
			StorageKey: common.BytesToHash(mocks.RemovedLeafKey).Hex(),
			StateKey:   common.BytesToHash(mocks.ContractLeafKey).Hex(),
			Path:       []byte{'\x03'},
		})
		dc, err = cid.Decode(storageNodes[0].CID)
		if err != nil {
			t.Fatal(err)
		}
		mhKey = dshelp.MultihashToDsKey(dc.Hash())
		prefixedKey = blockstore.BlockPrefix.String() + mhKey.String()
		test_helpers.ExpectEqual(t, prefixedKey, shared.RemovedNodeMhKey)
		err = db.Get(context.Background(), &data, ipfsPgGet, prefixedKey)
		if err != nil {
			t.Fatal(err)
		}
		test_helpers.ExpectEqual(t, data, []byte{})
	})
}
