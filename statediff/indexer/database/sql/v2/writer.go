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

package sql

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/models/v2"
	"github.com/ethereum/go-ethereum/statediff/indexer/node"
)

var (
	nullHash = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
)

// Writer handles processing and writing of indexed IPLD objects to Postgres
type Writer struct {
	DB      interfaces.Database
	metrics sql.IndexerMetricsHandles
	nodeID  int64
}

// NewWriter creates a new pointer to a Writer
func NewWriter(db interfaces.Database) *Writer {
	return &Writer{
		DB: db,
	}
}

// Close satisfies io.Closer
func (w *Writer) Close() error {
	return w.DB.Close()
}

/*
InsertNodeInfo inserts a node info model
INSERT INTO nodes (genesis_block, network_id, node_id, client_name, chain_id) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (genesis_block, network_id, node_id, chain_id) DO NOTHING
*/
func (w *Writer) InsertNodeInfo(info node.Info) error {
	var nodeID int64
	if err := w.DB.QueryRow(w.DB.Context(), w.DB.InsertNodeInfoStm(), info.GenesisBlock, info.NetworkID, info.ID,
		info.ClientName, info.ChainID).Scan(&nodeID); err != nil {
		return err
	}
	w.nodeID = nodeID
	return nil
}

/*
InsertHeaderCID inserts a header model
INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (block_number, block_hash) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee) = ($3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, eth.header_cids.times_validated + 1, $16)
*/
func (w *Writer) InsertHeaderCID(tx interfaces.Tx, header *models.HeaderModel) (int64, error) {
	var headerID int64
	err := tx.QueryRow(w.DB.Context(), w.DB.InsertHeaderStm(),
		header.BlockNumber, header.BlockHash, header.ParentHash, header.CID, header.TotalDifficulty, w.nodeID,
		header.Reward, header.StateRoot, header.TxRoot, header.RctRoot, header.UncleRoot, header.Bloom,
		header.Timestamp, header.MhKey, 1, header.BaseFee).Scan(&headerID)
	if err != nil {
		return 0, fmt.Errorf("error inserting header_cids entry: %v", err)
	}
	w.metrics.Blocks.Inc(1)
	return headerID, nil
}

/*
InsertUncleCID inserts an uncle model
INSERT INTO eth.uncle_cids (block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (header_id, block_hash) DO NOTHING
*/
func (w *Writer) InsertUncleCID(tx interfaces.Tx, uncle *models.UncleModel) error {
	_, err := tx.Exec(w.DB.Context(), w.DB.InsertUncleStm(),
		uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID, uncle.Reward, uncle.MhKey)
	if err != nil {
		return fmt.Errorf("error inserting uncle_cids entry: %v", err)
	}
	return nil
}

/*
InsertTransactionCID inserts a tx model
INSERT INTO eth.transaction_cids (header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (header_id, tx_hash) DO NOTHING
*/
func (w *Writer) InsertTransactionCID(tx interfaces.Tx, transaction *models.TxModel) (int64, error) {
	var txID int64
	err := tx.QueryRow(w.DB.Context(), w.DB.InsertTxStm(),
		transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src, transaction.Index,
		transaction.MhKey, transaction.Data, transaction.Type).Scan(&txID)
	if err != nil {
		return 0, fmt.Errorf("error inserting transaction_cids entry: %v", err)
	}
	w.metrics.Transactions.Inc(1)
	return txID, nil
}

/*
InsertAccessListElement inserts an access list element model
INSERT INTO eth.access_list_elements (tx_id, index, address, storage_keys) VALUES ($1, $2, $3, $4)
ON CONFLICT (tx_id, index) DO NOTHING
*/
func (w *Writer) InsertAccessListElement(tx interfaces.Tx, accessListElement *models.AccessListElementModel) error {
	_, err := tx.Exec(w.DB.Context(), w.DB.InsertAccessListElementStm(),
		accessListElement.TxID, accessListElement.Index, accessListElement.Address, accessListElement.StorageKeys)
	if err != nil {
		return fmt.Errorf("error inserting access_list_element entry: %v", err)
	}
	w.metrics.AccessListEntries.Inc(1)
	return nil
}

/*
InsertReceiptCID inserts a receipt model
INSERT INTO eth.receipt_cids (tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
ON CONFLICT (tx_id) DO NOTHING
*/
func (w *Writer) InsertReceiptCID(tx interfaces.Tx, rct *models.ReceiptModel) (int64, error) {
	var receiptID int64
	err := tx.QueryRow(w.DB.Context(), w.DB.InsertRctStm(),
		rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey, rct.PostState, rct.PostStatus, rct.LogRoot).Scan(&receiptID)
	if err != nil {
		return 0, fmt.Errorf("error inserting receipt_cids entry: %w", err)
	}
	w.metrics.Receipts.Inc(1)
	return receiptID, nil
}

/*
InsertLogCID inserts a log model
INSERT INTO eth.log_cids (leaf_cid, leaf_mh_key, rct_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (receipt_id, index) DO NOTHING
*/
func (w *Writer) InsertLogCID(tx interfaces.Tx, logs []*models.LogsModel) error {
	for _, log := range logs {
		_, err := tx.Exec(w.DB.Context(), w.DB.InsertLogStm(),
			log.LeafCID, log.LeafMhKey, log.ReceiptID, log.Address, log.Index, log.Topic0, log.Topic1, log.Topic2,
			log.Topic3, log.Data)
		if err != nil {
			return fmt.Errorf("error inserting logs entry: %w", err)
		}
		w.metrics.Logs.Inc(1)
	}
	return nil
}

/*
InsertStateCID inserts a state model
INSERT INTO eth.state_cids (header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (header_id, state_path) DO UPDATE SET (state_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)
*/
func (w *Writer) InsertStateCID(tx interfaces.Tx, stateNode *models.StateNodeModel) (int64, error) {
	var stateID int64
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	err := tx.QueryRow(w.DB.Context(), w.DB.InsertStateStm(),
		stateNode.HeaderID, stateKey, stateNode.CID, stateNode.Path, stateNode.NodeType, true, stateNode.MhKey).Scan(&stateID)
	if err != nil {
		return 0, fmt.Errorf("error inserting state_cids entry: %v", err)
	}
	return stateID, nil
}

/*
InsertStateAccount inserts a state account model
INSERT INTO eth.state_accounts (state_id, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (state_id) DO NOTHING
*/
func (w *Writer) InsertStateAccount(tx interfaces.Tx, stateAccount *models.StateAccountModel) error {
	_, err := tx.Exec(w.DB.Context(), w.DB.InsertAccountStm(),
		stateAccount.StateID, stateAccount.Balance, stateAccount.Nonce, stateAccount.CodeHash,
		stateAccount.StorageRoot)
	if err != nil {
		return fmt.Errorf("error inserting state_accounts entry: %v", err)
	}
	return nil
}

/*
InsertStorageCID inserts a storage model
INSERT INTO eth.storage_cids (state_id, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (state_id, storage_path) DO UPDATE SET (storage_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)
*/
func (w *Writer) InsertStorageCID(tx interfaces.Tx, storageCID *models.StorageNodeModel) error {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	_, err := tx.Exec(w.DB.Context(), w.DB.InsertStorageStm(),
		storageCID.StateID, storageKey, storageCID.CID, storageCID.Path, storageCID.NodeType,
		true, storageCID.MhKey)
	if err != nil {
		return fmt.Errorf("error inserting storage_cids entry: %v", err)
	}
	return nil
}

// Stats returns the stats for the underlying DB
func (w *Writer) Stats() interfaces.Stats {
	return w.DB.Stats()
}
