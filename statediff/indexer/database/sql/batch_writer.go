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

package sql

/*
import (
	"fmt"

	"github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres"

	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	"github.com/jmoiron/sqlx"
)

*/
/*
// PG_MAX_PARAMS is the max number of placeholders+args a statement can support
// above this limit we need to split into a separate batch
const PG_MAX_PARAMS int = 32767

const (
	ipldInsertPgStr string = `INSERT INTO public.blocks (key, data) VALUES (unnest($1), unnest($2)) ON CONFLICT (key) DO NOTHING`
	headerCIDsPgStr string = `INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee)
								VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), unnest($6), unnest($7), unnest($8), unnest($9), unnest($10), unnest($11), unnest($12), unnest($13), unnest($14), unnest($15), unnest($16))
								ON CONFLICT (block_number, block_hash) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee) = (excluded.parent_hash, excluded.cid, excluded.td, excluded.node_id, excluded.reward, excluded.state_root, excluded.tx_root, excluded.receipt_root, excluded.uncle_root, excluded.bloom, excluded.timestamp, excluded.mh_key, eth.header_cids.times_validated + 1, excluded.base_fee)
								RETURNING id`
	unclesCIDsPgStr string = `INSERT INTO eth.uncle_cids (block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), unnest($6))
								ON CONFLICT (header_id, block_hash) DO UPDATE SET (parent_hash, cid, reward, mh_key) = (excluded.parent_hash, excluded.cid, excluded.reward, excluded.mh_key)`
	txCIDsPgStr string = `INSERT INTO eth.transaction_cids (header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type) VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), unnest($6), unnest($7), unnest($8), unnest($9))
									ON CONFLICT (header_id, tx_hash) DO UPDATE SET (cid, dst, src, index, mh_key, tx_data, tx_type) = (excluded.cid, excluded.dst, excluded.src, excluded.index, excluded.mh_key, excluded.tx_data, excluded.tx_type)
									RETURNING id`
	accessListPgStr string = `INSERT INTO eth.access_list_elements (tx_id, index, address, storage_keys) VALUES (unnest($1), unnest($2), unnest($3), unnest($4))
								ON CONFLICT (tx_id, index) DO UPDATE SET (address, storage_keys) = (excluded.address, excluded.storage_keys)`
	rctCIDsPgStr string = `INSERT INTO eth.receipt_cids (tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), unnest($6), unnest($7), unnest($8))
 							  ON CONFLICT (tx_id) DO UPDATE SET (leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) = (excluded.leaf_cid, excluded.contract, excluded.contract_hash, excluded.leaf_mh_key, excluded.post_state, excluded.post_status, excluded.log_root)
 							  RETURNING id`
	logCIDsPgStr string = `INSERT INTO eth.log_cids (leaf_cid, leaf_mh_key, receipt_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), unnest($6), unnest($7), unnest($8), unnest($9), unnest($10))
							ON CONFLICT (receipt_id, index) DO UPDATE SET (leaf_cid, leaf_mh_key, address, topic0, topic1, topic2, topic3, log_data) = (excluded.leaf_cid, excluded.leaf_mh_key, excluded.address, excluded.topic0, excluded.topic1, excluded.topic2, excluded.topic3, excluded.log_data)`
	stateCIDsPgStr string = `INSERT INTO eth.state_cids (header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), unnest($6), unnest($7))
								ON CONFLICT (header_id, state_path) DO UPDATE SET (state_leaf_key, cid, node_type, diff, mh_key) = (excluded.state_leaf_key, excluded.cid, excluded.node_type, excluded.diff, excluded.mh_key)
								RETURNING id`
	stateAccountsPgStr string = `INSERT INTO eth.state_accounts (state_id, balance, nonce, code_hash, storage_root) VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5))
							  ON CONFLICT (state_id) DO UPDATE SET (balance, nonce, code_hash, storage_root) = (excluded.balance, excluded.nonce, excluded.code_hash, excluded.storage_root)`
	storageCIDsPgStr string = `INSERT INTO eth.storage_cids (state_id, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES (unnest($1), unnest($2), unnest($3), unnest($4), unnest($5), unnest($6), unnest($7))
							  ON CONFLICT (state_id, storage_path) DO UPDATE SET (storage_leaf_key, cid, node_type, diff, mh_key) = (excluded.storage_leaf_key, excluded.cid, excluded.node_type, excluded.diff, excluded.mh_key)`
)

// PostgresBatchWriter is used to write statediff data to Postgres using batch inserts/upserts
type PostgresBatchWriter struct {
	db *postgres.DB

	// prepared statements (prepared inside tx)
	ipldsPreparedStm      *sqlx.Stmt
	unclesPrepared        *sqlx.Stmt
	txPreparedStm         *sqlx.Stmt
	accessListPreparedStm *sqlx.Stmt
	rctPreparedStm        *sqlx.Stmt
	logPreparedStm        *sqlx.Stmt
	statePreparedStm      *sqlx.Stmt
	accountPreparedStm    *sqlx.Stmt
	storagePreparedStm    *sqlx.Stmt

	// cached arguments
	queuedHeaderArgs     models.HeaderModel
	queuedUnclesArgs     models.UncleBatch
	queuedTxArgs         models.TxBatch
	queuedAccessListArgs models.AccessListBatch
	queuedRctArgs        models.ReceiptBatch
	queuedLogArgs        models.LogBatch
	queuedStateArgs      models.StateBatch
	queuedAccountArgs    models.AccountBatch
	queuedStorageArgs    models.StorageBatch
}

// NewPostgresBatchWriter creates a new pointer to a PostgresBatchWriter
func NewPostgresBatchWriter(db *postgres.DB) *PostgresBatchWriter {
	return &PostgresBatchWriter{
		db: db,
	}
}

func (pbw *PostgresBatchWriter) queueHeader(header models.HeaderModel) {
	pbw.queuedHeaderArgs = header
}

func (pbw *PostgresBatchWriter) queueUncle(uncle models.UncleModel) {
	pbw.queuedUnclesArgs.BlockHashes = append(pbw.queuedUnclesArgs.BlockHashes, uncle.BlockHash)
	pbw.queuedUnclesArgs.ParentHashes = append(pbw.queuedUnclesArgs.ParentHashes, uncle.ParentHash)
	pbw.queuedUnclesArgs.CIDs = append(pbw.queuedUnclesArgs.CIDs, uncle.CID)
	pbw.queuedUnclesArgs.MhKeys = append(pbw.queuedUnclesArgs.MhKeys, uncle.MhKey)
	pbw.queuedUnclesArgs.Rewards = append(pbw.queuedUnclesArgs.Rewards, uncle.Reward)
}

func (pbw *PostgresBatchWriter) queueTransaction(tx models.TxModel) {
	pbw.queuedTxArgs.Indexes = append(pbw.queuedTxArgs.Indexes, tx.Index)
	pbw.queuedTxArgs.TxHashes = append(pbw.queuedTxArgs.TxHashes, tx.TxHash)
	pbw.queuedTxArgs.CIDs = append(pbw.queuedTxArgs.CIDs, tx.CID)
	pbw.queuedTxArgs.MhKeys = append(pbw.queuedTxArgs.MhKeys, tx.MhKey)
	pbw.queuedTxArgs.Dsts = append(pbw.queuedTxArgs.Dsts, tx.Dst)
	pbw.queuedTxArgs.Srcs = append(pbw.queuedTxArgs.Srcs, tx.Src)
	pbw.queuedTxArgs.Datas = append(pbw.queuedTxArgs.Datas, tx.Data)
	pbw.queuedTxArgs.Types = append(pbw.queuedTxArgs.Types, tx.Type)
}

func (pbw *PostgresBatchWriter) queueAccessListElement(al models.AccessListElementModel) {

}

func (pbw *PostgresBatchWriter) queueReceipt(rct models.ReceiptModel) {

}

func (pbw *PostgresBatchWriter) upsertTransactionCID(tx *sqlx.Tx, transaction models.TxModel, headerID int64) (int64, error) {
	var txID int64
	err := tx.QueryRowx(`INSERT INTO eth.transaction_cids (header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
									ON CONFLICT (header_id, tx_hash) DO UPDATE SET (cid, dst, src, index, mh_key, tx_data, tx_type) = ($3, $4, $5, $6, $7, $8, $9)
									RETURNING id`,
		headerID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src, transaction.Index, transaction.MhKey, transaction.Data, transaction.Type).Scan(&txID)
	if err != nil {
		return 0, fmt.Errorf("error upserting transaction_cids entry: %v", err)
	}
	indexerMetrics.transactions.Inc(1)
	return txID, nil
}

func (pbw *PostgresBatchWriter) upsertAccessListElement(tx *sqlx.Tx, accessListElement models.AccessListElementModel, txID int64) error {
	_, err := tx.Exec(`INSERT INTO eth.access_list_elements (tx_id, index, address, storage_keys) VALUES ($1, $2, $3, $4)
								ON CONFLICT (tx_id, index) DO UPDATE SET (address, storage_keys) = ($3, $4)`,
		txID, accessListElement.Index, accessListElement.Address, accessListElement.StorageKeys)
	if err != nil {
		return fmt.Errorf("error upserting access_list_element entry: %v", err)
	}
	indexerMetrics.accessListEntries.Inc(1)
	return nil
}

func (pbw *PostgresBatchWriter) upsertReceiptCID(tx *sqlx.Tx, rct *models.ReceiptModel, txID int64) (int64, error) {
	var receiptID int64
	err := tx.QueryRowx(`INSERT INTO eth.receipt_cids (tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
 							  ON CONFLICT (tx_id) DO UPDATE SET (leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) = ($2, $3, $4, $5, $6, $7, $8)
 							  RETURNING id`,
		txID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey, rct.PostState, rct.PostStatus, rct.LogRoot).Scan(&receiptID)
	if err != nil {
		return 0, fmt.Errorf("error upserting receipt_cids entry: %w", err)
	}
	indexerMetrics.receipts.Inc(1)
	return receiptID, nil
}

func (pbw *PostgresBatchWriter) upsertLogCID(tx *sqlx.Tx, logs []*models.LogsModel, receiptID int64) error {
	for _, log := range logs {
		_, err := tx.Exec(`INSERT INTO eth.log_cids (leaf_cid, leaf_mh_key, receipt_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
					ON CONFLICT (receipt_id, index) DO UPDATE SET (leaf_cid, leaf_mh_key, address, topic0, topic1, topic2, topic3, log_data) = ($1, $2, $4, $6, $7, $8, $9, $10)`,
			log.LeafCID, log.LeafMhKey, receiptID, log.Address, log.Index, log.Topic0, log.Topic1, log.Topic2, log.Topic3, log.Data)
		if err != nil {
			return fmt.Errorf("error upserting logs entry: %w", err)
		}
		indexerMetrics.logs.Inc(1)
	}
	return nil
}

func (pbw *PostgresBatchWriter) upsertStateCID(tx *sqlx.Tx, stateNode models.StateNodeModel, headerID int64) (int64, error) {
	var stateID int64
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	err := tx.QueryRowx(`INSERT INTO eth.state_cids (header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
									ON CONFLICT (header_id, state_path) DO UPDATE SET (state_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)
									RETURNING id`,
		headerID, stateKey, stateNode.CID, stateNode.Path, stateNode.NodeType, true, stateNode.MhKey).Scan(&stateID)
	if err != nil {
		return 0, fmt.Errorf("error upserting state_cids entry: %v", err)
	}
	return stateID, nil
}

func (pbw *PostgresBatchWriter) upsertStateAccount(tx *sqlx.Tx, stateAccount models.StateAccountModel, stateID int64) error {
	_, err := tx.Exec(`INSERT INTO eth.state_accounts (state_id, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5)
							  ON CONFLICT (state_id) DO UPDATE SET (balance, nonce, code_hash, storage_root) = ($2, $3, $4, $5)`,
		stateID, stateAccount.Balance, stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot)
	if err != nil {
		return fmt.Errorf("error upserting state_accounts entry: %v", err)
	}
	return nil
}

func (pbw *PostgresBatchWriter) upsertStorageCID(tx *sqlx.Tx, storageCID models.StorageNodeModel, stateID int64) error {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	_, err := tx.Exec(`INSERT INTO eth.storage_cids (state_id, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
							  ON CONFLICT (state_id, storage_path) DO UPDATE SET (storage_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)`,
		stateID, storageKey, storageCID.CID, storageCID.Path, storageCID.NodeType, true, storageCID.MhKey)
	if err != nil {
		return fmt.Errorf("error upserting storage_cids entry: %v", err)
	}
	return nil
}
*/
