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
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
)

var (
	nullHash = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
)

// Writer handles processing and writing of indexed IPLD objects to Postgres
type Writer struct {
	db Database
}

// NewWriter creates a new pointer to a Writer
func NewWriter(db Database) *Writer {
	return &Writer{
		db: db,
	}
}

func (in *Writer) upsertHeaderCID(tx Tx, header models.HeaderModel) (int64, error) {
	var headerID int64
	err := tx.QueryRow(in.db.Context(), in.db.InsertHeaderStm(),
		header.BlockNumber, header.BlockHash, header.ParentHash, header.CID, header.TotalDifficulty, in.db.NodeID(), header.Reward, header.StateRoot, header.TxRoot,
		header.RctRoot, header.UncleRoot, header.Bloom, header.Timestamp, header.MhKey, 1, header.BaseFee).Scan(&headerID)
	if err != nil {
		return 0, fmt.Errorf("error upserting header_cids entry: %v", err)
	}
	indexerMetrics.blocks.Inc(1)
	return headerID, nil
}

func (in *Writer) upsertUncleCID(tx Tx, uncle models.UncleModel, headerID int64) error {
	_, err := tx.Exec(in.db.Context(), in.db.InsertUncleStm(),
		uncle.BlockHash, headerID, uncle.ParentHash, uncle.CID, uncle.Reward, uncle.MhKey)
	if err != nil {
		return fmt.Errorf("error upserting uncle_cids entry: %v", err)
	}
	return nil
}

func (in *Writer) upsertTransactionCID(tx Tx, transaction models.TxModel, headerID int64) (int64, error) {
	var txID int64
	err := tx.QueryRow(in.db.Context(), in.db.InsertTxStm(),
		headerID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src, transaction.Index, transaction.MhKey, transaction.Data, transaction.Type).Scan(&txID)
	if err != nil {
		return 0, fmt.Errorf("error upserting transaction_cids entry: %v", err)
	}
	indexerMetrics.transactions.Inc(1)
	return txID, nil
}

func (in *Writer) upsertAccessListElement(tx Tx, accessListElement models.AccessListElementModel, txID int64) error {
	_, err := tx.Exec(in.db.Context(), in.db.InsertAccessListElementStm(),
		txID, accessListElement.Index, accessListElement.Address, accessListElement.StorageKeys)
	if err != nil {
		return fmt.Errorf("error upserting access_list_element entry: %v", err)
	}
	indexerMetrics.accessListEntries.Inc(1)
	return nil
}

func (in *Writer) upsertReceiptCID(tx Tx, rct *models.ReceiptModel, txID int64) (int64, error) {
	var receiptID int64
	err := tx.QueryRow(in.db.Context(), in.db.InsertRctStm(),
		txID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey, rct.PostState, rct.PostStatus, rct.LogRoot).Scan(&receiptID)
	if err != nil {
		return 0, fmt.Errorf("error upserting receipt_cids entry: %w", err)
	}
	indexerMetrics.receipts.Inc(1)
	return receiptID, nil
}

func (in *Writer) upsertLogCID(tx Tx, logs []*models.LogsModel, receiptID int64) error {
	for _, log := range logs {
		_, err := tx.Exec(in.db.Context(), in.db.InsertLogStm(),
			log.LeafCID, log.LeafMhKey, receiptID, log.Address, log.Index, log.Topic0, log.Topic1, log.Topic2, log.Topic3, log.Data)
		if err != nil {
			return fmt.Errorf("error upserting logs entry: %w", err)
		}
		indexerMetrics.logs.Inc(1)
	}
	return nil
}

func (in *Writer) upsertStateCID(tx Tx, stateNode models.StateNodeModel, headerID int64) (int64, error) {
	var stateID int64
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	err := tx.QueryRow(in.db.Context(), in.db.InsertStateStm(),
		headerID, stateKey, stateNode.CID, stateNode.Path, stateNode.NodeType, true, stateNode.MhKey).Scan(&stateID)
	if err != nil {
		return 0, fmt.Errorf("error upserting state_cids entry: %v", err)
	}
	return stateID, nil
}

func (in *Writer) upsertStateAccount(tx Tx, stateAccount models.StateAccountModel, stateID int64) error {
	_, err := tx.Exec(in.db.Context(), in.db.InsertAccountStm(),
		stateID, stateAccount.Balance, stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot)
	if err != nil {
		return fmt.Errorf("error upserting state_accounts entry: %v", err)
	}
	return nil
}

func (in *Writer) upsertStorageCID(tx Tx, storageCID models.StorageNodeModel, stateID int64) error {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	_, err := tx.Exec(in.db.Context(), in.db.InsertStorageStm(),
		stateID, storageKey, storageCID.CID, storageCID.Path, storageCID.NodeType, true, storageCID.MhKey)
	if err != nil {
		return fmt.Errorf("error upserting storage_cids entry: %v", err)
	}
	return nil
}
