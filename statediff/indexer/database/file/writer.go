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

package file

import (
	"fmt"
	"os"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	node "github.com/ipfs/go-ipld-format"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
)

var (
	nullHash         = common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
	collatedStmtSize = 65336 // min(linuxPipeSize, macOSPipeSize)
)

// SQLWriter writes sql statements to a file
type SQLWriter struct {
	file           *os.File
	stmts          chan []byte
	collatedStmt   []byte
	collationIndex int

	quitChan chan struct{}
	doneChan chan struct{}
}

// NewSQLWriter creates a new pointer to a Writer
func NewSQLWriter(file *os.File) *SQLWriter {
	return &SQLWriter{
		file:         file,
		stmts:        make(chan []byte),
		collatedStmt: make([]byte, collatedStmtSize),
		quitChan:     make(chan struct{}),
		doneChan:     make(chan struct{}),
	}
}

// Loop enables concurrent writes to the underlying os.File
// since os.File does not buffer, it utilizes an internal buffer that is the size of a unix pipe
// by using copy() and tracking the index/size of the buffer, we require only the initial memory allocation
func (sqw *SQLWriter) Loop() {
	sqw.collationIndex = 0
	go func() {
		defer close(sqw.doneChan)
		var l int
		for {
			select {
			case stmt := <-sqw.stmts:
				l = len(stmt)
				if l+sqw.collationIndex+1 > collatedStmtSize {
					if err := sqw.flush(); err != nil {
						log.Error("error writing cached sql stmts to file", "err", err)
					}
				}
				copy(sqw.collatedStmt[sqw.collationIndex:sqw.collationIndex+l-1], stmt)
				sqw.collationIndex += l
			case <-sqw.quitChan:
				if err := sqw.flush(); err != nil {
					log.Error("error writing cached sql stmts to file", "err", err)
				}
				return
			}
		}
	}()
}

// Close satisfies io.Closer
func (sqw *SQLWriter) Close() error {
	close(sqw.quitChan)
	<-sqw.doneChan
	return nil
}

func (sqw *SQLWriter) flush() error {
	if _, err := sqw.file.Write(sqw.collatedStmt[0 : sqw.collationIndex-1]); err != nil {
		return err
	}
	sqw.collationIndex = 0
	return nil
}

const (
	ipldInsert = `INSERT INTO public.blocks (key, data) VALUES (%s, %x) ON CONFLICT (key) DO NOTHING;\n`

	headerInsert = `INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee)
VALUES (%s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %d, %s, %d, %d)
ON CONFLICT (block_hash) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee) = (%s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %d, %s, eth.header_cids.times_validated + 1, %d);\n`

	headerInsertWithoutBaseFee = `INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee)
VALUES (%s, %s, %s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %d, %s, %d, NULL)
ON CONFLICT (block_hash) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee) = (%s, %s, %s, %d, %s, %s, %s, %s, %s, %s, %d, %s, eth.header_cids.times_validated + 1, NULL);\n`

	uncleInsert = `INSERT INTO eth.uncle_cids (block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (block_hash) DO NOTHING;\n`

	txInsert = `INSERT INTO eth.transaction_cids (header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type) VALUES (%s, %s, %s, %s, %s, %d, %s, %s, %d)
ON CONFLICT (tx_hash) DO NOTHING;\n`

	alInsert = `INSERT INTO eth.access_list_element (tx_id, index, address, storage_keys) VALUES (%s, %d, %s, %s)
ON CONFLICT (tx_id, index) DO NOTHING;\n`

	rctInsert = `INSERT INTO eth.receipt_cids (tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES (%s, %s, %s, %s, %s, %s, %d, %s)
ON CONFLICT (tx_id) DO NOTHING;\n`

	logInsert = `INSERT INTO eth.log_cids (leaf_cid, leaf_mh_key, rct_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES (%s, %s, %s, %s, %d, %s, %s, %s, %s, %s)
ON CONFLICT (rct_id, index) DO NOTHING;\n`

	stateInsert = `INSERT INTO eth.state_cids (header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES (%s, %s, %s, %s, %d, %t, %s)
ON CONFLICT (header_id, state_path) DO UPDATE SET (state_leaf_key, cid, node_type, diff, mh_key) = (%s, %s, %d, %t, %s);\n`

	accountInsert = `INSERT INTO eth.state_accounts (header_id, state_path, balance, nonce, code_hash, storage_root) VALUES (%s, %s, %s, %d, %s, %s)
ON CONFLICT (header_id, state_path) DO NOTHING;\n`

	storageInsert = `INSERT INTO eth.storage_cids (header_id, state_path, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES (%s, %s, %s, %s, %s, %d, %t, %s)
ON CONFLICT (header_id, state_path, storage_path) DO UPDATE SET (storage_leaf_key, cid, node_type, diff, mh_key) = (%s, %s, %d, %t, %s);\n`
)

func (sqw *SQLWriter) upsertIPLD(ipld models.IPLDModel) {
	sqw.stmts <- []byte(fmt.Sprintf(ipldInsert, ipld.Key, ipld.Data))
}

func (sqw *SQLWriter) upsertIPLDDirect(key string, value []byte) {
	sqw.upsertIPLD(models.IPLDModel{
		Key:  key,
		Data: value,
	})
}

func (sqw *SQLWriter) upsertIPLDNode(i node.Node) {
	sqw.upsertIPLD(models.IPLDModel{
		Key:  blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(i.Cid().Hash()).String(),
		Data: i.RawData(),
	})
}

func (sqw *SQLWriter) upsertIPLDRaw(codec, mh uint64, raw []byte) (string, string, error) {
	c, err := ipld.RawdataToCid(codec, raw, mh)
	if err != nil {
		return "", "", err
	}
	prefixedKey := blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(c.Hash()).String()
	sqw.upsertIPLD(models.IPLDModel{
		Key:  prefixedKey,
		Data: raw,
	})
	return c.String(), prefixedKey, err
}

func (sqw *SQLWriter) upsertHeaderCID(header models.HeaderModel) {
	var stmt string
	if header.BaseFee == nil {
		stmt = fmt.Sprintf(headerInsertWithoutBaseFee, header.BlockNumber, header.BlockHash, header.ParentHash, header.CID,
			header.TotalDifficulty, header.NodeID, header.Reward, header.StateRoot, header.TxRoot,
			header.RctRoot, header.UncleRoot, header.Bloom, header.Timestamp, header.MhKey, 1,
			header.ParentHash, header.CID, header.TotalDifficulty, header.NodeID, header.Reward, header.StateRoot,
			header.TxRoot, header.RctRoot, header.UncleRoot, header.Bloom, header.Timestamp, header.MhKey)
	} else {
		stmt = fmt.Sprintf(headerInsert, header.BlockNumber, header.BlockHash, header.ParentHash, header.CID,
			header.TotalDifficulty, header.NodeID, header.Reward, header.StateRoot, header.TxRoot,
			header.RctRoot, header.UncleRoot, header.Bloom, header.Timestamp, header.MhKey, 1, header.BaseFee,
			header.ParentHash, header.CID, header.TotalDifficulty, header.NodeID, header.Reward, header.StateRoot,
			header.TxRoot, header.RctRoot, header.UncleRoot, header.Bloom, header.Timestamp, header.MhKey, header.BaseFee)
	}
	sqw.stmts <- []byte(stmt)
	indexerMetrics.blocks.Inc(1)
}

func (sqw *SQLWriter) upsertUncleCID(uncle models.UncleModel) {
	sqw.stmts <- []byte(fmt.Sprintf(uncleInsert, uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID, uncle.Reward, uncle.MhKey))
}

func (sqw *SQLWriter) upsertTransactionCID(transaction models.TxModel) {
	sqw.stmts <- []byte(fmt.Sprintf(txInsert, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst, transaction.Src, transaction.Index, transaction.MhKey, transaction.Data, transaction.Type))
	indexerMetrics.transactions.Inc(1)
}

func (sqw *SQLWriter) upsertAccessListElement(accessListElement models.AccessListElementModel) {
	sqw.stmts <- []byte(fmt.Sprintf(alInsert, accessListElement.TxID, accessListElement.Index, accessListElement.Address, formatPostgresStringArray(accessListElement.StorageKeys)))
	indexerMetrics.accessListEntries.Inc(1)
}

func (sqw *SQLWriter) upsertReceiptCID(rct *models.ReceiptModel) {
	sqw.stmts <- []byte(fmt.Sprintf(rctInsert, rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey, rct.PostState, rct.PostStatus, rct.LogRoot))
	indexerMetrics.receipts.Inc(1)
}

func (sqw *SQLWriter) upsertLogCID(logs []*models.LogsModel) {
	for _, l := range logs {
		sqw.stmts <- []byte(fmt.Sprintf(logInsert, l.LeafCID, l.LeafMhKey, l.ReceiptID, l.Address, l.Index, l.Topic0, l.Topic1, l.Topic2, l.Topic3, l.Data))
		indexerMetrics.logs.Inc(1)
	}
}

func (sqw *SQLWriter) upsertStateCID(stateNode models.StateNodeModel) {
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	sqw.stmts <- []byte(fmt.Sprintf(stateInsert, stateNode.HeaderID, stateKey, stateNode.CID, stateNode.Path, stateNode.NodeType,
		true, stateNode.MhKey, stateKey, stateNode.CID, stateNode.NodeType, true, stateNode.MhKey))
}

func (sqw *SQLWriter) upsertStateAccount(stateAccount models.StateAccountModel) {
	sqw.stmts <- []byte(fmt.Sprintf(accountInsert, stateAccount.HeaderID, stateAccount.StatePath, stateAccount.Balance,
		stateAccount.Nonce, stateAccount.CodeHash, stateAccount.StorageRoot))
}

func (sqw *SQLWriter) upsertStorageCID(storageCID models.StorageNodeModel) {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	sqw.stmts <- []byte(fmt.Sprintf(storageInsert, storageCID.HeaderID, storageCID.StatePath, storageKey, storageCID.CID,
		storageCID.Path, storageCID.NodeType, true, storageCID.MhKey, storageKey, storageCID.CID, storageCID.NodeType,
		true, storageCID.MhKey))
}
