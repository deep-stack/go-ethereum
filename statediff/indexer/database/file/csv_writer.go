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

package file

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	node "github.com/ipfs/go-ipld-format"

	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
	nodeinfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/types"
)

var (
	Tables = []*types.Table{
		&types.TableIPLDBlock,
		&types.TableNodeInfo,
		&types.TableHeader,
		&types.TableStateNode,
		&types.TableStorageNode,
		&types.TableUncle,
		&types.TableTransaction,
		&types.TableAccessListElement,
		&types.TableReceipt,
		&types.TableLog,
		&types.TableStateAccount,
	}
)

type CSVWriter struct {
	dir     string // dir containing output files
	writers fileWriters
}

type fileWriter struct {
	*csv.Writer
}

// fileWriters wraps the file writers for each output table
type fileWriters map[string]fileWriter

func newFileWriter(path string) (ret fileWriter, err error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return
	}
	ret = fileWriter{csv.NewWriter(file)}
	return
}

func (tx fileWriters) write(tbl *types.Table, args ...interface{}) error {
	row := tbl.ToCsvRow(args...)
	return tx[tbl.Name].Write(row)
}

func makeFileWriters(dir string, tables []*types.Table) (fileWriters, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	writers := fileWriters{}
	for _, tbl := range tables {
		w, err := newFileWriter(TableFile(dir, tbl.Name))
		if err != nil {
			return nil, err
		}
		writers[tbl.Name] = w
	}
	return writers, nil
}

func (tx fileWriters) flush() error {
	for _, w := range tx {
		w.Flush()
		if err := w.Error(); err != nil {
			return err
		}
	}
	return nil
}

func NewCSVWriter(path string) (*CSVWriter, error) {
	if err := os.MkdirAll(path, 0777); err != nil {
		return nil, fmt.Errorf("unable to make MkdirAll for path: %s err: %s", path, err)
	}
	writers, err := makeFileWriters(path, Tables)
	if err != nil {
		return nil, err
	}
	csvWriter := &CSVWriter{
		writers: writers,
		dir:     path,
	}
	return csvWriter, nil
}

// Flush sends a flush signal to the looping process
func (csw *CSVWriter) Flush() {
	csw.writers.flush()
}

func TableFile(dir, name string) string { return filepath.Join(dir, name+".csv") }

// Close satisfies io.Closer
func (csw *CSVWriter) Close() error {
	return csw.writers.flush()
}

func (csw *CSVWriter) upsertNode(node nodeinfo.Info) {
	csw.writers.write(&types.TableNodeInfo, node.GenesisBlock, node.NetworkID, node.ID, node.ClientName, node.ChainID)
	csw.writers.flush()
}

func (csw *CSVWriter) upsertIPLD(ipld models.IPLDModel) {
	csw.writers.write(&types.TableIPLDBlock, ipld.BlockNumber, ipld.Key, ipld.Data)
	csw.writers.flush()
}

func (csw *CSVWriter) upsertIPLDDirect(blockNumber, key string, value []byte) {
	csw.upsertIPLD(models.IPLDModel{
		BlockNumber: blockNumber,
		Key:         key,
		Data:        value,
	})
}

func (csw *CSVWriter) upsertIPLDNode(blockNumber string, i node.Node) {
	csw.upsertIPLD(models.IPLDModel{
		BlockNumber: blockNumber,
		Key:         blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(i.Cid().Hash()).String(),
		Data:        i.RawData(),
	})
}

func (csw *CSVWriter) upsertIPLDRaw(blockNumber string, codec, mh uint64, raw []byte) (string, string, error) {
	c, err := ipld.RawdataToCid(codec, raw, mh)
	if err != nil {
		return "", "", err
	}
	prefixedKey := blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(c.Hash()).String()
	csw.upsertIPLD(models.IPLDModel{
		BlockNumber: blockNumber,
		Key:         prefixedKey,
		Data:        raw,
	})
	return c.String(), prefixedKey, err
}

func (csw *CSVWriter) upsertHeaderCID(header models.HeaderModel) {
	csw.writers.write(&types.TableHeader, header.BlockNumber, header.BlockHash, header.ParentHash, header.CID,
		header.TotalDifficulty, header.NodeID, header.Reward, header.StateRoot, header.TxRoot,
		header.RctRoot, header.UncleRoot, header.Bloom, strconv.FormatUint(header.Timestamp, 10), header.MhKey, 1, header.Coinbase)
	csw.writers.flush()
	indexerMetrics.blocks.Inc(1)
}

func (csw *CSVWriter) upsertUncleCID(uncle models.UncleModel) {
	csw.writers.write(&types.TableUncle, uncle.BlockNumber, uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID,
		uncle.Reward, uncle.MhKey)
	csw.writers.flush()
}

func (csw *CSVWriter) upsertTransactionCID(transaction models.TxModel) {
	csw.writers.write(&types.TableTransaction, transaction.BlockNumber, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst,
		transaction.Src, transaction.Index, transaction.MhKey, transaction.Data, transaction.Type, transaction.Value)
	csw.writers.flush()
	indexerMetrics.transactions.Inc(1)
}

func (csw *CSVWriter) upsertAccessListElement(accessListElement models.AccessListElementModel) {
	csw.writers.write(&types.TableAccessListElement, accessListElement.BlockNumber, accessListElement.TxID, accessListElement.Index, accessListElement.Address, accessListElement.StorageKeys)
	csw.writers.flush()
	indexerMetrics.accessListEntries.Inc(1)
}

func (csw *CSVWriter) upsertReceiptCID(rct *models.ReceiptModel) {
	csw.writers.write(&types.TableReceipt, rct.BlockNumber, rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey,
		rct.PostState, rct.PostStatus, rct.LogRoot)
	csw.writers.flush()
	indexerMetrics.receipts.Inc(1)
}

func (csw *CSVWriter) upsertLogCID(logs []*models.LogsModel) {
	for _, l := range logs {
		csw.writers.write(&types.TableLog, l.BlockNumber, l.LeafCID, l.LeafMhKey, l.ReceiptID, l.Address, l.Index, l.Topic0,
			l.Topic1, l.Topic2, l.Topic3, l.Data)
		indexerMetrics.logs.Inc(1)
	}
	csw.writers.flush()
}

func (csw *CSVWriter) upsertStateCID(stateNode models.StateNodeModel) {
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}
	csw.writers.write(&types.TableStateNode, stateNode.BlockNumber, stateNode.HeaderID, stateKey, stateNode.CID, stateNode.Path,
		stateNode.NodeType, true, stateNode.MhKey)
	csw.writers.flush()
}

func (csw *CSVWriter) upsertStateAccount(stateAccount models.StateAccountModel) {
	csw.writers.write(&types.TableStateAccount, stateAccount.BlockNumber, stateAccount.HeaderID, stateAccount.StatePath, stateAccount.Balance,
		strconv.FormatUint(stateAccount.Nonce, 10), stateAccount.CodeHash, stateAccount.StorageRoot)
	csw.writers.flush()
}

func (csw *CSVWriter) upsertStorageCID(storageCID models.StorageNodeModel) {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}
	csw.writers.write(&types.TableStorageNode, storageCID.BlockNumber, storageCID.HeaderID, storageCID.StatePath, storageKey, storageCID.CID,
		storageCID.Path, storageCID.NodeType, true, storageCID.MhKey)
	csw.writers.flush()
}
