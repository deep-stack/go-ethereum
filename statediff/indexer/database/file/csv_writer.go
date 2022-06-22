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

type tableRow struct {
	table  types.Table
	values []interface{}
}

type CSVWriter struct {
	dir     string // dir containing output files
	writers fileWriters

	rows          chan tableRow
	flushChan     chan struct{}
	flushFinished chan struct{}
	quitChan      chan struct{}
	doneChan      chan struct{}
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
		writers:       writers,
		dir:           path,
		rows:          make(chan tableRow),
		flushChan:     make(chan struct{}),
		flushFinished: make(chan struct{}),
		quitChan:      make(chan struct{}),
		doneChan:      make(chan struct{}),
	}
	return csvWriter, nil
}

func (csw *CSVWriter) Loop() {
	go func() {
		defer close(csw.doneChan)
		for {
			select {
			case row := <-csw.rows:
				// TODO: Check available buffer size and flush
				csw.writers.flush()

				csw.writers.write(&row.table, row.values...)
			case <-csw.quitChan:
				if err := csw.writers.flush(); err != nil {
					panic(fmt.Sprintf("error writing csv buffer to file: %v", err))
				}
				return
			case <-csw.flushChan:
				if err := csw.writers.flush(); err != nil {
					panic(fmt.Sprintf("error writing csv buffer to file: %v", err))
				}
				csw.flushFinished <- struct{}{}
			}
		}
	}()
}

// Flush sends a flush signal to the looping process
func (csw *CSVWriter) Flush() {
	csw.flushChan <- struct{}{}
	<-csw.flushFinished
}

func TableFile(dir, name string) string { return filepath.Join(dir, name+".csv") }

// Close satisfies io.Closer
func (csw *CSVWriter) Close() error {
	close(csw.quitChan)
	<-csw.doneChan
	return nil
}

func (csw *CSVWriter) upsertNode(node nodeinfo.Info) {
	var values []interface{}
	values = append(values, node.GenesisBlock, node.NetworkID, node.ID, node.ClientName, node.ChainID)
	csw.rows <- tableRow{types.TableNodeInfo, values}
}

func (csw *CSVWriter) upsertIPLD(ipld models.IPLDModel) {
	var values []interface{}
	values = append(values, ipld.BlockNumber, ipld.Key, ipld.Data)
	csw.rows <- tableRow{types.TableIPLDBlock, values}
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
	var values []interface{}
	values = append(values, header.BlockNumber, header.BlockHash, header.ParentHash, header.CID,
		header.TotalDifficulty, header.NodeID, header.Reward, header.StateRoot, header.TxRoot,
		header.RctRoot, header.UncleRoot, header.Bloom, strconv.FormatUint(header.Timestamp, 10), header.MhKey, 1, header.Coinbase)
	csw.rows <- tableRow{types.TableHeader, values}
	indexerMetrics.blocks.Inc(1)
}

func (csw *CSVWriter) upsertUncleCID(uncle models.UncleModel) {
	var values []interface{}
	values = append(values, uncle.BlockNumber, uncle.BlockHash, uncle.HeaderID, uncle.ParentHash, uncle.CID,
		uncle.Reward, uncle.MhKey)
	csw.rows <- tableRow{types.TableUncle, values}
}

func (csw *CSVWriter) upsertTransactionCID(transaction models.TxModel) {
	var values []interface{}
	values = append(values, transaction.BlockNumber, transaction.HeaderID, transaction.TxHash, transaction.CID, transaction.Dst,
		transaction.Src, transaction.Index, transaction.MhKey, transaction.Data, transaction.Type, transaction.Value)
	csw.rows <- tableRow{types.TableTransaction, values}
	indexerMetrics.transactions.Inc(1)
}

func (csw *CSVWriter) upsertAccessListElement(accessListElement models.AccessListElementModel) {
	var values []interface{}
	values = append(values, accessListElement.BlockNumber, accessListElement.TxID, accessListElement.Index, accessListElement.Address, accessListElement.StorageKeys)
	csw.rows <- tableRow{types.TableAccessListElement, values}
	indexerMetrics.accessListEntries.Inc(1)
}

func (csw *CSVWriter) upsertReceiptCID(rct *models.ReceiptModel) {
	var values []interface{}
	values = append(values, rct.BlockNumber, rct.TxID, rct.LeafCID, rct.Contract, rct.ContractHash, rct.LeafMhKey,
		rct.PostState, rct.PostStatus, rct.LogRoot)
	csw.rows <- tableRow{types.TableReceipt, values}
	indexerMetrics.receipts.Inc(1)
}

func (csw *CSVWriter) upsertLogCID(logs []*models.LogsModel) {
	for _, l := range logs {
		var values []interface{}
		values = append(values, l.BlockNumber, l.LeafCID, l.LeafMhKey, l.ReceiptID, l.Address, l.Index, l.Topic0,
			l.Topic1, l.Topic2, l.Topic3, l.Data)
		csw.rows <- tableRow{types.TableLog, values}
		indexerMetrics.logs.Inc(1)
	}
}

func (csw *CSVWriter) upsertStateCID(stateNode models.StateNodeModel) {
	var stateKey string
	if stateNode.StateKey != nullHash.String() {
		stateKey = stateNode.StateKey
	}

	var values []interface{}
	values = append(values, stateNode.BlockNumber, stateNode.HeaderID, stateKey, stateNode.CID, stateNode.Path,
		stateNode.NodeType, true, stateNode.MhKey)
	csw.rows <- tableRow{types.TableStateNode, values}
}

func (csw *CSVWriter) upsertStateAccount(stateAccount models.StateAccountModel) {
	var values []interface{}
	values = append(values, stateAccount.BlockNumber, stateAccount.HeaderID, stateAccount.StatePath, stateAccount.Balance,
		strconv.FormatUint(stateAccount.Nonce, 10), stateAccount.CodeHash, stateAccount.StorageRoot)
	csw.rows <- tableRow{types.TableStateAccount, values}
}

func (csw *CSVWriter) upsertStorageCID(storageCID models.StorageNodeModel) {
	var storageKey string
	if storageCID.StorageKey != nullHash.String() {
		storageKey = storageCID.StorageKey
	}

	var values []interface{}
	values = append(values, storageCID.BlockNumber, storageCID.HeaderID, storageCID.StatePath, storageKey, storageCID.CID,
		storageCID.Path, storageCID.NodeType, true, storageCID.MhKey)
	csw.rows <- tableRow{types.TableStorageNode, values}
}
