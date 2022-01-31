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

// Package sql provides an interface for pushing and indexing IPLD objects into a sql database
// Metrics for reporting processing and connection stats are defined in ./metrics.go

package sql

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	node "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multihash"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
	v2Writer "github.com/ethereum/go-ethereum/statediff/indexer/database/sql/v2"
	v3Writer "github.com/ethereum/go-ethereum/statediff/indexer/database/sql/v3"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
	"github.com/ethereum/go-ethereum/statediff/indexer/ipld"
	sharedModels "github.com/ethereum/go-ethereum/statediff/indexer/models/shared"
	v2Models "github.com/ethereum/go-ethereum/statediff/indexer/models/v2"
	v3Models "github.com/ethereum/go-ethereum/statediff/indexer/models/v3"
	nodeInfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/indexer/shared"
	sdtypes "github.com/ethereum/go-ethereum/statediff/types"
)

var _ interfaces.StateDiffIndexer = &StateDiffIndexer{}

var (
	indexerMetrics = RegisterIndexerMetrics(metrics.DefaultRegistry)
	dbMetrics      = RegisterDBMetrics(metrics.DefaultRegistry)
)

// StateDiffIndexer satisfies the indexer.StateDiffIndexer interface for ethereum statediff objects on top of an SQL sql
type StateDiffIndexer struct {
	ctx         context.Context
	chainConfig *params.ChainConfig
	oldDBWriter *v2Writer.Writer
	newDBWriter *v3Writer.Writer
}

// NewStateDiffIndexer creates a sql implementation of interfaces.StateDiffIndexer
func NewStateDiffIndexer(ctx context.Context, chainConfig *params.ChainConfig, info nodeInfo.Info, old, new interfaces.Database) (*StateDiffIndexer, error) {
	// Write the removed node to the db on init
	if _, err := old.Exec(ctx, old.InsertIPLDStm(), shared.RemovedNodeMhKey, []byte{}); err != nil {
		return nil, err
	}
	if _, err := new.Exec(ctx, new.InsertIPLDStm(), shared.RemovedNodeMhKey, []byte{}); err != nil {
		return nil, err
	}
	// Write node info to the db on init
	oldWriter := v2Writer.NewWriter(old)
	newWriter := v3Writer.NewWriter(new)
	if err := oldWriter.InsertNodeInfo(info); err != nil {
		return nil, err
	}
	if err := newWriter.InsertNodeInfo(info); err != nil {
		return nil, err
	}
	return &StateDiffIndexer{
		ctx:         ctx,
		chainConfig: chainConfig,
		oldDBWriter: oldWriter,
		newDBWriter: newWriter,
	}, nil
}

// ReportOldDBMetrics is a reporting function to run as goroutine
func (sdi *StateDiffIndexer) ReportOldDBMetrics(delay time.Duration, quit <-chan bool) {
	if !metrics.Enabled {
		return
	}
	ticker := time.NewTicker(delay)
	go func() {
		for {
			select {
			case <-ticker.C:
				dbMetrics.Update(sdi.oldDBWriter.Stats())
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

// ReportNewDBMetrics is a reporting function to run as goroutine
func (sdi *StateDiffIndexer) ReportNewDBMetrics(delay time.Duration, quit <-chan bool) {
	if !metrics.Enabled {
		return
	}
	ticker := time.NewTicker(delay)
	go func() {
		for {
			select {
			case <-ticker.C:
				dbMetrics.Update(sdi.newDBWriter.DB.Stats())
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

// PushBlock pushes and indexes block data in sql, except state & storage nodes (includes header, uncles, transactions & receipts)
// Returns an initiated DB transaction which must be Closed via defer to commit or rollback
func (sdi *StateDiffIndexer) PushBlock(block *types.Block, receipts types.Receipts, totalDifficulty *big.Int) (interfaces.Batch, int64, error) {
	start, t := time.Now(), time.Now()
	blockHash := block.Hash()
	blockHashStr := blockHash.String()
	height := block.NumberU64()
	traceMsg := fmt.Sprintf("indexer stats for statediff at %d with hash %s:\r\n", height, blockHashStr)
	transactions := block.Transactions()
	// Derive any missing fields
	if err := receipts.DeriveFields(sdi.chainConfig, blockHash, height, transactions); err != nil {
		return nil, 0, err
	}

	// Generate the block iplds
	headerNode, uncleNodes, txNodes, txTrieNodes, rctNodes, rctTrieNodes, logTrieNodes, logLeafNodeCIDs, rctLeafNodeCIDs, err := ipld.FromBlockAndReceipts(block, receipts)
	if err != nil {
		return nil, 0, fmt.Errorf("error creating IPLD nodes from block and receipts: %v", err)
	}

	if len(txNodes) != len(rctNodes) || len(rctNodes) != len(rctLeafNodeCIDs) {
		return nil, 0, fmt.Errorf("expected number of transactions (%d), receipts (%d), and receipt trie leaf nodes (%d) to be equal", len(txNodes), len(rctNodes), len(rctLeafNodeCIDs))
	}
	if len(txTrieNodes) != len(rctTrieNodes) {
		return nil, 0, fmt.Errorf("expected number of tx trie (%d) and rct trie (%d) nodes to be equal", len(txTrieNodes), len(rctTrieNodes))
	}

	// Calculate reward
	var reward *big.Int
	// in PoA networks block reward is 0
	if sdi.chainConfig.Clique != nil {
		reward = big.NewInt(0)
	} else {
		reward = shared.CalcEthBlockReward(block.Header(), block.Uncles(), block.Transactions(), receipts)
	}
	t = time.Now()

	// Begin new db tx for everything
	oldTx, err := sdi.oldDBWriter.DB.Begin(sdi.ctx)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if p := recover(); p != nil {
			rollback(sdi.ctx, oldTx)
			panic(p)
		} else if err != nil {
			rollback(sdi.ctx, oldTx)
		}
	}()
	newTx, err := sdi.newDBWriter.DB.Begin(sdi.ctx)
	if err != nil {
		return nil, 0, err
	}
	defer func() {
		if p := recover(); p != nil {
			rollback(sdi.ctx, newTx)
			panic(p)
		} else if err != nil {
			rollback(sdi.ctx, newTx)
		}
	}()
	blockTx := &BatchTx{
		ctx:         sdi.ctx,
		BlockNumber: height,
		oldStmt:     sdi.oldDBWriter.DB.InsertIPLDsStm(),
		newStmt:     sdi.newDBWriter.DB.InsertStateStm(),
		iplds:       make(chan sharedModels.IPLDModel),
		quit:        make(chan struct{}),
		ipldCache:   sharedModels.IPLDBatch{},
		oldDBTx:     oldTx,
		newDBTx:     newTx,
		// handle transaction commit or rollback for any return case
		submit: func(self *BatchTx, err error) error {
			defer func() {
				close(self.quit)
				close(self.iplds)
			}()
			if p := recover(); p != nil {
				rollback(sdi.ctx, oldTx)
				rollback(sdi.ctx, newTx)
				panic(p)
			} else if err != nil {
				rollback(sdi.ctx, oldTx)
				rollback(sdi.ctx, newTx)
			} else {
				tDiff := time.Since(t)
				indexerMetrics.TimeStateStoreCodeProcessing.Update(tDiff)
				traceMsg += fmt.Sprintf("state, storage, and code storage processing time: %s\r\n", tDiff.String())
				t = time.Now()
				if err := self.flush(); err != nil {
					rollback(sdi.ctx, oldTx)
					rollback(sdi.ctx, newTx)
					traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
					log.Debug(traceMsg)
					return err
				}
				errs := make([]string, 0, 2)
				err = oldTx.Commit(sdi.ctx)
				if err != nil {
					errs = append(errs, fmt.Sprintf("old DB tx commit error: %s", err.Error()))
				}
				err = newTx.Commit(sdi.ctx)
				if err != nil {
					errs = append(errs, fmt.Sprintf("new DB tx commit error: %s", err.Error()))
				}
				if len(errs) > 0 {
					err = errors.New(strings.Join(errs, " && "))
				}
				tDiff = time.Since(t)
				indexerMetrics.TimePostgresCommit.Update(tDiff)
				traceMsg += fmt.Sprintf("postgres transaction commit duration: %s\r\n", tDiff.String())
			}
			traceMsg += fmt.Sprintf(" TOTAL PROCESSING DURATION: %s\r\n", time.Since(start).String())
			log.Debug(traceMsg)
			return err
		},
	}
	go blockTx.cache()

	tDiff := time.Since(t)
	indexerMetrics.TimeFreePostgres.Update(tDiff)

	traceMsg += fmt.Sprintf("time spent waiting for free postgres tx: %s:\r\n", tDiff.String())
	t = time.Now()

	// Publish and index header, collect headerID
	var headerID int64
	headerID, err = sdi.processHeader(blockTx, block.Header(), headerNode, reward, totalDifficulty)
	if err != nil {
		return nil, 0, err
	}
	tDiff = time.Since(t)
	indexerMetrics.TimeHeaderProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("header processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index uncles
	err = sdi.processUncles(blockTx, blockHashStr, headerID, height, uncleNodes)
	if err != nil {
		return nil, 0, err
	}
	tDiff = time.Since(t)
	indexerMetrics.TimeUncleProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("uncle processing time: %s\r\n", tDiff.String())
	t = time.Now()
	// Publish and index receipts and txs
	err = sdi.processReceiptsAndTxs(blockTx, processArgs{
		headerHash:      blockHashStr,
		headerID:        headerID,
		blockNumber:     block.Number(),
		receipts:        receipts,
		txs:             transactions,
		rctNodes:        rctNodes,
		rctTrieNodes:    rctTrieNodes,
		txNodes:         txNodes,
		txTrieNodes:     txTrieNodes,
		logTrieNodes:    logTrieNodes,
		logLeafNodeCIDs: logLeafNodeCIDs,
		rctLeafNodeCIDs: rctLeafNodeCIDs,
	})
	if err != nil {
		return nil, 0, err
	}
	tDiff = time.Since(t)
	indexerMetrics.TimeTxAndRecProcessing.Update(tDiff)
	traceMsg += fmt.Sprintf("tx and receipt processing time: %s\r\n", tDiff.String())
	t = time.Now()

	return blockTx, headerID, err
}

// processHeader publishes and indexes a header IPLD in Postgres
// it returns the headerID
func (sdi *StateDiffIndexer) processHeader(tx *BatchTx, header *types.Header, headerNode node.Node, reward, td *big.Int) (int64, error) {
	tx.cacheIPLD(headerNode)

	var baseFee *string
	if header.BaseFee != nil {
		baseFee = new(string)
		*baseFee = header.BaseFee.String()
	}
	// index header
	headerID, err := sdi.oldDBWriter.InsertHeaderCID(tx.oldDBTx, &v2Models.HeaderModel{
		CID:             headerNode.Cid().String(),
		MhKey:           shared.MultihashKeyFromCID(headerNode.Cid()),
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     header.Number.String(),
		BlockHash:       header.Hash().String(),
		TotalDifficulty: td.String(),
		Reward:          reward.String(),
		Bloom:           header.Bloom.Bytes(),
		StateRoot:       header.Root.String(),
		RctRoot:         header.ReceiptHash.String(),
		TxRoot:          header.TxHash.String(),
		UncleRoot:       header.UncleHash.String(),
		Timestamp:       header.Time,
		BaseFee:         baseFee,
	})
	if err != nil {
		return 0, err
	}
	if err := sdi.newDBWriter.InsertHeaderCID(tx.newDBTx, v3Models.HeaderModel{
		CID:             headerNode.Cid().String(),
		MhKey:           shared.MultihashKeyFromCID(headerNode.Cid()),
		ParentHash:      header.ParentHash.String(),
		BlockNumber:     header.Number.String(),
		BlockHash:       header.Hash().String(),
		TotalDifficulty: td.String(),
		Reward:          reward.String(),
		Bloom:           header.Bloom.Bytes(),
		StateRoot:       header.Root.String(),
		RctRoot:         header.ReceiptHash.String(),
		TxRoot:          header.TxHash.String(),
		UncleRoot:       header.UncleHash.String(),
		Timestamp:       header.Time,
		Coinbase:        header.Coinbase.String(),
	}); err != nil {
		return 0, err
	}
	return headerID, nil
}

// processUncles publishes and indexes uncle IPLDs in Postgres
func (sdi *StateDiffIndexer) processUncles(tx *BatchTx, headerHash string, headerID int64, blockNumber uint64, uncleNodes []*ipld.EthHeader) error {
	// publish and index uncles
	for _, uncleNode := range uncleNodes {
		tx.cacheIPLD(uncleNode)
		var uncleReward *big.Int
		// in PoA networks uncle reward is 0
		if sdi.chainConfig.Clique != nil {
			uncleReward = big.NewInt(0)
		} else {
			uncleReward = shared.CalcUncleMinerReward(blockNumber, uncleNode.Number.Uint64())
		}
		if err := sdi.oldDBWriter.InsertUncleCID(tx.oldDBTx, &v2Models.UncleModel{
			HeaderID:   headerID,
			CID:        uncleNode.Cid().String(),
			MhKey:      shared.MultihashKeyFromCID(uncleNode.Cid()),
			ParentHash: uncleNode.ParentHash.String(),
			BlockHash:  uncleNode.Hash().String(),
			Reward:     uncleReward.String(),
		}); err != nil {
			return err
		}
		if err := sdi.newDBWriter.InsertUncleCID(tx.newDBTx, &v3Models.UncleModel{
			HeaderID:   headerHash,
			CID:        uncleNode.Cid().String(),
			MhKey:      shared.MultihashKeyFromCID(uncleNode.Cid()),
			ParentHash: uncleNode.ParentHash.String(),
			BlockHash:  uncleNode.Hash().String(),
			Reward:     uncleReward.String(),
		}); err != nil {
			return err
		}
	}
	return nil
}

// processArgs bundles arguments to processReceiptsAndTxs
type processArgs struct {
	headerID        int64
	headerHash      string
	blockNumber     *big.Int
	receipts        types.Receipts
	txs             types.Transactions
	rctNodes        []*ipld.EthReceipt
	rctTrieNodes    []*ipld.EthRctTrie
	txNodes         []*ipld.EthTx
	txTrieNodes     []*ipld.EthTxTrie
	logTrieNodes    [][]node.Node
	logLeafNodeCIDs [][]cid.Cid
	rctLeafNodeCIDs []cid.Cid
}

// processReceiptsAndTxs publishes and indexes receipt and transaction IPLDs in Postgres
func (sdi *StateDiffIndexer) processReceiptsAndTxs(tx *BatchTx, args processArgs) error {
	// Process receipts and txs
	signer := types.MakeSigner(sdi.chainConfig, args.blockNumber)
	for i, receipt := range args.receipts {
		for _, logTrieNode := range args.logTrieNodes[i] {
			tx.cacheIPLD(logTrieNode)
		}
		txNode := args.txNodes[i]
		tx.cacheIPLD(txNode)

		// index tx
		trx := args.txs[i]
		txHash := trx.Hash().String()

		var val string
		if trx.Value() != nil {
			val = trx.Value().String()
		}

		// derive sender for the tx that corresponds with this receipt
		from, err := types.Sender(signer, trx)
		if err != nil {
			return fmt.Errorf("error deriving tx sender: %v", err)
		}
		txID, err := sdi.oldDBWriter.InsertTransactionCID(tx.oldDBTx, &v2Models.TxModel{
			HeaderID: args.headerID,
			Dst:      shared.HandleZeroAddrPointer(trx.To()),
			Src:      shared.HandleZeroAddr(from),
			TxHash:   txHash,
			Index:    int64(i),
			Data:     trx.Data(),
			CID:      txNode.Cid().String(),
			MhKey:    shared.MultihashKeyFromCID(txNode.Cid()),
			Type:     trx.Type(),
		})
		if err != nil {
			return err
		}
		if err := sdi.newDBWriter.InsertTransactionCID(tx.newDBTx, &v3Models.TxModel{
			HeaderID: args.headerHash,
			Dst:      shared.HandleZeroAddrPointer(trx.To()),
			Src:      shared.HandleZeroAddr(from),
			TxHash:   txHash,
			Index:    int64(i),
			Data:     trx.Data(),
			CID:      txNode.Cid().String(),
			MhKey:    shared.MultihashKeyFromCID(txNode.Cid()),
			Type:     trx.Type(),
			Value:    val,
		}); err != nil {
			return err
		}
		// index access list if this is one
		for j, accessListElement := range trx.AccessList() {
			storageKeys := make([]string, len(accessListElement.StorageKeys))
			for k, storageKey := range accessListElement.StorageKeys {
				storageKeys[k] = storageKey.Hex()
			}
			if err := sdi.oldDBWriter.InsertAccessListElement(tx.oldDBTx, &v2Models.AccessListElementModel{
				TxID:        txID,
				Index:       int64(j),
				Address:     accessListElement.Address.Hex(),
				StorageKeys: storageKeys,
			}); err != nil {
				return err
			}
			if err := sdi.newDBWriter.InsertAccessListElement(tx.newDBTx, &v3Models.AccessListElementModel{
				TxID:        txHash,
				Index:       int64(j),
				Address:     accessListElement.Address.Hex(),
				StorageKeys: storageKeys,
			}); err != nil {
				return err
			}
		}

		// this is the contract address if this receipt is for a contract creation tx
		contract := shared.HandleZeroAddr(receipt.ContractAddress)
		var contractHash string
		if contract != "" {
			contractHash = crypto.Keccak256Hash(common.HexToAddress(contract).Bytes()).String()
		}

		// index receipt
		if !args.rctLeafNodeCIDs[i].Defined() {
			return fmt.Errorf("invalid receipt leaf node cid")
		}

		var postState string
		var postStatus uint64
		if len(receipt.PostState) == 0 {
			postStatus = receipt.Status
		} else {
			postState = common.Bytes2Hex(receipt.PostState)
		}

		rctID, err := sdi.oldDBWriter.InsertReceiptCID(tx.oldDBTx, &v2Models.ReceiptModel{
			TxID:         txID,
			Contract:     contract,
			ContractHash: contractHash,
			LeafCID:      args.rctLeafNodeCIDs[i].String(),
			LeafMhKey:    shared.MultihashKeyFromCID(args.rctLeafNodeCIDs[i]),
			LogRoot:      args.rctNodes[i].LogRoot.String(),
			PostState:    postState,
			PostStatus:   postStatus,
		})
		if err != nil {
			return err
		}
		if err := sdi.newDBWriter.InsertReceiptCID(tx.newDBTx, &v3Models.ReceiptModel{
			TxID:         txHash,
			Contract:     contract,
			ContractHash: contractHash,
			LeafCID:      args.rctLeafNodeCIDs[i].String(),
			LeafMhKey:    shared.MultihashKeyFromCID(args.rctLeafNodeCIDs[i]),
			LogRoot:      args.rctNodes[i].LogRoot.String(),
			PostState:    postState,
			PostStatus:   postStatus,
		}); err != nil {
			return err
		}

		// index logs
		rctLen := len(receipt.Logs)
		oldLogDataSet := make([]*v2Models.LogsModel, rctLen)
		newLogDataSet := make([]*v3Models.LogsModel, rctLen)
		for idx, l := range receipt.Logs {
			topicSet := make([]string, 4)
			for ti, topic := range l.Topics {
				topicSet[ti] = topic.Hex()
			}

			if !args.logLeafNodeCIDs[i][idx].Defined() {
				return fmt.Errorf("invalid log cid")
			}

			oldLogDataSet[idx] = &v2Models.LogsModel{
				ReceiptID: rctID,
				Address:   l.Address.String(),
				Index:     int64(l.Index),
				Data:      l.Data,
				LeafCID:   args.logLeafNodeCIDs[i][idx].String(),
				LeafMhKey: shared.MultihashKeyFromCID(args.logLeafNodeCIDs[i][idx]),
				Topic0:    topicSet[0],
				Topic1:    topicSet[1],
				Topic2:    topicSet[2],
				Topic3:    topicSet[3],
			}
			newLogDataSet[idx] = &v3Models.LogsModel{
				ReceiptID: txHash,
				Address:   l.Address.String(),
				Index:     int64(l.Index),
				Data:      l.Data,
				LeafCID:   args.logLeafNodeCIDs[i][idx].String(),
				LeafMhKey: shared.MultihashKeyFromCID(args.logLeafNodeCIDs[i][idx]),
				Topic0:    topicSet[0],
				Topic1:    topicSet[1],
				Topic2:    topicSet[2],
				Topic3:    topicSet[3],
			}
		}
		if err := sdi.oldDBWriter.InsertLogCID(tx.oldDBTx, oldLogDataSet); err != nil {
			return err
		}
		if err := sdi.newDBWriter.InsertLogCID(tx.newDBTx, newLogDataSet); err != nil {
			return err
		}
	}

	// publish trie nodes, these aren't indexed directly
	for i, n := range args.txTrieNodes {
		tx.cacheIPLD(n)
		tx.cacheIPLD(args.rctTrieNodes[i])
	}

	return nil
}

// PushStateNode publishes and indexes a state diff node object (including any child storage nodes) in the IPLD sql
func (sdi *StateDiffIndexer) PushStateNode(batch interfaces.Batch, stateNode sdtypes.StateNode, headerHash string, headerID int64) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("sql batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// publish the state node
	if stateNode.NodeType == sdtypes.Removed {
		// short circuit if it is a Removed node
		// this assumes the db has been initialized and a public.blocks entry for the Removed node is present
		_, err := sdi.oldDBWriter.InsertStateCID(tx.oldDBTx, &v2Models.StateNodeModel{
			HeaderID: headerID,
			Path:     stateNode.Path,
			StateKey: common.BytesToHash(stateNode.LeafKey).String(),
			CID:      shared.RemovedNodeStateCID,
			MhKey:    shared.RemovedNodeMhKey,
			NodeType: stateNode.NodeType.Int(),
		})
		if err != nil {
			return err
		}
		return sdi.newDBWriter.InsertStateCID(tx.newDBTx, &v3Models.StateNodeModel{
			HeaderID: headerHash,
			Path:     stateNode.Path,
			StateKey: common.BytesToHash(stateNode.LeafKey).String(),
			CID:      shared.RemovedNodeStateCID,
			MhKey:    shared.RemovedNodeMhKey,
			NodeType: stateNode.NodeType.Int(),
		})
	}
	stateCIDStr, stateMhKey, err := tx.cacheRaw(ipld.MEthStateTrie, multihash.KECCAK_256, stateNode.NodeValue)
	if err != nil {
		return fmt.Errorf("error generating and cacheing state node IPLD: %v", err)
	}
	// index the state node
	stateID, err := sdi.oldDBWriter.InsertStateCID(tx.oldDBTx, &v2Models.StateNodeModel{
		HeaderID: headerID,
		Path:     stateNode.Path,
		StateKey: common.BytesToHash(stateNode.LeafKey).String(),
		CID:      stateCIDStr,
		MhKey:    stateMhKey,
		NodeType: stateNode.NodeType.Int(),
	})
	if err := sdi.newDBWriter.InsertStateCID(tx.newDBTx, &v3Models.StateNodeModel{
		HeaderID: headerHash,
		Path:     stateNode.Path,
		StateKey: common.BytesToHash(stateNode.LeafKey).String(),
		CID:      stateCIDStr,
		MhKey:    stateMhKey,
		NodeType: stateNode.NodeType.Int(),
	}); err != nil {
		return err
	}
	// if we have a leaf, decode and index the account data
	if stateNode.NodeType == sdtypes.Leaf {
		var i []interface{}
		if err := rlp.DecodeBytes(stateNode.NodeValue, &i); err != nil {
			return fmt.Errorf("error decoding state leaf node rlp: %s", err.Error())
		}
		if len(i) != 2 {
			return fmt.Errorf("eth IPLDPublisher expected state leaf node rlp to decode into two elements")
		}
		var account types.StateAccount
		if err := rlp.DecodeBytes(i[1].([]byte), &account); err != nil {
			return fmt.Errorf("error decoding state account rlp: %s", err.Error())
		}
		if err := sdi.oldDBWriter.InsertStateAccount(tx.oldDBTx, &v2Models.StateAccountModel{
			StateID:     stateID,
			Balance:     account.Balance.String(),
			Nonce:       account.Nonce,
			CodeHash:    account.CodeHash,
			StorageRoot: account.Root.String(),
		}); err != nil {
			return err
		}
		if err := sdi.newDBWriter.InsertStateAccount(tx.newDBTx, &v3Models.StateAccountModel{
			HeaderID:    headerHash,
			StatePath:   stateNode.Path,
			Balance:     account.Balance.String(),
			Nonce:       account.Nonce,
			CodeHash:    account.CodeHash,
			StorageRoot: account.Root.String(),
		}); err != nil {
			return err
		}
	}
	// if there are any storage nodes associated with this node, publish and index them
	for _, storageNode := range stateNode.StorageNodes {
		if storageNode.NodeType == sdtypes.Removed {
			// short circuit if it is a Removed node
			// this assumes the db has been initialized and a public.blocks entry for the Removed node is present
			if err := sdi.oldDBWriter.InsertStorageCID(tx.oldDBTx, &v2Models.StorageNodeModel{
				StateID:    stateID,
				Path:       storageNode.Path,
				StorageKey: common.BytesToHash(storageNode.LeafKey).String(),
				CID:        shared.RemovedNodeStorageCID,
				MhKey:      shared.RemovedNodeMhKey,
				NodeType:   storageNode.NodeType.Int(),
			}); err != nil {
				return err
			}
			if err := sdi.newDBWriter.InsertStorageCID(tx.newDBTx, &v3Models.StorageNodeModel{
				HeaderID:   headerHash,
				StatePath:  stateNode.Path,
				Path:       storageNode.Path,
				StorageKey: common.BytesToHash(storageNode.LeafKey).String(),
				CID:        shared.RemovedNodeStorageCID,
				MhKey:      shared.RemovedNodeMhKey,
				NodeType:   storageNode.NodeType.Int(),
			}); err != nil {
				return err
			}
			continue
		}
		storageCIDStr, storageMhKey, err := tx.cacheRaw(ipld.MEthStorageTrie, multihash.KECCAK_256, storageNode.NodeValue)
		if err != nil {
			return fmt.Errorf("error generating and cacheing storage node IPLD: %v", err)
		}
		if err := sdi.oldDBWriter.InsertStorageCID(tx.oldDBTx, &v2Models.StorageNodeModel{
			StateID:    stateID,
			Path:       storageNode.Path,
			StorageKey: common.BytesToHash(storageNode.LeafKey).String(),
			CID:        storageCIDStr,
			MhKey:      storageMhKey,
			NodeType:   storageNode.NodeType.Int(),
		}); err != nil {
			return err
		}
		if err := sdi.newDBWriter.InsertStorageCID(tx.newDBTx, &v3Models.StorageNodeModel{
			HeaderID:   headerHash,
			StatePath:  stateNode.Path,
			Path:       storageNode.Path,
			StorageKey: common.BytesToHash(storageNode.LeafKey).String(),
			CID:        storageCIDStr,
			MhKey:      storageMhKey,
			NodeType:   storageNode.NodeType.Int(),
		}); err != nil {
			return err
		}
	}

	return nil
}

// PushCodeAndCodeHash publishes code and codehash pairs to the ipld sql
func (sdi *StateDiffIndexer) PushCodeAndCodeHash(batch interfaces.Batch, codeAndCodeHash sdtypes.CodeAndCodeHash) error {
	tx, ok := batch.(*BatchTx)
	if !ok {
		return fmt.Errorf("sql batch is expected to be of type %T, got %T", &BatchTx{}, batch)
	}
	// codec doesn't matter since db key is multihash-based
	mhKey, err := shared.MultihashKeyFromKeccak256(codeAndCodeHash.Hash)
	if err != nil {
		return fmt.Errorf("error deriving multihash key from codehash: %v", err)
	}
	tx.cacheDirect(mhKey, codeAndCodeHash.Code)
	return nil
}

// Close satisfies io.Closer
func (sdi *StateDiffIndexer) Close() error {
	if err := sdi.oldDBWriter.Close(); err != nil {
		return err
	}
	return sdi.newDBWriter.Close()
}
