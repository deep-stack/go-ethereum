// Copyright 2019 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package statediff

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/eth"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/p2p"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/trie"

	ind "github.com/ethereum/go-ethereum/statediff/indexer"
	nodeinfo "github.com/ethereum/go-ethereum/statediff/indexer/node"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"
	. "github.com/ethereum/go-ethereum/statediff/types"
)

const (
	chainEventChanSize = 20000
	genesisBlockNumber = 0
	defaultRetryLimit  = 3                   // default retry limit once deadlock is detected.
	deadlockDetected   = "deadlock detected" // 40P01 https://www.postgresql.org/docs/current/errcodes-appendix.html
)

// TODO: Take the watched addresses file path as a CLI arg.
const watchedAddressesFile = "./watched-addresses.json"

var writeLoopParams = Params{
	IntermediateStateNodes:   true,
	IntermediateStorageNodes: true,
	IncludeBlock:             true,
	IncludeReceipts:          true,
	IncludeTD:                true,
	IncludeCode:              true,
}

var statediffMetrics = RegisterStatediffMetrics(metrics.DefaultRegistry)

type blockChain interface {
	SubscribeChainEvent(ch chan<- core.ChainEvent) event.Subscription
	GetBlockByHash(hash common.Hash) *types.Block
	GetBlockByNumber(number uint64) *types.Block
	GetReceiptsByHash(hash common.Hash) types.Receipts
	GetTd(hash common.Hash, number uint64) *big.Int
	UnlockTrie(root common.Hash)
	StateCache() state.Database
}

// IService is the state-diffing service interface
type IService interface {
	// Start() and Stop()
	node.Lifecycle
	// Method to getting API(s) for this service
	APIs() []rpc.API
	// Main event loop for processing state diffs
	Loop(chainEventCh chan core.ChainEvent)
	// Method to subscribe to receive state diff processing output
	Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool, params Params)
	// Method to unsubscribe from state diff processing
	Unsubscribe(id rpc.ID) error
	// Method to get state diff object at specific block
	StateDiffAt(blockNumber uint64, params Params) (*Payload, error)
	// Method to get state diff object at specific block
	StateDiffFor(blockHash common.Hash, params Params) (*Payload, error)
	// Method to get state trie object at specific block
	StateTrieAt(blockNumber uint64, params Params) (*Payload, error)
	// Method to stream out all code and codehash pairs
	StreamCodeAndCodeHash(blockNumber uint64, outChan chan<- CodeAndCodeHash, quitChan chan<- bool)
	// Method to write state diff object directly to DB
	WriteStateDiffAt(blockNumber uint64, params Params) error
	// Method to write state diff object directly to DB
	WriteStateDiffFor(blockHash common.Hash, params Params) error
	// Event loop for progressively processing and writing diffs directly to DB
	WriteLoop(chainEventCh chan core.ChainEvent)
	// Method to add an address to be watched to write loop params
	WatchAddress(address common.Address) error
	// Method to get currently watched addresses from write loop params
	GetWathchedAddresses() []common.Address
}

// Wraps consructor parameters
type ServiceParams struct {
	DBParams *DBParams
	// Whether to enable writing state diffs directly to track blochain head
	EnableWriteLoop bool
	// Size of the worker pool
	NumWorkers uint
}

// Service is the underlying struct for the state diffing service
type Service struct {
	// Used to sync access to the Subscriptions
	sync.Mutex
	// Used to build the state diff objects
	Builder Builder
	// Used to subscribe to chain events (blocks)
	BlockChain blockChain
	// Used to signal shutdown of the service
	QuitChan chan bool
	// A mapping of rpc.IDs to their subscription channels, mapped to their subscription type (hash of the Params rlp)
	Subscriptions map[common.Hash]map[rpc.ID]Subscription
	// A mapping of subscription params rlp hash to the corresponding subscription params
	SubscriptionTypes map[common.Hash]Params
	// Cache the last block so that we can avoid having to lookup the next block's parent
	BlockCache blockCache
	// Whether or not we have any subscribers; only if we do, do we processes state diffs
	subscribers int32
	// Interface for publishing statediffs as PG-IPLD objects
	indexer ind.Indexer
	// Whether to enable writing state diffs directly to track blochain head
	enableWriteLoop bool
	// Size of the worker pool
	numWorkers uint
	// Number of retry for aborted transactions due to deadlock.
	maxRetry uint
}

// Wrap the cached last block for safe access from different service loops
type blockCache struct {
	sync.Mutex
	blocks  map[common.Hash]*types.Block
	maxSize uint
}

func NewBlockCache(max uint) blockCache {
	return blockCache{
		blocks:  make(map[common.Hash]*types.Block),
		maxSize: max,
	}
}

// New creates a new statediff.Service
// func New(stack *node.Node, ethServ *eth.Ethereum, dbParams *DBParams, enableWriteLoop bool) error {
func New(stack *node.Node, ethServ *eth.Ethereum, cfg *ethconfig.Config, params ServiceParams) error {
	blockChain := ethServ.BlockChain()
	var indexer ind.Indexer
	quitCh := make(chan bool)
	if params.DBParams != nil {
		info := nodeinfo.Info{
			GenesisBlock: blockChain.Genesis().Hash().Hex(),
			NetworkID:    strconv.FormatUint(cfg.NetworkId, 10),
			ChainID:      blockChain.Config().ChainID.Uint64(),
			ID:           params.DBParams.ID,
			ClientName:   params.DBParams.ClientName,
		}

		// TODO: pass max idle, open, lifetime?
		db, err := postgres.NewDB(params.DBParams.ConnectionURL, postgres.ConnectionConfig{}, info)
		if err != nil {
			return err
		}
		indexer, err = ind.NewStateDiffIndexer(blockChain.Config(), db)
		if err != nil {
			return err
		}

		indexer.ReportDBMetrics(10*time.Second, quitCh)
	}
	workers := params.NumWorkers
	if workers == 0 {
		workers = 1
	}
	sds := &Service{
		Mutex:             sync.Mutex{},
		BlockChain:        blockChain,
		Builder:           NewBuilder(blockChain.StateCache()),
		QuitChan:          quitCh,
		Subscriptions:     make(map[common.Hash]map[rpc.ID]Subscription),
		SubscriptionTypes: make(map[common.Hash]Params),
		BlockCache:        NewBlockCache(workers),
		indexer:           indexer,
		enableWriteLoop:   params.EnableWriteLoop,
		numWorkers:        workers,
		maxRetry:          defaultRetryLimit,
	}
	stack.RegisterLifecycle(sds)
	stack.RegisterAPIs(sds.APIs())

	err := loadWatchedAddresses()
	if err != nil {
		return err
	}

	return nil
}

// Protocols exports the services p2p protocols, this service has none
func (sds *Service) Protocols() []p2p.Protocol {
	return []p2p.Protocol{}
}

// APIs returns the RPC descriptors the statediff.Service offers
func (sds *Service) APIs() []rpc.API {
	return []rpc.API{
		{
			Namespace: APIName,
			Version:   APIVersion,
			Service:   NewPublicStateDiffAPI(sds),
			Public:    true,
		},
	}
}

// Return the parent block of currentBlock, using the cached block if available;
// and cache the passed block
func (lbc *blockCache) getParentBlock(currentBlock *types.Block, bc blockChain) *types.Block {
	lbc.Lock()
	parentHash := currentBlock.ParentHash()
	var parentBlock *types.Block
	if block, ok := lbc.blocks[parentHash]; ok {
		parentBlock = block
		if len(lbc.blocks) > int(lbc.maxSize) {
			delete(lbc.blocks, parentHash)
		}
	} else {
		parentBlock = bc.GetBlockByHash(parentHash)
	}
	lbc.blocks[currentBlock.Hash()] = currentBlock
	lbc.Unlock()
	return parentBlock
}

type workerParams struct {
	chainEventCh <-chan core.ChainEvent
	errCh        <-chan error
	wg           *sync.WaitGroup
	id           uint
}

func (sds *Service) WriteLoop(chainEventCh chan core.ChainEvent) {
	chainEventSub := sds.BlockChain.SubscribeChainEvent(chainEventCh)
	defer chainEventSub.Unsubscribe()
	errCh := chainEventSub.Err()
	var wg sync.WaitGroup
	// Process metrics for chain events, then forward to workers
	chainEventFwd := make(chan core.ChainEvent, chainEventChanSize)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case chainEvent := <-chainEventCh:
				statediffMetrics.lastEventHeight.Update(int64(chainEvent.Block.Number().Uint64()))
				statediffMetrics.writeLoopChannelLen.Update(int64(len(chainEventCh)))
				chainEventFwd <- chainEvent
			case <-sds.QuitChan:
				return
			}
		}
	}()
	wg.Add(int(sds.numWorkers))
	for worker := uint(0); worker < sds.numWorkers; worker++ {
		params := workerParams{chainEventCh: chainEventFwd, errCh: errCh, wg: &wg, id: worker}
		go sds.writeLoopWorker(params)
	}
	wg.Wait()
}

func (sds *Service) writeGenesisStateDiff(currBlock *types.Block, workerId uint) {
	// For genesis block we need to return the entire state trie hence we diff it with an empty trie.
	log.Info("Writing state diff", "block height", genesisBlockNumber, "worker", workerId)
	err := sds.writeStateDiffWithRetry(currBlock, common.Hash{}, writeLoopParams)
	if err != nil {
		log.Error("statediff.Service.WriteLoop: processing error", "block height",
			genesisBlockNumber, "error", err.Error(), "worker", workerId)
		return
	}
	statediffMetrics.lastStatediffHeight.Update(genesisBlockNumber)
}

func (sds *Service) writeLoopWorker(params workerParams) {
	defer params.wg.Done()
	for {
		select {
		//Notify chain event channel of events
		case chainEvent := <-params.chainEventCh:
			log.Debug("WriteLoop(): chain event received", "event", chainEvent)
			currentBlock := chainEvent.Block
			parentBlock := sds.BlockCache.getParentBlock(currentBlock, sds.BlockChain)
			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block", "block height", currentBlock.Number())
				continue
			}

			// chainEvent streams block from block 1, but we also need to include data from the genesis block.
			if parentBlock.Number().Uint64() == genesisBlockNumber {
				sds.writeGenesisStateDiff(parentBlock, params.id)
			}

			log.Info("Writing state diff", "block height", currentBlock.Number().Uint64(), "worker", params.id)
			err := sds.writeStateDiffWithRetry(currentBlock, parentBlock.Root(), writeLoopParams)
			if err != nil {
				log.Error("statediff.Service.WriteLoop: processing error", "block height", currentBlock.Number().Uint64(), "error", err.Error(), "worker", params.id)
				continue
			}
			// TODO: how to handle with concurrent workers
			statediffMetrics.lastStatediffHeight.Update(int64(currentBlock.Number().Uint64()))
		case err := <-params.errCh:
			log.Warn("Error from chain event subscription", "error", err, "worker", params.id)
			sds.close()
			return
		case <-sds.QuitChan:
			log.Info("Quitting the statediff writing process", "worker", params.id)
			sds.close()
			return
		}
	}
}

// Loop is the main processing method
func (sds *Service) Loop(chainEventCh chan core.ChainEvent) {
	chainEventSub := sds.BlockChain.SubscribeChainEvent(chainEventCh)
	defer chainEventSub.Unsubscribe()
	errCh := chainEventSub.Err()
	for {
		select {
		//Notify chain event channel of events
		case chainEvent := <-chainEventCh:
			statediffMetrics.serviceLoopChannelLen.Update(int64(len(chainEventCh)))
			log.Debug("Loop(): chain event received", "event", chainEvent)
			// if we don't have any subscribers, do not process a statediff
			if atomic.LoadInt32(&sds.subscribers) == 0 {
				log.Debug("Currently no subscribers to the statediffing service; processing is halted")
				continue
			}
			currentBlock := chainEvent.Block
			parentBlock := sds.BlockCache.getParentBlock(currentBlock, sds.BlockChain)

			if parentBlock == nil {
				log.Error("Parent block is nil, skipping this block", "block height", currentBlock.Number())
				continue
			}

			// chainEvent streams block from block 1, but we also need to include data from the genesis block.
			if parentBlock.Number().Uint64() == genesisBlockNumber {
				// For genesis block we need to return the entire state trie hence we diff it with an empty trie.
				sds.streamStateDiff(parentBlock, common.Hash{})
			}

			sds.streamStateDiff(currentBlock, parentBlock.Root())
		case err := <-errCh:
			log.Warn("Error from chain event subscription", "error", err)
			sds.close()
			return
		case <-sds.QuitChan:
			log.Info("Quitting the statediffing process")
			sds.close()
			return
		}
	}
}

// streamStateDiff method builds the state diff payload for each subscription according to their subscription type and sends them the result
func (sds *Service) streamStateDiff(currentBlock *types.Block, parentRoot common.Hash) {
	sds.Lock()
	for ty, subs := range sds.Subscriptions {
		params, ok := sds.SubscriptionTypes[ty]
		if !ok {
			log.Error("no parameter set associated with this subscription", "subscription type", ty.Hex())
			sds.closeType(ty)
			continue
		}
		// create payload for this subscription type
		payload, err := sds.processStateDiff(currentBlock, parentRoot, params)
		if err != nil {
			log.Error("statediff processing error", "block height", currentBlock.Number().Uint64(), "parameters", params, "error", err.Error())
			continue
		}
		for id, sub := range subs {
			select {
			case sub.PayloadChan <- *payload:
				log.Debug("sending statediff payload at head", "height", currentBlock.Number(), "subscription id", id)
			default:
				log.Info("unable to send statediff payload; channel has no receiver", "subscription id", id)
			}
		}
	}
	sds.Unlock()
}

// StateDiffAt returns a state diff object payload at the specific blockheight
// This operation cannot be performed back past the point of db pruning; it requires an archival node for historical data
func (sds *Service) StateDiffAt(blockNumber uint64, params Params) (*Payload, error) {
	currentBlock := sds.BlockChain.GetBlockByNumber(blockNumber)
	log.Info("sending state diff", "block height", blockNumber)
	if blockNumber == 0 {
		return sds.processStateDiff(currentBlock, common.Hash{}, params)
	}
	parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
	return sds.processStateDiff(currentBlock, parentBlock.Root(), params)
}

// StateDiffFor returns a state diff object payload for the specific blockhash
// This operation cannot be performed back past the point of db pruning; it requires an archival node for historical data
func (sds *Service) StateDiffFor(blockHash common.Hash, params Params) (*Payload, error) {
	currentBlock := sds.BlockChain.GetBlockByHash(blockHash)
	log.Info("sending state diff", "block hash", blockHash)
	if currentBlock.NumberU64() == 0 {
		return sds.processStateDiff(currentBlock, common.Hash{}, params)
	}
	parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
	return sds.processStateDiff(currentBlock, parentBlock.Root(), params)
}

// processStateDiff method builds the state diff payload from the current block, parent state root, and provided params
func (sds *Service) processStateDiff(currentBlock *types.Block, parentRoot common.Hash, params Params) (*Payload, error) {
	stateDiff, err := sds.Builder.BuildStateDiffObject(Args{
		NewStateRoot: currentBlock.Root(),
		OldStateRoot: parentRoot,
		BlockHash:    currentBlock.Hash(),
		BlockNumber:  currentBlock.Number(),
	}, params)
	// allow dereferencing of parent, keep current locked as it should be the next parent
	sds.BlockChain.UnlockTrie(parentRoot)
	if err != nil {
		return nil, err
	}
	stateDiffRlp, err := rlp.EncodeToBytes(stateDiff)
	if err != nil {
		return nil, err
	}
	log.Info("state diff size", "at block height", currentBlock.Number().Uint64(), "rlp byte size", len(stateDiffRlp))
	return sds.newPayload(stateDiffRlp, currentBlock, params)
}

func (sds *Service) newPayload(stateObject []byte, block *types.Block, params Params) (*Payload, error) {
	payload := &Payload{
		StateObjectRlp: stateObject,
	}
	if params.IncludeBlock {
		blockBuff := new(bytes.Buffer)
		if err := block.EncodeRLP(blockBuff); err != nil {
			return nil, err
		}
		payload.BlockRlp = blockBuff.Bytes()
	}
	if params.IncludeTD {
		payload.TotalDifficulty = sds.BlockChain.GetTd(block.Hash(), block.NumberU64())
	}
	if params.IncludeReceipts {
		receiptBuff := new(bytes.Buffer)
		receipts := sds.BlockChain.GetReceiptsByHash(block.Hash())
		if err := rlp.Encode(receiptBuff, receipts); err != nil {
			return nil, err
		}
		payload.ReceiptsRlp = receiptBuff.Bytes()
	}
	return payload, nil
}

// StateTrieAt returns a state trie object payload at the specified blockheight
// This operation cannot be performed back past the point of db pruning; it requires an archival node for historical data
func (sds *Service) StateTrieAt(blockNumber uint64, params Params) (*Payload, error) {
	currentBlock := sds.BlockChain.GetBlockByNumber(blockNumber)
	log.Info("sending state trie", "block height", blockNumber)
	return sds.processStateTrie(currentBlock, params)
}

func (sds *Service) processStateTrie(block *types.Block, params Params) (*Payload, error) {
	stateNodes, err := sds.Builder.BuildStateTrieObject(block)
	if err != nil {
		return nil, err
	}
	stateTrieRlp, err := rlp.EncodeToBytes(stateNodes)
	if err != nil {
		return nil, err
	}
	log.Info("state trie size", "at block height", block.Number().Uint64(), "rlp byte size", len(stateTrieRlp))
	return sds.newPayload(stateTrieRlp, block, params)
}

// Subscribe is used by the API to subscribe to the service loop
func (sds *Service) Subscribe(id rpc.ID, sub chan<- Payload, quitChan chan<- bool, params Params) {
	log.Info("Subscribing to the statediff service")
	if atomic.CompareAndSwapInt32(&sds.subscribers, 0, 1) {
		log.Info("State diffing subscription received; beginning statediff processing")
	}
	// Subscription type is defined as the hash of the rlp-serialized subscription params
	by, err := rlp.EncodeToBytes(params)
	if err != nil {
		log.Error("State diffing params need to be rlp-serializable")
		return
	}
	subscriptionType := crypto.Keccak256Hash(by)
	// Add subscriber
	sds.Lock()
	if sds.Subscriptions[subscriptionType] == nil {
		sds.Subscriptions[subscriptionType] = make(map[rpc.ID]Subscription)
	}
	sds.Subscriptions[subscriptionType][id] = Subscription{
		PayloadChan: sub,
		QuitChan:    quitChan,
	}
	sds.SubscriptionTypes[subscriptionType] = params
	sds.Unlock()
}

// Unsubscribe is used to unsubscribe from the service loop
func (sds *Service) Unsubscribe(id rpc.ID) error {
	log.Info("Unsubscribing from the statediff service", "subscription id", id)
	sds.Lock()
	for ty := range sds.Subscriptions {
		delete(sds.Subscriptions[ty], id)
		if len(sds.Subscriptions[ty]) == 0 {
			// If we removed the last subscription of this type, remove the subscription type outright
			delete(sds.Subscriptions, ty)
			delete(sds.SubscriptionTypes, ty)
		}
	}
	if len(sds.Subscriptions) == 0 {
		if atomic.CompareAndSwapInt32(&sds.subscribers, 1, 0) {
			log.Info("No more subscriptions; halting statediff processing")
		}
	}
	sds.Unlock()
	return nil
}

// Start is used to begin the service
func (sds *Service) Start() error {
	log.Info("Starting statediff service")

	chainEventCh := make(chan core.ChainEvent, chainEventChanSize)
	go sds.Loop(chainEventCh)

	if sds.enableWriteLoop {
		log.Info("Starting statediff DB write loop", "params", writeLoopParams)
		chainEventCh := make(chan core.ChainEvent, chainEventChanSize)
		go sds.WriteLoop(chainEventCh)
	}

	return nil
}

// Stop is used to close down the service
func (sds *Service) Stop() error {
	log.Info("Stopping statediff service")
	close(sds.QuitChan)
	return nil
}

// close is used to close all listening subscriptions
func (sds *Service) close() {
	sds.Lock()
	for ty, subs := range sds.Subscriptions {
		for id, sub := range subs {
			select {
			case sub.QuitChan <- true:
				log.Info("closing subscription", "id", id)
			default:
				log.Info("unable to close subscription; channel has no receiver", "subscription id", id)
			}
			delete(sds.Subscriptions[ty], id)
		}
		delete(sds.Subscriptions, ty)
		delete(sds.SubscriptionTypes, ty)
	}
	sds.Unlock()
}

// closeType is used to close all subscriptions of given type
// closeType needs to be called with subscription access locked
func (sds *Service) closeType(subType common.Hash) {
	subs := sds.Subscriptions[subType]
	for id, sub := range subs {
		sendNonBlockingQuit(id, sub)
	}
	delete(sds.Subscriptions, subType)
	delete(sds.SubscriptionTypes, subType)
}

func sendNonBlockingQuit(id rpc.ID, sub Subscription) {
	select {
	case sub.QuitChan <- true:
		log.Info("closing subscription", "id", id)
	default:
		log.Info("unable to close subscription; channel has no receiver", "subscription id", id)
	}
}

// StreamCodeAndCodeHash subscription method for extracting all the codehash=>code mappings that exist in the trie at the provided height
func (sds *Service) StreamCodeAndCodeHash(blockNumber uint64, outChan chan<- CodeAndCodeHash, quitChan chan<- bool) {
	current := sds.BlockChain.GetBlockByNumber(blockNumber)
	log.Info("sending code and codehash", "block height", blockNumber)
	currentTrie, err := sds.BlockChain.StateCache().OpenTrie(current.Root())
	if err != nil {
		log.Error("error creating trie for block", "block height", current.Number(), "err", err)
		close(quitChan)
		return
	}
	it := currentTrie.NodeIterator([]byte{})
	leafIt := trie.NewIterator(it)
	go func() {
		defer close(quitChan)
		for leafIt.Next() {
			select {
			case <-sds.QuitChan:
				return
			default:
			}
			account := new(types.StateAccount)
			if err := rlp.DecodeBytes(leafIt.Value, account); err != nil {
				log.Error("error decoding state account", "err", err)
				return
			}
			codeHash := common.BytesToHash(account.CodeHash)
			code, err := sds.BlockChain.StateCache().ContractCode(common.Hash{}, codeHash)
			if err != nil {
				log.Error("error collecting contract code", "err", err)
				return
			}
			outChan <- CodeAndCodeHash{
				Hash: codeHash,
				Code: code,
			}
		}
	}()
}

// WriteStateDiffAt writes a state diff at the specific blockheight directly to the database
// This operation cannot be performed back past the point of db pruning; it requires an archival node
// for historical data
func (sds *Service) WriteStateDiffAt(blockNumber uint64, params Params) error {
	currentBlock := sds.BlockChain.GetBlockByNumber(blockNumber)
	parentRoot := common.Hash{}
	if blockNumber != 0 {
		parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
		parentRoot = parentBlock.Root()
	}
	return sds.writeStateDiffWithRetry(currentBlock, parentRoot, params)
}

// WriteStateDiffFor writes a state diff for the specific blockhash directly to the database
// This operation cannot be performed back past the point of db pruning; it requires an archival node
// for historical data
func (sds *Service) WriteStateDiffFor(blockHash common.Hash, params Params) error {
	currentBlock := sds.BlockChain.GetBlockByHash(blockHash)
	parentRoot := common.Hash{}
	if currentBlock.NumberU64() != 0 {
		parentBlock := sds.BlockChain.GetBlockByHash(currentBlock.ParentHash())
		parentRoot = parentBlock.Root()
	}
	return sds.writeStateDiffWithRetry(currentBlock, parentRoot, params)
}

// Writes a state diff from the current block, parent state root, and provided params
func (sds *Service) writeStateDiff(block *types.Block, parentRoot common.Hash, params Params) error {
	// log.Info("Writing state diff", "block height", block.Number().Uint64())
	var totalDifficulty *big.Int
	var receipts types.Receipts
	var err error
	var tx *ind.BlockTx
	if params.IncludeTD {
		totalDifficulty = sds.BlockChain.GetTd(block.Hash(), block.NumberU64())
	}
	if params.IncludeReceipts {
		receipts = sds.BlockChain.GetReceiptsByHash(block.Hash())
	}
	tx, err = sds.indexer.PushBlock(block, receipts, totalDifficulty)
	if err != nil {
		return err
	}
	// defer handling of commit/rollback for any return case
	defer tx.Close(err)
	output := func(node StateNode) error {
		return sds.indexer.PushStateNode(tx, node)
	}
	codeOutput := func(c CodeAndCodeHash) error {
		return sds.indexer.PushCodeAndCodeHash(tx, c)
	}
	err = sds.Builder.WriteStateDiffObject(StateRoots{
		NewStateRoot: block.Root(),
		OldStateRoot: parentRoot,
	}, params, output, codeOutput)

	// allow dereferencing of parent, keep current locked as it should be the next parent
	sds.BlockChain.UnlockTrie(parentRoot)
	if err != nil {
		return err
	}
	return nil
}

// Wrapper function on writeStateDiff to retry when the deadlock is detected.
func (sds *Service) writeStateDiffWithRetry(block *types.Block, parentRoot common.Hash, params Params) error {
	var err error
	for i := uint(0); i < sds.maxRetry; i++ {
		err = sds.writeStateDiff(block, parentRoot, params)
		if err != nil && strings.Contains(err.Error(), deadlockDetected) {
			// Retry only when the deadlock is detected.
			continue
		}
		break
	}
	return err
}

// Adds the provided address to the list of watched addresses in write loop params and to the watched addresses file
func (sds *Service) WatchAddress(address common.Address) error {
	// Check if address is already being watched
	if containsAddress(writeLoopParams.WatchedAddresses, address) {
		return fmt.Errorf("Address %s already watched", address)
	}

	// Check if the watched addresses file exists
	fileExists, err := doesFileExist(watchedAddressesFile)
	if err != nil {
		return err
	}

	// Create the watched addresses file if doesn't exist
	if !fileExists {
		_, err := os.Create(watchedAddressesFile)
		if err != nil {
			return err
		}
	}

	watchedAddresses := append(writeLoopParams.WatchedAddresses, address)

	// Write the updated list of watched address to a json file
	content, err := json.Marshal(watchedAddresses)
	err = ioutil.WriteFile(watchedAddressesFile, content, 0644)
	if err != nil {
		return err
	}

	// Update the in-memory params as well
	writeLoopParams.WatchedAddresses = watchedAddresses

	return nil
}

// Gets currently watched addresses from the in-memory write loop params
func (sds *Service) GetWathchedAddresses() []common.Address {
	return writeLoopParams.WatchedAddresses
}

// loadWatchedAddresses is used to load watched addresses to the in-memory write loop params from a json file if it exists
func loadWatchedAddresses() error {
	// Check if the watched addresses file exists
	fileExists, err := doesFileExist(watchedAddressesFile)
	if err != nil {
		return err
	}

	if fileExists {
		content, err := ioutil.ReadFile(watchedAddressesFile)
		if err != nil {
			return err
		}

		var watchedAddresses []common.Address
		err = json.Unmarshal(content, &watchedAddresses)
		if err != nil {
			return err
		}

		writeLoopParams.WatchedAddresses = watchedAddresses
	}

	return nil
}

// containsAddress is used to check if an address is present in the provided list of watched addresses
func containsAddress(watchedAddresses []common.Address, address common.Address) bool {
	for _, addr := range watchedAddresses {
		if addr == address {
			return true
		}
	}
	return false
}

// doesFileExist is used to check if file at a given path exists
func doesFileExist(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
