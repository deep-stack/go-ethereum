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

// Contains a batch of utility type declarations used by the tests. As the node
// operates on unique types, a lot of them are needed to check various features.

package statediff

import (
	"bytes"
	"fmt"

	"github.com/oleiade/lane"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/statediff/trie_helpers"
	types2 "github.com/ethereum/go-ethereum/statediff/types"
	"github.com/ethereum/go-ethereum/trie"
)

var (
	nullHashBytes     = common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000000")
	emptyNode, _      = rlp.EncodeToBytes(&[]byte{})
	emptyContractRoot = crypto.Keccak256Hash(emptyNode)
	nullCodeHash      = crypto.Keccak256Hash([]byte{}).Bytes()
)

// Builder interface exposes the method for building a state diff between two blocks
type Builder interface {
	BuildStateDiffObject(args Args, params Params) (types2.StateObject, error)
	BuildStateTrieObject(current *types.Block) (types2.StateObject, error)
	WriteStateDiffObject(args types2.StateRoots, params Params, output types2.StateNodeSink, codeOutput types2.CodeSink) error
}

type StateDiffBuilder struct {
	StateCache state.Database
}

type IterPair struct {
	Older, Newer trie.NodeIterator
}

// convenience
func StateNodeAppender(nodes *[]types2.StateNode) types2.StateNodeSink {
	return func(node types2.StateNode) error {
		*nodes = append(*nodes, node)
		return nil
	}
}
func StorageNodeAppender(nodes *[]types2.StorageNode) types2.StorageNodeSink {
	return func(node types2.StorageNode) error {
		*nodes = append(*nodes, node)
		return nil
	}
}
func CodeMappingAppender(codeAndCodeHashes *[]types2.CodeAndCodeHash) types2.CodeSink {
	return func(c types2.CodeAndCodeHash) error {
		*codeAndCodeHashes = append(*codeAndCodeHashes, c)
		return nil
	}
}

// NewBuilder is used to create a statediff builder
func NewBuilder(stateCache state.Database) Builder {
	return &StateDiffBuilder{
		StateCache: stateCache, // state cache is safe for concurrent reads
	}
}

// BuildStateTrieObject builds a state trie object from the provided block
func (sdb *StateDiffBuilder) BuildStateTrieObject(current *types.Block) (types2.StateObject, error) {
	currentTrie, err := sdb.StateCache.OpenTrie(current.Root())
	if err != nil {
		return types2.StateObject{}, fmt.Errorf("error creating trie for block %d: %v", current.Number(), err)
	}
	it := currentTrie.NodeIterator([]byte{})
	stateNodes, codeAndCodeHashes, err := sdb.buildStateTrie(it)
	if err != nil {
		return types2.StateObject{}, fmt.Errorf("error collecting state nodes for block %d: %v", current.Number(), err)
	}
	return types2.StateObject{
		BlockNumber:       current.Number(),
		BlockHash:         current.Hash(),
		Nodes:             stateNodes,
		CodeAndCodeHashes: codeAndCodeHashes,
	}, nil
}

func (sdb *StateDiffBuilder) buildStateTrie(it trie.NodeIterator) ([]types2.StateNode, []types2.CodeAndCodeHash, error) {
	stateNodes := make([]types2.StateNode, 0)
	codeAndCodeHashes := make([]types2.CodeAndCodeHash, 0)
	for it.Next(true) {
		// skip value nodes
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}
		node, nodeElements, err := trie_helpers.ResolveNode(it, sdb.StateCache.TrieDB())
		if err != nil {
			return nil, nil, err
		}
		switch node.NodeType {
		case types2.Leaf:
			var account types.StateAccount
			if err := rlp.DecodeBytes(nodeElements[1].([]byte), &account); err != nil {
				return nil, nil, fmt.Errorf("error decoding account for leaf node at path %x nerror: %v", node.Path, err)
			}
			partialPath := trie.CompactToHex(nodeElements[0].([]byte))
			valueNodePath := append(node.Path, partialPath...)
			encodedPath := trie.HexToCompact(valueNodePath)
			leafKey := encodedPath[1:]
			node.LeafKey = leafKey
			if !bytes.Equal(account.CodeHash, nullCodeHash) {
				var storageNodes []types2.StorageNode
				err := sdb.buildStorageNodesEventual(account.Root, true, StorageNodeAppender(&storageNodes))
				if err != nil {
					return nil, nil, fmt.Errorf("failed building eventual storage diffs for account %+v\r\nerror: %v", account, err)
				}
				node.StorageNodes = storageNodes
				// emit codehash => code mappings for cod
				codeHash := common.BytesToHash(account.CodeHash)
				code, err := sdb.StateCache.ContractCode(common.Hash{}, codeHash)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to retrieve code for codehash %s\r\n error: %v", codeHash.String(), err)
				}
				codeAndCodeHashes = append(codeAndCodeHashes, types2.CodeAndCodeHash{
					Hash: codeHash,
					Code: code,
				})
			}
			stateNodes = append(stateNodes, node)
		case types2.Extension, types2.Branch:
			stateNodes = append(stateNodes, node)
		default:
			return nil, nil, fmt.Errorf("unexpected node type %s", node.NodeType)
		}
	}
	return stateNodes, codeAndCodeHashes, it.Error()
}

// BuildStateDiffObject builds a statediff object from two blocks and the provided parameters
func (sdb *StateDiffBuilder) BuildStateDiffObject(args Args, params Params) (types2.StateObject, error) {
	var stateNodes []types2.StateNode
	var codeAndCodeHashes []types2.CodeAndCodeHash
	err := sdb.WriteStateDiffObject(
		types2.StateRoots{OldStateRoot: args.OldStateRoot, NewStateRoot: args.NewStateRoot},
		params, StateNodeAppender(&stateNodes), CodeMappingAppender(&codeAndCodeHashes))
	if err != nil {
		return types2.StateObject{}, err
	}
	return types2.StateObject{
		BlockHash:         args.BlockHash,
		BlockNumber:       args.BlockNumber,
		Nodes:             stateNodes,
		CodeAndCodeHashes: codeAndCodeHashes,
	}, nil
}

// WriteStateDiffObject writes a statediff object to output callback
func (sdb *StateDiffBuilder) WriteStateDiffObject(args types2.StateRoots, params Params, output types2.StateNodeSink, codeOutput types2.CodeSink) error {
	// Load tries for old and new states
	oldTrie, err := sdb.StateCache.OpenTrie(args.OldStateRoot)
	if err != nil {
		return fmt.Errorf("error creating trie for oldStateRoot: %v", err)
	}
	newTrie, err := sdb.StateCache.OpenTrie(args.NewStateRoot)
	if err != nil {
		return fmt.Errorf("error creating trie for newStateRoot: %v", err)
	}

	iterPairs := []IterPair{
		{
			Older: oldTrie.NodeIterator([]byte{}),
			Newer: newTrie.NodeIterator([]byte{}),
		},
		{
			Older: oldTrie.NodeIterator([]byte{}),
			Newer: newTrie.NodeIterator([]byte{}),
		},
	}

	return sdb.BuildStateDiff(iterPairs, params, output, codeOutput)
}

func (sdb *StateDiffBuilder) BuildStateDiff(iterPairs []IterPair, params Params, output types2.StateNodeSink, codeOutput types2.CodeSink) error {
	// collect a slice of all the nodes that were touched and exist at B (B-A)
	// a map of their leafkey to all the accounts that were touched and exist at B
	// and a slice of all the paths for the nodes in both of the above sets
	diffAccountsAtB, diffPathsAtB, err := sdb.createdAndUpdatedState(
		iterPairs[0].Older, iterPairs[0].Newer,
		params.watchedAddressesLeafKeys, params.IntermediateStateNodes, output)
	if err != nil {
		return fmt.Errorf("error collecting createdAndUpdatedNodes: %v", err)
	}

	// collect a slice of all the nodes that existed at a path in A and don't exist in B (A-B)
	// a map of their leafkey to all the accounts that were touched and exist at A
	diffAccountsAtA, err := sdb.deletedOrUpdatedState(
		iterPairs[1].Older, iterPairs[1].Newer,
		diffAccountsAtB, diffPathsAtB, params.watchedAddressesLeafKeys,
		params.IntermediateStateNodes, params.IntermediateStorageNodes, output)
	if err != nil {
		return fmt.Errorf("error collecting deletedOrUpdatedNodes: %v", err)
	}

	// collect and sort the leafkey keys for both account mappings into a slice
	createKeys := trie_helpers.SortKeys(diffAccountsAtB)
	deleteKeys := trie_helpers.SortKeys(diffAccountsAtA)

	// and then find the intersection of these keys
	// these are the leafkeys for the accounts which exist at both A and B but are different
	// this also mutates the passed in createKeys and deleteKeys, removing the intersection keys
	// and leaving the truly created or deleted keys in place
	updatedKeys := trie_helpers.FindIntersection(createKeys, deleteKeys)

	// build the diff nodes for the updated accounts using the mappings at both A and B as directed by the keys found as the intersection of the two
	err = sdb.buildAccountUpdates(
		diffAccountsAtB, diffAccountsAtA, updatedKeys,
		params.IntermediateStorageNodes, output)
	if err != nil {
		return fmt.Errorf("error building diff for updated accounts: %v", err)
	}
	// build the diff nodes for created accounts
	err = sdb.buildAccountCreations(diffAccountsAtB, params.IntermediateStorageNodes, output, codeOutput)
	if err != nil {
		return fmt.Errorf("error building diff for created accounts: %v", err)
	}
	return nil
}

// element type for intermediateNodeStack
type intermediateStateDiffNode struct {
	node        types2.StateNode
	shouldIndex bool
	hash        common.Hash
}

// createdAndUpdatedState returns
// a slice of all the nodes (optionally including intermediate nodes) that exist in a different state at B than A
// a mapping of their leafkeys to all the accounts that exist in a different state at B than A
// and a slice of the paths for all of the nodes included in both
func (sdb *StateDiffBuilder) createdAndUpdatedState(a, b trie.NodeIterator, watchedAddressesLeafKeys map[common.Hash]struct{}, shouldIndexIntermediateStateNodes bool, output types2.StateNodeSink) (types2.AccountMap, map[string]bool, error) {
	diffPathsAtB := make(map[string]bool)
	diffAcountsAtB := make(types2.AccountMap)
	it, _ := trie.NewDifferenceIterator(a, b)

	// stack to hold intermediate nodes into during difference iteration
	// used only when len(watchedAddressesLeafKeys) > 0
	var intermediateNodeStack *lane.Stack = lane.NewStack()

	// is the list of watched addresses empty
	isWatchedAddressesEmpty := len(watchedAddressesLeafKeys) == 0

	for it.Next(true) {
		// skip value nodes
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}

		node, nodeElements, err := trie_helpers.ResolveNode(it, sdb.StateCache.TrieDB())
		if err != nil {
			return nil, nil, err
		}
		switch node.NodeType {
		case types2.Leaf:
			// created vs updated is important for leaf nodes since we need to diff their storage
			// so we need to map all changed accounts at B to their leafkey, since account can change paths but not leafkey
			var account types.StateAccount
			if err := rlp.DecodeBytes(nodeElements[1].([]byte), &account); err != nil {
				return nil, nil, fmt.Errorf("error decoding account for leaf node at path %x nerror: %v", node.Path, err)
			}
			partialPath := trie.CompactToHex(nodeElements[0].([]byte))
			valueNodePath := append(node.Path, partialPath...)
			encodedPath := trie.HexToCompact(valueNodePath)
			leafKey := encodedPath[1:]
			if isWatchedAddress(watchedAddressesLeafKeys, leafKey) {
				// if the address at a leaf node is being watched, the intermediate nodes along it's path should be indexed
				// set shouldIndex of top (if available) of intermediateNodeStack to true
				if !isWatchedAddressesEmpty && shouldIndexIntermediateStateNodes && intermediateNodeStack.Size() > 0 {
					topIntermediateNode := intermediateNodeStack.Pop().(intermediateStateDiffNode)
					topIntermediateNode.shouldIndex = true
					intermediateNodeStack.Push(topIntermediateNode)
				}

				diffAcountsAtB[common.Bytes2Hex(leafKey)] = types2.AccountWrapper{
					NodeType:  node.NodeType,
					Path:      node.Path,
					NodeValue: node.NodeValue,
					LeafKey:   leafKey,
					Account:   &account,
				}
			}
		case types2.Extension, types2.Branch:
			if shouldIndexIntermediateStateNodes {
				// create a diff for any intermediate node that has changed at b
				// created vs updated makes no difference for intermediate nodes since we do not need to diff storage
				if isWatchedAddressesEmpty {
					if err := output(types2.StateNode{
						NodeType:  node.NodeType,
						Path:      node.Path,
						NodeValue: node.NodeValue,
					}); err != nil {
						return nil, nil, err
					}
				} else {
					// process nodes from intermediateNodeStack
					intermediateNodeStack, err = processCreatedOrUpdatedIntermediateNodes(intermediateNodeStack, node, it.Hash(), it.Parent(), output)
					if err != nil {
						return nil, nil, err
					}
				}
			}
		default:
			return nil, nil, fmt.Errorf("unexpected node type %s", node.NodeType)
		}

		// add both intermediate and leaf node paths to the list of diffPathsAtB
		diffPathsAtB[common.Bytes2Hex(node.Path)] = true
	}

	// after iterating over the diff of state tries, some intermediate nodes might be left to be processed in the stack
	// process outstanding intermediate nodes
	for !isWatchedAddressesEmpty && intermediateNodeStack.Size() > 0 {
		var err error
		intermediateNodeStack, err = popAndIndexCreatedOrUpdatedIntermediateNodes(intermediateNodeStack, output)
		if err != nil {
			return nil, nil, err
		}
	}

	return diffAcountsAtB, diffPathsAtB, it.Error()
}

// processCreatedOrUpdatedIntermediateNodes handles created or updated intermediate diff nodes
func processCreatedOrUpdatedIntermediateNodes(intermediateNodeStack *lane.Stack, currNode types2.StateNode, currHash, currParentHash common.Hash, output types2.StateNodeSink) (*lane.Stack, error) {
	// 1. compare parenthash of current intermediate node with hash of node at top of the intermediateNodeStack
	//    if unequal, that means the intermediate node at top of the stack is not a parent of the current node
	//   		the current node is on a branch different than that of node at the top of the stack
	//   		pop the top node and index if required
	//    repeat until the stack gets emptied or the hashes turn out to be equal
	// 2. if equal, that means the intermediate node at top of the stack is parent of the current node
	//    or if the intermediateNodeStack is empty
	//    	push the current node to the stack

	for intermediateNodeStack.Size() > 0 && intermediateNodeStack.First().(intermediateStateDiffNode).hash != currParentHash {
		var err error
		intermediateNodeStack, err = popAndIndexCreatedOrUpdatedIntermediateNodes(intermediateNodeStack, output)
		if err != nil {
			return nil, err
		}
	}

	intermediateNodeStack.Push(intermediateStateDiffNode{
		node: types2.StateNode{
			NodeType:  currNode.NodeType,
			Path:      currNode.Path,
			NodeValue: currNode.NodeValue,
		},
		hash: currHash,
	})

	return intermediateNodeStack, nil
}

// popAndIndexCreatedOrUpdatedIntermediateNodes pops and indexes (if required) the node at top of given intermediateNodeStack
func popAndIndexCreatedOrUpdatedIntermediateNodes(intermediateNodeStack *lane.Stack, output types2.StateNodeSink) (*lane.Stack, error) {
	intermediateNode := intermediateNodeStack.Pop().(intermediateStateDiffNode)

	if intermediateNode.shouldIndex {
		if err := output(intermediateNode.node); err != nil {
			return nil, err
		}

		// if a child gets indexed, it's parent should get indexed too
		if intermediateNodeStack.Size() > 0 {
			topIntermediateNode := intermediateNodeStack.Pop().(intermediateStateDiffNode)
			topIntermediateNode.shouldIndex = true
			intermediateNodeStack.Push(topIntermediateNode)
		}
	}

	return intermediateNodeStack, nil
}

// deletedOrUpdatedState returns a slice of all the paths that are emptied at B
// and a mapping of their leafkeys to all the accounts that exist in a different state at A than B
func (sdb *StateDiffBuilder) deletedOrUpdatedState(a, b trie.NodeIterator, diffAccountsAtB types2.AccountMap, diffPathsAtB map[string]bool, watchedAddressesLeafKeys map[common.Hash]struct{}, shouldIndexIntermediateStateNodes, shouldIndexIntermediateStorageNodes bool, output types2.StateNodeSink) (types2.AccountMap, error) {
	diffAccountAtA := make(types2.AccountMap)
	it, _ := trie.NewDifferenceIterator(b, a)

	// stack to hold intermediate nodes into during difference iteration
	// used only when len(watchedAddressesLeafKeys) > 0
	var intermediateNodeStack *lane.Stack = lane.NewStack()

	// is the list of watched addresses empty
	isWatchedAddressesEmpty := len(watchedAddressesLeafKeys) == 0

	for it.Next(true) {
		// skip value nodes
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}

		node, nodeElements, err := trie_helpers.ResolveNode(it, sdb.StateCache.TrieDB())
		if err != nil {
			return nil, err
		}
		switch node.NodeType {
		case types2.Leaf:
			// map all different accounts at A to their leafkey
			var account types.StateAccount
			if err := rlp.DecodeBytes(nodeElements[1].([]byte), &account); err != nil {
				return nil, fmt.Errorf("error decoding account for leaf node at path %x nerror: %v", node.Path, err)
			}
			partialPath := trie.CompactToHex(nodeElements[0].([]byte))
			valueNodePath := append(node.Path, partialPath...)
			encodedPath := trie.HexToCompact(valueNodePath)
			leafKey := encodedPath[1:]
			if isWatchedAddress(watchedAddressesLeafKeys, leafKey) {
				// if the address at a leaf node is being watched, the intermediate nodes along it's path should be indexed
				// set shouldIndex of top (if available) of intermediateNodeStack to true
				if !isWatchedAddressesEmpty && shouldIndexIntermediateStateNodes && intermediateNodeStack.Size() > 0 {
					topIntermediateNode := intermediateNodeStack.Pop().(intermediateStateDiffNode)
					topIntermediateNode.shouldIndex = true
					intermediateNodeStack.Push(topIntermediateNode)
				}

				diffAccountAtA[common.Bytes2Hex(leafKey)] = types2.AccountWrapper{
					NodeType:  node.NodeType,
					Path:      node.Path,
					NodeValue: node.NodeValue,
					LeafKey:   leafKey,
					Account:   &account,
				}
				// if this node's path did not show up in diffPathsAtB
				// that means the node at this path was deleted (or moved) in B
				if _, ok := diffPathsAtB[common.Bytes2Hex(node.Path)]; !ok {
					var diff types2.StateNode
					// if this node's leaf key also did not show up in diffAccountsAtB
					// that means the node was deleted
					// in that case, emit an empty "removed" diff state node
					// include empty "removed" diff storage nodes for all the storage slots
					if _, ok := diffAccountsAtB[common.Bytes2Hex(leafKey)]; !ok {
						diff = types2.StateNode{
							NodeType:  types2.Removed,
							Path:      node.Path,
							LeafKey:   leafKey,
							NodeValue: []byte{},
						}

						var storageDiffs []types2.StorageNode
						err := sdb.buildRemovedAccountStorageNodes(account.Root, shouldIndexIntermediateStorageNodes, StorageNodeAppender(&storageDiffs))
						if err != nil {
							return nil, fmt.Errorf("failed building storage diffs for removed node %x\r\nerror: %v", node.Path, err)
						}
						diff.StorageNodes = storageDiffs
					} else {
						// emit an empty "removed" diff with empty leaf key if the account was moved
						diff = types2.StateNode{
							NodeType:  types2.Removed,
							Path:      node.Path,
							NodeValue: []byte{},
						}
					}

					if err := output(diff); err != nil {
						return nil, err
					}
				}
			}
		case types2.Extension, types2.Branch:
			// process nodes from intermediateNodeStack
			if shouldIndexIntermediateStateNodes {
				// if this node's path did not show up in diffPathsAtB
				// that means the node at this path was deleted (or moved) in B
				// emit an empty "removed" diff to signify as such
				if isWatchedAddressesEmpty {
					if _, ok := diffPathsAtB[common.Bytes2Hex(node.Path)]; !ok {
						if err := output(types2.StateNode{
							Path:      node.Path,
							NodeValue: []byte{},
							NodeType:  types2.Removed,
						}); err != nil {
							return nil, err
						}
					}
				} else {
					intermediateNodeStack, err = processDeletedOrUpdatedIntermediateNodes(intermediateNodeStack, node, it.Hash(), it.Parent(), diffPathsAtB, output)
				}
			}

			// fall through, we did everything we need to do with these node types
		default:
			return nil, fmt.Errorf("unexpected node type %s", node.NodeType)
		}
	}

	// after iterating over the diff of state tries, some intermediate nodes might be left to be processed in the stack
	// process outstanding intermediate nodes
	for !isWatchedAddressesEmpty && intermediateNodeStack.Size() > 0 {
		var err error
		intermediateNodeStack, err = popAndIndexDeletedOrUpdatedIntermediateNodes(intermediateNodeStack, diffPathsAtB, output)
		if err != nil {
			return nil, err
		}
	}

	return diffAccountAtA, it.Error()
}

// processDeletedOrUpdatedIntermediateNodes handles deleted or updated intermediate diff nodes
func processDeletedOrUpdatedIntermediateNodes(intermediateNodeStack *lane.Stack, currNode types2.StateNode, currHash, currParentHash common.Hash, diffPathsAtB map[string]bool, output types2.StateNodeSink) (*lane.Stack, error) {
	for intermediateNodeStack.Size() > 0 && intermediateNodeStack.First().(intermediateStateDiffNode).hash != currParentHash {
		var err error
		intermediateNodeStack, err = popAndIndexDeletedOrUpdatedIntermediateNodes(intermediateNodeStack, diffPathsAtB, output)
		if err != nil {
			return nil, err
		}
	}

	intermediateNodeStack.Push(intermediateStateDiffNode{
		node: types2.StateNode{
			Path:      currNode.Path,
			NodeValue: []byte{},
			NodeType:  types2.Removed,
		},
		hash: currHash,
	})

	return intermediateNodeStack, nil
}

// popAndIndexDeletedOrUpdatedIntermediateNodes pops and indexes (if required) the node at top of given intermediateNodeStack
func popAndIndexDeletedOrUpdatedIntermediateNodes(intermediateNodeStack *lane.Stack, diffPathsAtB map[string]bool, output types2.StateNodeSink) (*lane.Stack, error) {
	// pop from the stack
	intermediateNode := intermediateNodeStack.Pop().(intermediateStateDiffNode)

	if intermediateNode.shouldIndex {
		if _, ok := diffPathsAtB[common.Bytes2Hex(intermediateNode.node.Path)]; !ok {
			if err := output(intermediateNode.node); err != nil {
				return nil, err
			}
		}

		// if a child gets indexed, it's parent should get indexed too
		if intermediateNodeStack.Size() > 0 {
			topIntermediateNode := intermediateNodeStack.Pop().(intermediateStateDiffNode)
			topIntermediateNode.shouldIndex = true
			intermediateNodeStack.Push(topIntermediateNode)
		}
	}

	return intermediateNodeStack, nil
}

// buildAccountUpdates uses the account diffs maps for A => B and B => A and the known intersection of their leafkeys
// to generate the statediff node objects for all of the accounts that existed at both A and B but in different states
// needs to be called before building account creations and deletions as this mutates
// those account maps to remove the accounts which were updated
func (sdb *StateDiffBuilder) buildAccountUpdates(creations, deletions types2.AccountMap, updatedKeys []string, shouldIndexIntermediateStorageNodes bool, output types2.StateNodeSink) error {
	var err error
	for _, key := range updatedKeys {
		createdAcc := creations[key]
		deletedAcc := deletions[key]
		var storageDiffs []types2.StorageNode
		if deletedAcc.Account != nil && createdAcc.Account != nil {
			oldSR := deletedAcc.Account.Root
			newSR := createdAcc.Account.Root
			err = sdb.buildStorageNodesIncremental(
				oldSR, newSR, shouldIndexIntermediateStorageNodes,
				StorageNodeAppender(&storageDiffs))
			if err != nil {
				return fmt.Errorf("failed building incremental storage diffs for account with leafkey %s\r\nerror: %v", key, err)
			}
		}
		if err = output(types2.StateNode{
			NodeType:     createdAcc.NodeType,
			Path:         createdAcc.Path,
			NodeValue:    createdAcc.NodeValue,
			LeafKey:      createdAcc.LeafKey,
			StorageNodes: storageDiffs,
		}); err != nil {
			return err
		}
		delete(creations, key)
		delete(deletions, key)
	}

	return nil
}

// buildAccountCreations returns the statediff node objects for all the accounts that exist at B but not at A
// it also returns the code and codehash for created contract accounts
func (sdb *StateDiffBuilder) buildAccountCreations(accounts types2.AccountMap, shouldIndexIntermediateStorageNodes bool, output types2.StateNodeSink, codeOutput types2.CodeSink) error {
	for _, val := range accounts {
		diff := types2.StateNode{
			NodeType:  val.NodeType,
			Path:      val.Path,
			LeafKey:   val.LeafKey,
			NodeValue: val.NodeValue,
		}
		if !bytes.Equal(val.Account.CodeHash, nullCodeHash) {
			// For contract creations, any storage node contained is a diff
			var storageDiffs []types2.StorageNode
			err := sdb.buildStorageNodesEventual(val.Account.Root, shouldIndexIntermediateStorageNodes, StorageNodeAppender(&storageDiffs))
			if err != nil {
				return fmt.Errorf("failed building eventual storage diffs for node %x\r\nerror: %v", val.Path, err)
			}
			diff.StorageNodes = storageDiffs
			// emit codehash => code mappings for cod
			codeHash := common.BytesToHash(val.Account.CodeHash)
			code, err := sdb.StateCache.ContractCode(common.Hash{}, codeHash)
			if err != nil {
				return fmt.Errorf("failed to retrieve code for codehash %s\r\n error: %v", codeHash.String(), err)
			}
			if err := codeOutput(types2.CodeAndCodeHash{
				Hash: codeHash,
				Code: code,
			}); err != nil {
				return err
			}
		}
		if err := output(diff); err != nil {
			return err
		}
	}

	return nil
}

// buildStorageNodesEventual builds the storage diff node objects for a created account
// i.e. it returns all the storage nodes at this state, since there is no previous state
func (sdb *StateDiffBuilder) buildStorageNodesEventual(sr common.Hash, shouldIndexIntermediateNodes bool, output types2.StorageNodeSink) error {
	if bytes.Equal(sr.Bytes(), emptyContractRoot.Bytes()) {
		return nil
	}
	log.Debug("Storage Root For Eventual Diff", "root", sr.Hex())
	sTrie, err := sdb.StateCache.OpenTrie(sr)
	if err != nil {
		log.Info("error in build storage diff eventual", "error", err)
		return err
	}
	it := sTrie.NodeIterator(make([]byte, 0))
	err = sdb.buildStorageNodesFromTrie(it, shouldIndexIntermediateNodes, output)
	if err != nil {
		return err
	}
	return nil
}

// buildStorageNodesFromTrie returns all the storage diff node objects in the provided node interator
// including intermediate nodes can be turned on or off
func (sdb *StateDiffBuilder) buildStorageNodesFromTrie(it trie.NodeIterator, shouldIndexIntermediateNodes bool, output types2.StorageNodeSink) error {
	for it.Next(true) {
		// skip value nodes
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}
		node, nodeElements, err := trie_helpers.ResolveNode(it, sdb.StateCache.TrieDB())
		if err != nil {
			return err
		}
		switch node.NodeType {
		case types2.Leaf:
			partialPath := trie.CompactToHex(nodeElements[0].([]byte))
			valueNodePath := append(node.Path, partialPath...)
			encodedPath := trie.HexToCompact(valueNodePath)
			leafKey := encodedPath[1:]
			if err := output(types2.StorageNode{
				NodeType:  node.NodeType,
				Path:      node.Path,
				NodeValue: node.NodeValue,
				LeafKey:   leafKey,
			}); err != nil {
				return err
			}
		case types2.Extension, types2.Branch:
			if shouldIndexIntermediateNodes {
				if err := output(types2.StorageNode{
					NodeType:  node.NodeType,
					Path:      node.Path,
					NodeValue: node.NodeValue,
				}); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected node type %s", node.NodeType)
		}
	}
	return it.Error()
}

// buildRemovedAccountStorageNodes builds the "removed" diffs for all the storage nodes for a destroyed account
func (sdb *StateDiffBuilder) buildRemovedAccountStorageNodes(sr common.Hash, shouldIndexIntermediateNodes bool, output types2.StorageNodeSink) error {
	if bytes.Equal(sr.Bytes(), emptyContractRoot.Bytes()) {
		return nil
	}
	log.Debug("Storage Root For Removed Diffs", "root", sr.Hex())
	sTrie, err := sdb.StateCache.OpenTrie(sr)
	if err != nil {
		log.Info("error in build removed account storage diffs", "error", err)
		return err
	}
	it := sTrie.NodeIterator(make([]byte, 0))
	err = sdb.buildRemovedStorageNodesFromTrie(it, shouldIndexIntermediateNodes, output)
	if err != nil {
		return err
	}
	return nil
}

// buildRemovedStorageNodesFromTrie returns diffs for all the storage nodes in the provided node interator
// including intermediate nodes can be turned on or off
func (sdb *StateDiffBuilder) buildRemovedStorageNodesFromTrie(it trie.NodeIterator, shouldIndexIntermediateNodes bool, output types2.StorageNodeSink) error {
	for it.Next(true) {
		// skip value nodes
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}
		node, nodeElements, err := trie_helpers.ResolveNode(it, sdb.StateCache.TrieDB())
		if err != nil {
			return err
		}
		switch node.NodeType {
		case types2.Leaf:
			partialPath := trie.CompactToHex(nodeElements[0].([]byte))
			valueNodePath := append(node.Path, partialPath...)
			encodedPath := trie.HexToCompact(valueNodePath)
			leafKey := encodedPath[1:]
			if err := output(types2.StorageNode{
				NodeType:  types2.Removed,
				Path:      node.Path,
				NodeValue: []byte{},
				LeafKey:   leafKey,
			}); err != nil {
				return err
			}
		case types2.Extension, types2.Branch:
			if shouldIndexIntermediateNodes {
				if err := output(types2.StorageNode{
					NodeType:  types2.Removed,
					Path:      node.Path,
					NodeValue: []byte{},
				}); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unexpected node type %s", node.NodeType)
		}
	}
	return it.Error()
}

// buildStorageNodesIncremental builds the storage diff node objects for all nodes that exist in a different state at B than A
func (sdb *StateDiffBuilder) buildStorageNodesIncremental(oldSR common.Hash, newSR common.Hash, shouldIndexIntermediateNodes bool, output types2.StorageNodeSink) error {
	if bytes.Equal(newSR.Bytes(), oldSR.Bytes()) {
		return nil
	}
	log.Debug("Storage Roots for Incremental Diff", "old", oldSR.Hex(), "new", newSR.Hex())
	oldTrie, err := sdb.StateCache.OpenTrie(oldSR)
	if err != nil {
		return err
	}
	newTrie, err := sdb.StateCache.OpenTrie(newSR)
	if err != nil {
		return err
	}

	diffSlotsAtB, diffPathsAtB, err := sdb.createdAndUpdatedStorage(
		oldTrie.NodeIterator([]byte{}), newTrie.NodeIterator([]byte{}),
		shouldIndexIntermediateNodes, output)
	if err != nil {
		return err
	}
	err = sdb.deletedOrUpdatedStorage(oldTrie.NodeIterator([]byte{}), newTrie.NodeIterator([]byte{}),
		diffSlotsAtB, diffPathsAtB, shouldIndexIntermediateNodes, output)
	if err != nil {
		return err
	}
	return nil
}

func (sdb *StateDiffBuilder) createdAndUpdatedStorage(a, b trie.NodeIterator, shouldIndexIntermediateNodes bool, output types2.StorageNodeSink) (map[string]bool, map[string]bool, error) {
	diffPathsAtB := make(map[string]bool)
	diffSlotsAtB := make(map[string]bool)
	it, _ := trie.NewDifferenceIterator(a, b)
	for it.Next(true) {
		// skip value nodes
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}
		node, nodeElements, err := trie_helpers.ResolveNode(it, sdb.StateCache.TrieDB())
		if err != nil {
			return nil, nil, err
		}
		switch node.NodeType {
		case types2.Leaf:
			partialPath := trie.CompactToHex(nodeElements[0].([]byte))
			valueNodePath := append(node.Path, partialPath...)
			encodedPath := trie.HexToCompact(valueNodePath)
			leafKey := encodedPath[1:]
			diffSlotsAtB[common.Bytes2Hex(leafKey)] = true
			if err := output(types2.StorageNode{
				NodeType:  node.NodeType,
				Path:      node.Path,
				NodeValue: node.NodeValue,
				LeafKey:   leafKey,
			}); err != nil {
				return nil, nil, err
			}
		case types2.Extension, types2.Branch:
			if shouldIndexIntermediateNodes {
				if err := output(types2.StorageNode{
					NodeType:  node.NodeType,
					Path:      node.Path,
					NodeValue: node.NodeValue,
				}); err != nil {
					return nil, nil, err
				}
			}
		default:
			return nil, nil, fmt.Errorf("unexpected node type %s", node.NodeType)
		}
		diffPathsAtB[common.Bytes2Hex(node.Path)] = true
	}
	return diffSlotsAtB, diffPathsAtB, it.Error()
}

func (sdb *StateDiffBuilder) deletedOrUpdatedStorage(a, b trie.NodeIterator, diffSlotsAtB, diffPathsAtB map[string]bool, shouldIndexIntermediateNodes bool, output types2.StorageNodeSink) error {
	it, _ := trie.NewDifferenceIterator(b, a)
	for it.Next(true) {
		// skip value nodes
		if it.Leaf() || bytes.Equal(nullHashBytes, it.Hash().Bytes()) {
			continue
		}
		node, nodeElements, err := trie_helpers.ResolveNode(it, sdb.StateCache.TrieDB())
		if err != nil {
			return err
		}

		switch node.NodeType {
		case types2.Leaf:
			partialPath := trie.CompactToHex(nodeElements[0].([]byte))
			valueNodePath := append(node.Path, partialPath...)
			encodedPath := trie.HexToCompact(valueNodePath)
			leafKey := encodedPath[1:]

			// if this node's path did not show up in diffPathsAtB
			// that means the node at this path was deleted (or moved) in B
			if _, ok := diffPathsAtB[common.Bytes2Hex(node.Path)]; !ok {
				// if this node's leaf key also did not show up in diffSlotsAtB
				// that means the node was deleted
				// in that case, emit an empty "removed" diff storage node
				if _, ok := diffSlotsAtB[common.Bytes2Hex(leafKey)]; !ok {
					if err := output(types2.StorageNode{
						NodeType:  types2.Removed,
						Path:      node.Path,
						NodeValue: []byte{},
						LeafKey:   leafKey,
					}); err != nil {
						return err
					}
				} else {
					// emit an empty "removed" diff with empty leaf key if the account was moved
					if err := output(types2.StorageNode{
						NodeType:  types2.Removed,
						Path:      node.Path,
						NodeValue: []byte{},
					}); err != nil {
						return err
					}
				}
			}
		case types2.Extension, types2.Branch:
			if shouldIndexIntermediateNodes {
				// if this node's path did not show up in diffPathsAtB
				// that means the node at this path was deleted in B
				// in that case, emit an empty "removed" diff storage node
				if _, ok := diffPathsAtB[common.Bytes2Hex(node.Path)]; !ok {
					if err := output(types2.StorageNode{
						NodeType:  types2.Removed,
						Path:      node.Path,
						NodeValue: []byte{},
					}); err != nil {
						return err
					}
				}
			}
		default:
			return fmt.Errorf("unexpected node type %s", node.NodeType)
		}
	}
	return it.Error()
}

// isWatchedAddress is used to check if a state account corresponds to one of the addresses the builder is configured to watch
func isWatchedAddress(watchedAddressesLeafKeys map[common.Hash]struct{}, stateLeafKey []byte) bool {
	// If we aren't watching any specific addresses, we are watching everything
	if len(watchedAddressesLeafKeys) == 0 {
		return true
	}

	_, ok := watchedAddressesLeafKeys[common.BytesToHash(stateLeafKey)]
	return ok
}
