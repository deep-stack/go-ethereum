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

package indexer

import (
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	node "github.com/ipfs/go-ipld-format"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"

	"github.com/ethereum/go-ethereum/statediff/indexer/ipfs/ipld"
	"github.com/ethereum/go-ethereum/statediff/indexer/models"
)

const ipldBatchInsertPgStr string = `INSERT INTO public.blocks (key, data) VALUES (unnest($1::TEXT[]), unnest($2::BYTEA[])) ON CONFLICT (key) DO NOTHING`

// BlockTx wraps a Postgres tx with the state necessary for building the Postgres tx concurrently during trie difference iteration
type BlockTx struct {
	dbtx        *sqlx.Tx
	BlockNumber uint64
	headerID    int64
	Close       func(blockTx *BlockTx, err error) error

	quit      chan struct{}
	iplds     chan models.IPLDModel
	ipldCache models.IPLDBatch
}

func (tx *BlockTx) flush() error {
	_, err := tx.dbtx.Exec(ipldBatchInsertPgStr, pq.Array(tx.ipldCache.Keys), pq.Array(tx.ipldCache.Values))
	if err != nil {
		return err
	}
	tx.ipldCache = models.IPLDBatch{}
	return nil
}

// run in background goroutine to synchronize concurrent appends to the ipldCache
func (tx *BlockTx) cache() {
	for {
		select {
		case i := <-tx.iplds:
			tx.ipldCache.Keys = append(tx.ipldCache.Keys, i.Key)
			tx.ipldCache.Values = append(tx.ipldCache.Values, i.Data)
		case <-tx.quit:
			return
		}
	}
}

func (tx *BlockTx) cacheDirect(key string, value []byte) {
	tx.iplds <- models.IPLDModel{
		Key:  key,
		Data: value,
	}
}

func (tx *BlockTx) cacheIPLD(i node.Node) {
	tx.iplds <- models.IPLDModel{
		Key:  blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(i.Cid().Hash()).String(),
		Data: i.RawData(),
	}
}

func (tx *BlockTx) cacheRaw(codec, mh uint64, raw []byte) (string, string, error) {
	c, err := ipld.RawdataToCid(codec, raw, mh)
	if err != nil {
		return "", "", err
	}
	prefixedKey := blockstore.BlockPrefix.String() + dshelp.MultihashToDsKey(c.Hash()).String()
	tx.iplds <- models.IPLDModel{
		Key:  prefixedKey,
		Data: raw,
	}
	return c.String(), prefixedKey, err
}
