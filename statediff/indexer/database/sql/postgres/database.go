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

package postgres

import "github.com/ethereum/go-ethereum/statediff/indexer/database/sql"

var _ sql.Database = &DB{}

const (
	createNodeStm = `INSERT INTO nodes (genesis_block, network_id, node_id, client_name, chain_id) VALUES ($1, $2, $3, $4, $5)
					 ON CONFLICT (node_id) DO NOTHING`
)

// NewPostgresDB returns a postgres.DB using the provided driver
func NewPostgresDB(driver sql.Driver) *DB {
	return &DB{driver}
}

// DB implements sql.Database using a configured driver and Postgres statement syntax
type DB struct {
	sql.Driver
}

// InsertHeaderStm satisfies the sql.Statements interface
func (db *DB) InsertHeaderStm() string {
	return `INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
			ON CONFLICT (block_hash) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, coinbase) = ($3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, eth.header_cids.times_validated + 1, $16)`
}

// InsertUncleStm satisfies the sql.Statements interface
func (db *DB) InsertUncleStm() string {
	return `INSERT INTO eth.uncle_cids (block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (block_hash) DO NOTHING`
}

// InsertTxStm satisfies the sql.Statements interface
func (db *DB) InsertTxStm() string {
	return `INSERT INTO eth.transaction_cids (header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type, value) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (tx_hash) DO NOTHING`
}

// InsertAccessListElementStm satisfies the sql.Statements interface
func (db *DB) InsertAccessListElementStm() string {
	return `INSERT INTO eth.access_list_elements (tx_id, index, address, storage_keys) VALUES ($1, $2, $3, $4)
			ON CONFLICT (tx_id, index) DO NOTHING`
}

// InsertRctStm satisfies the sql.Statements interface
func (db *DB) InsertRctStm() string {
	return `INSERT INTO eth.receipt_cids (tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		  	ON CONFLICT (tx_id) DO NOTHING`
}

// InsertLogStm satisfies the sql.Statements interface
func (db *DB) InsertLogStm() string {
	return `INSERT INTO eth.log_cids (leaf_cid, leaf_mh_key, rct_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (rct_id, index) DO NOTHING`
}

// InsertStateStm satisfies the sql.Statements interface
func (db *DB) InsertStateStm() string {
	return `INSERT INTO eth.state_cids (header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (header_id, state_path) DO UPDATE SET (state_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)`
}

// InsertAccountStm satisfies the sql.Statements interface
func (db *DB) InsertAccountStm() string {
	return `INSERT INTO eth.state_accounts (header_id, state_path, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5, $6)
		  	ON CONFLICT (header_id, state_path) DO NOTHING`
}

// InsertStorageStm satisfies the sql.Statements interface
func (db *DB) InsertStorageStm() string {
	return `INSERT INTO eth.storage_cids (header_id, state_path, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		  	ON CONFLICT (header_id, state_path, storage_path) DO UPDATE SET (storage_leaf_key, cid, node_type, diff, mh_key) = ($3, $4, $6, $7, $8)`
}

// InsertIPLDStm satisfies the sql.Statements interface
func (db *DB) InsertIPLDStm() string {
	return `INSERT INTO public.blocks (key, data) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`
}

// InsertIPLDsStm satisfies the sql.Statements interface
func (db *DB) InsertIPLDsStm() string {
	return `INSERT INTO public.blocks (key, data) VALUES (unnest($1::TEXT[]), unnest($2::BYTEA[])) ON CONFLICT (key) DO NOTHING`
}
