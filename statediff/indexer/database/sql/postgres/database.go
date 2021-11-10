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
					 ON CONFLICT (genesis_block, network_id, node_id, chain_id)
					 DO UPDATE
                    	SET genesis_block = $1,
                        network_id = $2,
                        node_id = $3,
                        client_name = $4,
						chain_id = $5
                	 RETURNING id`
)

// NewPostgresDB returns a postgres.DB using the provided driver
func NewPostgresDB(driver sql.Driver) *DB {
	return &DB{driver}
}

// DB implements sql.Databse using a configured driver and Postgres statement syntax
type DB struct {
	sql.Driver
}

// InsertHeaderStm satisfies the sql.Statements interface
func (db *DB) InsertHeaderStm() string {
	return `INSERT INTO eth.header_cids (block_number, block_hash, parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
			ON CONFLICT (block_number, block_hash) DO UPDATE SET (parent_hash, cid, td, node_id, reward, state_root, tx_root, receipt_root, uncle_root, bloom, timestamp, mh_key, times_validated, base_fee) = ($3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, eth.header_cids.times_validated + 1, $16)
			RETURNING id`
}

// InsertUncleStm satisfies the sql.Statements interface
func (db *DB) InsertUncleStm() string {
	return `INSERT INTO eth.uncle_cids (block_hash, header_id, parent_hash, cid, reward, mh_key) VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (header_id, block_hash) DO UPDATE SET (parent_hash, cid, reward, mh_key) = ($3, $4, $5, $6)`
}

// InsertTxStm satisfies the sql.Statements interface
func (db *DB) InsertTxStm() string {
	return `INSERT INTO eth.transaction_cids (header_id, tx_hash, cid, dst, src, index, mh_key, tx_data, tx_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (header_id, tx_hash) DO UPDATE SET (cid, dst, src, index, mh_key, tx_data, tx_type) = ($3, $4, $5, $6, $7, $8, $9)
			RETURNING id`
}

// InsertAccessListElementStm satisfies the sql.Statements interface
func (db *DB) InsertAccessListElementStm() string {
	return `INSERT INTO eth.access_list_element (tx_id, index, address, storage_keys) VALUES ($1, $2, $3, $4)
			ON CONFLICT (tx_id, index) DO UPDATE SET (address, storage_keys) = ($3, $4)`
}

// InsertRctStm satisfies the sql.Statements interface
func (db *DB) InsertRctStm() string {
	return `INSERT INTO eth.receipt_cids (tx_id, leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		  	ON CONFLICT (tx_id) DO UPDATE SET (leaf_cid, contract, contract_hash, leaf_mh_key, post_state, post_status, log_root) = ($2, $3, $4, $5, $6, $7, $8)
		  	RETURNING id`
}

// InsertLogStm satisfies the sql.Statements interface
func (db *DB) InsertLogStm() string {
	return `INSERT INTO eth.log_cids (leaf_cid, leaf_mh_key, receipt_id, address, index, topic0, topic1, topic2, topic3, log_data) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (receipt_id, index) DO UPDATE SET (leaf_cid, leaf_mh_key, address, topic0, topic1, topic2, topic3, log_data) = ($1, $2, $4, $6, $7, $8, $9, $10)`
}

// InsertStateStm satisfies the sql.Statements interface
func (db *DB) InsertStateStm() string {
	return `INSERT INTO eth.state_cids (header_id, state_leaf_key, cid, state_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (header_id, state_path) DO UPDATE SET (state_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)
			RETURNING id`
}

// InsertAccountStm satisfies the sql.Statements interface
func (db *DB) InsertAccountStm() string {
	return `INSERT INTO eth.state_accounts (state_id, balance, nonce, code_hash, storage_root) VALUES ($1, $2, $3, $4, $5)
		  	ON CONFLICT (state_id) DO UPDATE SET (balance, nonce, code_hash, storage_root) = ($2, $3, $4, $5)`
}

// InsertStorageStm satisfies the sql.Statements interface
func (db *DB) InsertStorageStm() string {
	return `INSERT INTO eth.storage_cids (state_id, storage_leaf_key, cid, storage_path, node_type, diff, mh_key) VALUES ($1, $2, $3, $4, $5, $6, $7)
		  	ON CONFLICT (state_id, storage_path) DO UPDATE SET (storage_leaf_key, cid, node_type, diff, mh_key) = ($2, $3, $5, $6, $7)`
}

// InsertIPLDStm satisfies the sql.Statements interface
func (db *DB) InsertIPLDStm() string {
	return `INSERT INTO public.blocks (key, data) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`
}

// InsertIPLDsStm satisfies the sql.Statements interface
func (db *DB) InsertIPLDsStm() string {
	return `INSERT INTO public.blocks (key, data) VALUES (unnest($1::TEXT[]), unnest($2::BYTEA[])) ON CONFLICT (key) DO NOTHING`
}
