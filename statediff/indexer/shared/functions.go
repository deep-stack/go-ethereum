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

package shared

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/statediff/indexer/postgres"

	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	"github.com/jmoiron/sqlx"
	"github.com/multiformats/go-multihash"
)

// IPLDInsertPgStr is the postgres statement string for IPLDs inserting into public.blocks
const IPLDInsertPgStr = `INSERT INTO public.blocks (key, data) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING`

// HandleZeroAddrPointer will return an empty string for a nil address pointer
func HandleZeroAddrPointer(to *common.Address) string {
	if to == nil {
		return ""
	}
	return to.Hex()
}

// HandleZeroAddr will return an empty string for a 0 value address
func HandleZeroAddr(to common.Address) string {
	if to.Hex() == "0x0000000000000000000000000000000000000000" {
		return ""
	}
	return to.Hex()
}

// Rollback sql transaction and log any error
func Rollback(tx *sqlx.Tx) {
	if err := tx.Rollback(); err != nil {
		log.Error(err.Error())
	}
}

// MultihashKeyFromCID converts a cid into a blockstore-prefixed multihash db key string
func MultihashKeyFromCID(c cid.Cid) string {
	dbKey := dshelp.MultihashToDsKey(c.Hash())
	return blockstore.BlockPrefix.String() + dbKey.String()
}

// MultihashKeyFromKeccak256 converts keccak256 hash bytes into a blockstore-prefixed multihash db key string
func MultihashKeyFromKeccak256(hash common.Hash) (string, error) {
	mh, err := multihash.Encode(hash.Bytes(), multihash.KECCAK_256)
	if err != nil {
		return "", err
	}
	dbKey := dshelp.MultihashToDsKey(mh)
	return blockstore.BlockPrefix.String() + dbKey.String(), nil
}

// PublishDirectWithDB diretly writes a previously derived mhkey => value pair to the ipld database
func PublishDirectWithDB(db *postgres.DB, key string, value []byte) error {
	_, err := db.Exec(IPLDInsertPgStr, key, value)
	return err
}
