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

import (
	"context"

	v2 "github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres/v2"
	v3 "github.com/ethereum/go-ethereum/statediff/indexer/database/sql/postgres/v3"
	"github.com/ethereum/go-ethereum/statediff/indexer/interfaces"
)

// SetupV3SQLXDB is used to setup a sqlx db for tests
func SetupV3SQLXDB() (interfaces.Database, error) {
	driver, err := NewSQLXDriver(context.Background(), DefaultV3Config)
	if err != nil {
		return nil, err
	}
	return v3.NewPostgresDB(driver), nil
}

// SetupV3PGXDB is used to setup a pgx db for tests
func SetupV3PGXDB() (interfaces.Database, error) {
	driver, err := NewPGXDriver(context.Background(), DefaultV3Config)
	if err != nil {
		return nil, err
	}
	return v3.NewPostgresDB(driver), nil
}

// SetupV2SQLXDB is used to setup a sqlx db for tests
func SetupV2SQLXDB() (interfaces.Database, error) {
	driver, err := NewSQLXDriver(context.Background(), DefaultV2Config)
	if err != nil {
		return nil, err
	}
	return v2.NewPostgresDB(driver), nil
}

// SetupV2PGXDB is used to setup a pgx db for tests
func SetupV2PGXDB() (interfaces.Database, error) {
	driver, err := NewPGXDriver(context.Background(), DefaultV2Config)
	if err != nil {
		return nil, err
	}
	return v2.NewPostgresDB(driver), nil
}
