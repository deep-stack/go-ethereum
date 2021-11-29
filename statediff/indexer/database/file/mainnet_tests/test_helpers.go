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

package mainnet_tests

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	defaultBlockFilePath    = "./block"
	defaultReceiptsFilePath = "./receipts"
)

const (
	TEST_RAW_URL      = "TEST_RAW_URL"
	TEST_BLOCK_NUMBER = "TEST_BLOCK_NUMBER"
)

// TestConfig holds configuration params for mainnet tests
type TestConfig struct {
	RawURL      string
	BlockNumber *big.Int
}

// DefaultTestConfig is the default TestConfig
var DefaultTestConfig = TestConfig{
	RawURL:      "http://127.0.0.1:8545",
	BlockNumber: big.NewInt(12914665),
}

// TestBlocksAndReceiptsFromEnv retrieves the block and receipts using env variables to override default config
func TestBlocksAndReceiptsFromEnv() (*types.Block, types.Receipts, error) {
	conf := DefaultTestConfig
	rawURL := os.Getenv(TEST_RAW_URL)
	if rawURL == "" {
		fmt.Println("Warning: no raw url configured for statediffing mainnet tests")
	} else {
		conf.RawURL = rawURL
	}
	blockNumberStr := os.Getenv(TEST_BLOCK_NUMBER)
	blockNumber, ok := new(big.Int).SetString(blockNumberStr, 10)
	if !ok {
		fmt.Println("Warning: no blockNumber configured for statediffing mainnet tests")
	} else {
		conf.BlockNumber = blockNumber
	}
	return TestBlocksAndReceipts(conf)
}

// TestBlocksAndReceipts retrieves the block and receipts for the provided test config
// It first tries to load files from the local system before setting up and using an ethclient.Client to pull the data
func TestBlocksAndReceipts(conf TestConfig) (*types.Block, types.Receipts, error) {
	var cli *ethclient.Client
	var err error
	var block *types.Block
	var receipts types.Receipts
	blockFilePath := fmt.Sprintf("%s.%s.rlp", defaultBlockFilePath, conf.BlockNumber.String())
	if _, err = os.Stat(blockFilePath); !errors.Is(err, os.ErrNotExist) {
		block, err = LoadBlockRLP(blockFilePath)
		if err != nil {
			cli, err = ethclient.Dial(conf.RawURL)
			if err != nil {
				return nil, nil, err
			}
			block, err = FetchBlock(cli, conf.BlockNumber)
			if err != nil {
				return nil, nil, err
			}
		}
	} else {
		cli, err = ethclient.Dial(conf.RawURL)
		if err != nil {
			return nil, nil, err
		}
		block, err = FetchBlock(cli, conf.BlockNumber)
		if err != nil {
			return nil, nil, err
		}
	}
	receiptsFilePath := fmt.Sprintf("%s.%s.enc", defaultReceiptsFilePath, conf.BlockNumber.String())
	if _, err = os.Stat(receiptsFilePath); !errors.Is(err, os.ErrNotExist) {
		receipts, err = LoadReceiptsEncoding(receiptsFilePath, len(block.Transactions()))
		if err != nil {
			if cli == nil {
				cli, err = ethclient.Dial(conf.RawURL)
				if err != nil {
					return nil, nil, err
				}
			}
			receipts, err = FetchReceipts(cli, block)
			if err != nil {
				return nil, nil, err
			}
		}
	} else {
		if cli == nil {
			cli, err = ethclient.Dial(conf.RawURL)
			if err != nil {
				return nil, nil, err
			}
		}
		receipts, err = FetchReceipts(cli, block)
		if err != nil {
			return nil, nil, err
		}
	}
	return block, receipts, nil
}

// FetchBlock fetches the block at the provided height using the ethclient.Client
func FetchBlock(cli *ethclient.Client, blockNumber *big.Int) (*types.Block, error) {
	return cli.BlockByNumber(context.Background(), blockNumber)
}

// FetchReceipts fetches the receipts for the provided block using the ethclient.Client
func FetchReceipts(cli *ethclient.Client, block *types.Block) (types.Receipts, error) {
	receipts := make(types.Receipts, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		rct, err := cli.TransactionReceipt(context.Background(), tx.Hash())
		if err != nil {
			return nil, err
		}
		receipts[i] = rct
	}
	return receipts, nil
}

// WriteBlockRLP writes out the RLP encoding of the block to the provided filePath
func WriteBlockRLP(filePath string, block *types.Block) error {
	if filePath == "" {
		filePath = fmt.Sprintf("%s_%s.rlp", defaultBlockFilePath, block.Number().String())
	}
	if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot create file, file (%s) already exists", filePath)
	}
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("unable to create file (%s), err: %v", filePath, err)
	}
	if err := block.EncodeRLP(file); err != nil {
		return err
	}
	return file.Close()
}

// LoadBlockRLP loads block from the rlp at filePath
func LoadBlockRLP(filePath string) (*types.Block, error) {
	blockBytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	block := new(types.Block)
	return block, rlp.DecodeBytes(blockBytes, block)
}

// LoadReceiptsEncoding loads receipts from the encoding at filePath
func LoadReceiptsEncoding(filePath string, cap int) (types.Receipts, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	receipts := make(types.Receipts, 0, cap)
	for scanner.Scan() {
		rctBinary := scanner.Bytes()
		rct := new(types.Receipt)
		if err := rct.UnmarshalBinary(rctBinary); err != nil {
			return nil, err
		}
		receipts = append(receipts, rct)
	}
	return receipts, nil
}

// WriteReceiptsEncoding writes out the consensus encoding of the receipts to the provided io.WriteCloser
func WriteReceiptsEncoding(filePath string, blockNumber *big.Int, receipts types.Receipts) error {
	if filePath == "" {
		filePath = fmt.Sprintf("%s_%s.enc", defaultReceiptsFilePath, blockNumber.String())
	}
	if _, err := os.Stat(filePath); !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cannot create file, file (%s) already exists", filePath)
	}
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("unable to create file (%s), err: %v", filePath, err)
	}
	for _, rct := range receipts {
		rctEncoding, err := rct.MarshalBinary()
		if err != nil {
			return err
		}
		if _, err := file.Write(rctEncoding); err != nil {
			return err
		}
		if _, err := file.Write([]byte("\n")); err != nil {
			return err
		}
	}
	return file.Close()
}
