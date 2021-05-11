package ipld

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

type kind string

const (
	legacy  kind = "legacy"
	eip1559 kind = "eip2930"
)

var blockFileNames = []string{
	"eth-block-12252078",
	"eth-block-12365585",
	"eth-block-12365586",
}

var receiptsFileNames = []string{
	"eth-receipts-12252078",
	"eth-receipts-12365585",
	"eth-receipts-12365586",
}

var kinds = []kind{
	eip1559,
	eip1559,
	legacy,
}

type testCase struct {
	kind     kind
	block    *types.Block
	receipts types.Receipts
}

func loadBlockData(t *testing.T) []testCase {
	fileDir := "./eip2930_test_data"
	testCases := make([]testCase, len(blockFileNames))
	for i, blockFileName := range blockFileNames {
		blockRLP, err := ioutil.ReadFile(filepath.Join(fileDir, blockFileName))
		if err != nil {
			t.Fatalf("failed to load blockRLP from file, err %v", err)
		}
		block := new(types.Block)
		if err := rlp.DecodeBytes(blockRLP, block); err != nil {
			t.Fatalf("failed to decode blockRLP, err %v", err)
		}
		receiptsFileName := receiptsFileNames[i]
		receiptsRLP, err := ioutil.ReadFile(filepath.Join(fileDir, receiptsFileName))
		if err != nil {
			t.Fatalf("failed to load receiptsRLP from file, err %s", err)
		}
		receipts := make(types.Receipts, 0)
		if err := rlp.DecodeBytes(receiptsRLP, &receipts); err != nil {
			t.Fatalf("failed to decode receiptsRLP, err %s", err)
		}
		testCases[i] = testCase{
			block:    block,
			receipts: receipts,
			kind:     kinds[i],
		}
	}
	return testCases
}

func TestFromBlockAndReceipts(t *testing.T) {
	testCases := loadBlockData(t)
	for i, tc := range testCases {
		fmt.Printf("testing %d\r\n", i)
		_, _, _, _, _, _, err := FromBlockAndReceipts(tc.block, tc.receipts)
		if err != nil {
			t.Fatalf("error generating IPLDs from block and receipts, err %v, kind %s, block hash %s", err, tc.kind, tc.block.Hash())
		}
	}
}
