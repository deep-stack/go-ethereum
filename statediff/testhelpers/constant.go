package testhelpers

import (
	"math/big"

	"github.com/ethereum/go-ethereum/params"
)

var (
	BalanceChange1000     = int64(1000)
	BalanceChange10000    = int64(10000)
	BalanceChange1Ether   = int64(params.Ether)
	Block1Account1Balance = big.NewInt(BalanceChange10000)
	Block2Account2Balance = big.NewInt(21000000000000)
	GasFees               = int64(params.GWei) * int64(params.TxGas)
	ContractGasLimit      = uint64(1000000)
)
