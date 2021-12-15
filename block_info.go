package scanner

import (
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

type BlockInfo struct {
	Number    *big.Int
	NumberU64 uint64
	Block     *types.Block
	Logs      []*types.Log
}
