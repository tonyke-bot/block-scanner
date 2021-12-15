package util

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
)

func WeiToEther(value decimal.Decimal) decimal.Decimal {
	return value.Div(decimal.New(1, 18))
}

func AddressEquals(a, b common.Address) bool {
	for i := 0; i < common.AddressLength; i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func HashEquals(a, b common.Hash) bool {
	for i := 0; i < common.HashLength; i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

func IsZeroHash(hash common.Hash) bool {
	for _, b := range hash {
		if b != 0 {
			return false
		}
	}

	return true
}
