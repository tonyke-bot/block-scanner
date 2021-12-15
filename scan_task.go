package scanner

import (
	"context"
	"math/big"
	"time"

	"github.com/THaGKi9/block-scanner/util"
	"github.com/ethereum/go-ethereum"
	"gorm.io/gorm"
)

// ScanTask defines an interface where developers can extend to implements different
// block scanner.
type ScanTask interface {
	// Name returns the name of a task. The name should be unique
	Name() string

	// Enabled returns whether the task is enabled
	Enabled() bool

	// StartBlock returns the start block of the task if the task is new
	StartBlock() uint64

	// BlockBatchSize returns the maxmium range of new blocks that this task wants to be notified
	BlockBatchSize() *big.Int

	// PullInterval returns the interval of polling for new block
	PullInterval() time.Duration

	// SetEthClient is called by the scanner to set an ethereum client to this task
	SetEthClient(client *util.RetryableEthclient)

	// SetArchiveSyncedEthClient is called by the scanner to set an fullsynced ethereum client to this task
	SetArchiveSyncedEthClient(client *util.RetryableEthclient)

	// NeedBlockInfo returns a boolean value to indicate whether the scanner should pass block information or not
	NeedBlockInfo() bool

	// SkipWhenNoLogs returns a boolean value to indicate whether the scanner should skip block
	SkipWhenNoLogs() bool

	// LogFilter returns the filters use to query logs. If nil is returned, no logs will be downloaded
	LogFilter() []*ethereum.FilterQuery

	// OnNewBlocks is fired when there are multiple new blocks
	OnNewBlock(ctx context.Context, block *BlockInfo, db *gorm.DB) error

	// OnBlockCommited is fired after the scanner commited the database changed
	OnBlockCommitted(ctx context.Context, block *BlockInfo) error
}

type ScanTaskOverridesEthRpcURL interface {
	// EthRpcUrl returns the URL to an etheruem network RPC endpoint
	EthRpcUrl() string

	// ArchiveSyncedEthRpcUrl returns the URL to an full-synced etheruem network RPC endpoint
	ArchiveSyncedEthRpcUrl() string
}

type ScanTaskStopable interface {
	// StopAtBlock returns at which block the scanner should stop
	StopAtBlock() uint64
}
