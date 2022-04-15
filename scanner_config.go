package scanner

import "time"

type ScannerOptions struct {
	RpcURL                   string
	BlockBatchSize           uint64
	MinimumConfirmationBlock uint64
	PullInterval             time.Duration
}

func DefaultScannerOptions() *ScannerOptions {
	return &ScannerOptions{
		RpcURL:                   "",
		BlockBatchSize:           2000,
		MinimumConfirmationBlock: 12,
		PullInterval:             5 * time.Second,
	}
}

func (opts *ScannerOptions) SetRpcURL(rpcUrl string) *ScannerOptions {
	opts.RpcURL = rpcUrl
	return opts
}

func (opts *ScannerOptions) SetBlockBatchSize(blockBatchSize uint64) *ScannerOptions {
	opts.BlockBatchSize = blockBatchSize
	return opts
}

func (opts *ScannerOptions) SetMinimumConfirmationBlock(minimumConfirmationBlock uint64) *ScannerOptions {
	opts.MinimumConfirmationBlock = minimumConfirmationBlock
	return opts
}

func (opts *ScannerOptions) SetPullInterval(pullInterval time.Duration) *ScannerOptions {
	opts.PullInterval = pullInterval
	return opts
}
