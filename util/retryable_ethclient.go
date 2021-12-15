package util

import (
	"context"
	"errors"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/sirupsen/logrus"
)

type RetryableEthclient struct {
	ctx    context.Context
	client *ethclient.Client
	logger *logrus.Entry

	retryInterval time.Duration
	maxRetryTime  uint
}

func NewRetryableEthclient(ctx context.Context, rpcUrl string, logger *logrus.Entry, retryInterval time.Duration, maxRetryTime uint) (*RetryableEthclient, error) {
	logger.Debugf("Connecting to ethereum RPC endpoint %v", rpcUrl)
	rawEthclient, err := ethclient.DialContext(ctx, rpcUrl)
	if err != nil {
		return nil, err
	}

	client := &RetryableEthclient{
		ctx:    ctx,
		client: rawEthclient,
		logger: logger,

		retryInterval: retryInterval,
		maxRetryTime:  maxRetryTime,
	}

	return client, nil
}

func (cli *RetryableEthclient) WithContext(ctx context.Context) *RetryableEthclient {
	client := *cli
	client.ctx = ctx

	return &client
}

func (cli *RetryableEthclient) Close() {
	if cli.client != nil {
		cli.client.Close()
		cli.client = nil
	}
}

func (cli *RetryableEthclient) Execute(executionName string, execution func(context.Context, *ethclient.Client) error) (uint, error) {
	retriedTimes := uint(0)
	var err error

	for retriedTimes < cli.maxRetryTime {
		err = execution(cli.ctx, cli.client)

		if err == nil {
			break
		}

		if errors.Is(err, context.Canceled) {
			return 0, err
		}

		if retriedTimes < cli.maxRetryTime {
			cli.logger.WithError(err).WithField("execution", executionName).Errorf("Exection fails after %v attempts. %v attempts left", retriedTimes+1, cli.maxRetryTime-retriedTimes)

			retriedTimes++
			time.Sleep(cli.retryInterval)
		} else {
			break
		}
	}

	if errors.Is(err, context.Canceled) {
		return 0, err
	} else if err != nil {
		cli.logger.WithError(err).WithField("execution", executionName).Errorf("Exection fails after %v attempts", cli.maxRetryTime+1)

		return retriedTimes, err
	}

	return uint(retriedTimes), nil
}

// BlockNumber returns the most recent block number
func (cli *RetryableEthclient) BlockNumber() (uint64, error) {
	count := uint64(0)

	_, err := cli.Execute("BlockNumber", func(ctx context.Context, c *ethclient.Client) error {
		internalCount, err := c.BlockNumber(ctx)
		if err != nil {
			return err
		}

		count = internalCount
		return nil
	})

	return count, err
}

// HeaderByNumber returns a block header from the current canonical chain. If number is
// nil, the latest known header is returned.
func (cli *RetryableEthclient) HeaderByNumber(number uint64) (*types.Header, error) {
	var result *types.Header
	blockNumber := big.NewInt(int64(number))

	_, err := cli.Execute("HeaderByNumber", func(ctx context.Context, c *ethclient.Client) error {

		internalResult, err := c.HeaderByNumber(ctx, blockNumber)
		if err != nil {
			return err
		}

		result = internalResult
		return nil
	})

	return result, err
}

// FilterLogs executes a filter query.
func (cli *RetryableEthclient) FilterLogs(q ethereum.FilterQuery) ([]types.Log, error) {
	var result []types.Log

	_, err := cli.Execute("FilterLogs", func(ctx context.Context, c *ethclient.Client) error {
		internalResult, err := c.FilterLogs(ctx, q)
		if err != nil {
			return err
		}

		result = internalResult
		return nil
	})

	return result, err
}

// BlockByNumber executes ethclient.BlockByNumber.
func (cli *RetryableEthclient) BlockByNumber(blockNumber *big.Int) (*types.Block, error) {
	var result *types.Block

	_, err := cli.Execute("BlockByNumber", func(ctx context.Context, c *ethclient.Client) error {
		internalResult, err := c.BlockByNumber(ctx, blockNumber)
		if err != nil {
			return err
		}

		result = internalResult
		return nil
	})

	return result, err
}
