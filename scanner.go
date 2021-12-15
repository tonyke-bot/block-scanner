package scanner

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/THaGKi9/block-scanner/util"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Scanner struct {
	startMutex    sync.Mutex
	started       bool
	runningTaskWg sync.WaitGroup

	options *ScannerOptions
	db      *gorm.DB

	ctx        context.Context
	cancelFunc context.CancelFunc

	scanTasks      []ScanTask
	scheduledTasks []ScheduledTask
}

func NewScanner(ctx context.Context, db *gorm.DB) *Scanner {
	ctx, cancelFunc := context.WithCancel(ctx)

	return &Scanner{
		started: false,
		db:      db,

		ctx:        ctx,
		cancelFunc: cancelFunc,
	}
}

func (s *Scanner) Start() error {
	s.startMutex.Lock()
	defer s.startMutex.Unlock()

	if len(s.scanTasks)+len(s.scheduledTasks) == 0 {
		return errors.New("there are no tasks to run")
	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Minute*10)
	defer cancelFunc()

	s.runningTaskWg.Add(len(s.scanTasks) + len(s.scheduledTasks))

	for _, task := range s.scanTasks {
		s.startScanTask(ctx, task)
	}

	for _, task := range s.scheduledTasks {
		s.startScheduledTask(ctx, task)
	}

	s.started = true

	return nil
}

func (s *Scanner) Stop(ctx context.Context) {
	s.startMutex.Lock()
	defer s.startMutex.Unlock()

	if !s.started {
		return
	}

	s.cancelFunc()

	c := make(chan struct{})
	go func() {
		defer close(c)
		s.runningTaskWg.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-c:
	}
}

func (s *Scanner) Done() <-chan *struct{} {
	ch := make(chan *struct{})

	go func() {
		s.runningTaskWg.Wait()
		ch <- nil
	}()

	return ch
}

// AddScanTask adds a new scan task to the scanner
func (s *Scanner) AddScanTask(task ScanTask) error {
	if s.started {
		return errors.New("the scanner has started")
	}

	// Validating name
	taskName := task.Name()
	if taskName == "" {
		return errors.New("task name must be defined")
	}

	for i, task := range s.scanTasks {
		if taskName == task.Name() {
			return fmt.Errorf("task name '%v' is duplicating with task #%v", taskName, i)
		}
	}

	// Add to list
	s.scanTasks = append(s.scanTasks, task)

	return nil
}

// AddScheduledTask adds a new scheduled task to the scanner
func (s *Scanner) AddScheduledTask(task ScheduledTask) error {
	if s.started {
		return errors.New("the scanner has started")
	}

	// Validating name
	taskName := task.Name()
	if taskName == "" {
		return errors.New("task name must be defined")
	}

	for i, task := range s.scheduledTasks {
		if taskName == task.Name() {
			return fmt.Errorf("task name '%v' is duplicating with task #%v", taskName, i)
		}
	}

	// Add to list
	s.scheduledTasks = append(s.scheduledTasks, task)

	return nil
}

// startScanTask create task when necessary and starts a goroutine to execute the task. It's possible that the start stage could fail.
// In this case, the start of the scanner should be treated as failed and abort everything.
func (s *Scanner) startScanTask(ctx context.Context, task ScanTask) error {
	logger := logrus.WithField("task", task.Name())

	if !task.Enabled() {
		s.runningTaskWg.Done()
		logger.Info("Task is disabled")
		return nil
	}

	taskModel, err := GetScannerTaskByName(s.db.WithContext(ctx), task.Name())
	if err != nil {
		s.runningTaskWg.Done()
		return fmt.Errorf("fail get scanner task: %v", err)
	} else if taskModel == nil {
		taskModel, err = CreateScannerTaskByName(s.db.WithContext(ctx), task.Name(), task.StartBlock())

		if err != nil {
			s.runningTaskWg.Done()
			return fmt.Errorf("fail to get scanner task: %v", err)
		}

		logger.Info("Create new task as this task doens't exist")
	}

	logger.Info("Starting task")

	// Use scanner ctx as the based context so that we are also to propagate cancellation with context
	taskCtx, cancelFunc := context.WithCancel(s.ctx)

	go func() {
		defer s.runningTaskWg.Done()
		defer cancelFunc()

		err := s.scanTaskDoWork(taskCtx, taskModel.NextBlock, task)

		if err == nil || errors.Is(err, context.Canceled) {
			logger.Info("Task finished")
		} else {
			logger.WithField("error", err).Error("Task aborted")
		}
	}()

	return nil
}

type bulkBlockInfo struct {
	fromBlock  uint64
	toBlock    uint64
	blockInfos map[uint64]*BlockInfo
}

func (s *Scanner) scanTaskDoWork(ctx context.Context, nextBlock uint64, task ScanTask) error {
	logger := logrus.WithField("task", task.Name())

	ethClientRpcUrl := s.options.RpcURL
	fullSyncedEthClientRpcUrl := s.options.ArchivedSyncedRpcURL

	if task, ok := task.(ScanTaskOverridesEthRpcURL); ok {
		ethClientRpcUrl = task.EthRpcUrl()
		fullSyncedEthClientRpcUrl = task.ArchiveSyncedEthRpcUrl()
	}

	ethClient, err := util.NewRetryableEthclient(
		ctx,
		ethClientRpcUrl,
		logger.WithField("client", "normal"),
		time.Millisecond*500,
		3)
	if err != nil {
		return fmt.Errorf("fail to connect to ethereum node: %v", err)
	}
	defer ethClient.Close()

	logger.Infof("Connected to the ethereum network: %v", ethClientRpcUrl)

	archiveSyncedEthClient, err := util.NewRetryableEthclient(
		ctx,
		fullSyncedEthClientRpcUrl,
		logger.WithField("client", "archived"),
		time.Millisecond*500,
		3)
	if err != nil {
		return fmt.Errorf("fail to connect to fullsync ethereum node: %v", err)
	}
	defer archiveSyncedEthClient.Close()

	logger.Infof("Connected to the fully synced ethereum network: %v", fullSyncedEthClientRpcUrl)

	task.SetEthClient(ethClient)
	task.SetArchiveSyncedEthClient(archiveSyncedEthClient)

	ticker := time.NewTicker(task.PullInterval())
	defer ticker.Stop()

	chData := make(chan *bulkBlockInfo, 5)
	wg := sync.WaitGroup{}
	wg.Add(1)

	go s.processData(ctx, wg, chData, logger, task)

	for {
		select {
		case <-ctx.Done():
			goto exit
		case <-ticker.C:
		}

		currentBlock, err := ethClient.BlockNumber()
		if err != nil {
			return err
		}

		confirmedBlock := currentBlock - s.options.MinimumConfirmationBlock
		if nextBlock > confirmedBlock {
			logger.WithField("next-block", nextBlock).Infof("Waiting for new block confirmation. Highest Block: %v. Last confirmed block: %v", currentBlock, confirmedBlock)
			continue
		}

		archiveSyncedClient := ethClient

		if confirmedBlock-nextBlock > 64 {
			archiveSyncedClient = archiveSyncedEthClient
		}

		task.SetArchiveSyncedEthClient(archiveSyncedClient)

		fromBlock := nextBlock
		toBlock := uint64(math.Min(float64(confirmedBlock), float64(fromBlock+task.BlockBatchSize().Uint64()-1)))

		blocksLogger := logger.WithField("block-range", fmt.Sprintf("%v-%v", fromBlock, toBlock))

		var getBlocksError, getLogsError error
		logs := map[uint64][]*types.Log{}
		blocks := map[uint64]*types.Block{}

		getDataWaitGroup := sync.WaitGroup{}
		getDataWaitGroup.Add(2)

		go func() {
			defer getDataWaitGroup.Done()

			if task.NeedBlockInfo() {
				blocks, getBlocksError = s.getBlocks(ctx, ethClient, blocksLogger, fromBlock, toBlock)
			}
		}()

		go func() {
			defer getDataWaitGroup.Done()

			if task.LogFilter() != nil {
				logs, getLogsError = s.getLogs(ctx, archiveSyncedClient, blocksLogger, fromBlock, toBlock, task.LogFilter())
			}
		}()

		getDataWaitGroup.Wait()

		if getBlocksError != nil {
			blocksLogger.WithError(getBlocksError).Errorf("Fail to get blocks")
			return err
		} else if getLogsError != nil {
			blocksLogger.WithError(getLogsError).Errorf("Fail to get logs")
			return err
		}

		blockInfos := map[uint64]*BlockInfo{}

		for i := fromBlock; i <= toBlock; i++ {
			blockInfos[i] = &BlockInfo{
				NumberU64: i,
				Number:    big.NewInt(int64(i)),
				Block:     blocks[i],
				Logs:      logs[i],
			}
		}

		chData <- &bulkBlockInfo{
			fromBlock:  fromBlock,
			toBlock:    toBlock,
			blockInfos: blockInfos,
		}

		nextBlock = toBlock + 1
	}

exit:

	close(chData)
	wg.Wait()

	return nil
}

func (s *Scanner) processData(ctx context.Context, wg sync.WaitGroup, chData <-chan *bulkBlockInfo, logger *logrus.Entry, task ScanTask) {
	stopAtBlock := uint64(math.MaxUint64)

	if task, ok := task.(ScanTaskStopable); ok {
		if b := task.StopAtBlock(); b > 0 {
			stopAtBlock = b
		}
	}

	taskModel, err := GetScannerTaskByName(s.db, task.Name())
	if err != nil {
		logger.WithError(err).Error("Fail to find the task in the database")
	}

	for bulkBlockInfo := range chData {
		fromBlock := bulkBlockInfo.fromBlock
		toBlock := bulkBlockInfo.toBlock

		blocksLogger := logger.WithFields(logrus.Fields{
			"start-block": bulkBlockInfo.fromBlock,
			"end-block":   bulkBlockInfo.toBlock,
		})

		dbTx := s.db.Begin().WithContext(ctx)
		rollbackFunc := func() {
			if result := dbTx.Rollback(); result.Error != nil {
				blocksLogger.WithError(result.Error).Error("Fail to rollback database transaction")
			} else {
				blocksLogger.Info("Successfully rollback the database transaction")
			}
		}

		for ; fromBlock <= toBlock; fromBlock++ {
			blockLogger := logger.WithField("block", fromBlock)

			blockInfo := bulkBlockInfo.blockInfos[fromBlock]

			if fromBlock > stopAtBlock {
				blockLogger.Info("The task is finished after reaching destination block")
				goto exit
			}

			if task.NeedBlockInfo() && blockInfo == nil {
				blockLogger.Errorf("Block is not downloaded")
			}

			if task.LogFilter() != nil && len(blockInfo.Logs) == 0 && task.SkipWhenNoLogs() {
				blockLogger.Info("Skip block as there is no logs")
				continue
			}

			err = util.ExecuteWithRecover(blockLogger, "ScanTask.OnNewBlock", func() error {
				return task.OnNewBlock(ctx, blockInfo, dbTx)
			})

			if err != nil {
				blockLogger.WithError(err).Error("Fail to process new block")
				rollbackFunc()
				goto exit
			}

			newToken := uuid.New().String()
			if err := UpdateNextBlock(dbTx, taskModel, fromBlock+1, newToken); err != nil {
				blockLogger.WithError(err).Errorf("Fail to set next block to %v", fromBlock+1)
				rollbackFunc()
				goto exit
			}

			taskModel.NextBlock = fromBlock + 1
			taskModel.JobToken = newToken

			// Post commitment
			err = util.ExecuteWithRecover(blockLogger, "ScanTask.OnBlockComitted", func() error {
				return task.OnBlockCommitted(ctx, blockInfo)
			})

			if err != nil {
				blocksLogger.WithError(err).Error("Fail to execute OnBlockCommitted")
			}
		}

		// Commit changes
		if result := dbTx.Commit(); result.Error != nil {
			blocksLogger.WithError(result.Error).Error("Fail to commit database transaction")
			rollbackFunc()
			goto exit
		}

		blocksLogger.Debug("Block changes committed")
	}

exit:

	return
}

func (s *Scanner) getLogs(ctx context.Context, ethClient *util.RetryableEthclient, logger *logrus.Entry, fromBlock, toBlock uint64, filters []*ethereum.FilterQuery) (map[uint64][]*types.Log, error) {
	var wg sync.WaitGroup
	wg.Add(len(filters))

	var logsMutex sync.Mutex
	var logsByBlock map[uint64][]*types.Log = make(map[uint64][]*types.Log)
	var failed bool

	for _, filter := range filters {
		localFilter := ethereum.FilterQuery{
			FromBlock: big.NewInt(int64(fromBlock)),
			ToBlock:   big.NewInt(int64(toBlock)),
			Topics:    filter.Topics[:],
			Addresses: filter.Addresses[:],
		}

		go func() {
			defer wg.Done()

			blocksToGet := toBlock - fromBlock + 1

			logger.Infof("Getting events for from %v blocks, from %v to %v...", blocksToGet, fromBlock, toBlock)
			logs, err := ethClient.FilterLogs(localFilter)

			if err != nil {
				logger.WithError(err).Error("Fail to get logs events")
				failed = true
			}

			logger.Infof("Get %v events for from %v blocks, %v to %v", len(logs), blocksToGet, fromBlock, toBlock)

			logsMutex.Lock()

			// Split events by block
			for _, log := range logs {
				if _, ok := logsByBlock[log.BlockNumber]; !ok {
					logsByBlock[log.BlockNumber] = nil
				}

				copiedLog := log
				logsByBlock[log.BlockNumber] = append(logsByBlock[log.BlockNumber], &copiedLog)
			}

			logsMutex.Unlock()
		}()
	}

	wg.Wait()

	if failed {
		return nil, errors.New("fail to get logs")
	}

	for _, logs := range logsByBlock {
		sort.Slice(logs, func(i, j int) bool {
			return logs[i].Index <= logs[j].Index
		})
	}

	return logsByBlock, nil
}

func (s *Scanner) getBlocks(ctx context.Context, ethClient *util.RetryableEthclient, logger *logrus.Entry, fromBlock, toBlock uint64) (map[uint64]*types.Block, error) {
	var wg sync.WaitGroup
	blocksToGet := toBlock - fromBlock + 1

	blocks := make(map[uint64]*types.Block)
	innerCtx, cancelFunc := context.WithCancel(ctx)
	defer cancelFunc()
	ethClient = ethClient.WithContext(innerCtx)

	counter := 0

	for blocksToGet > 0 {
		batchSize := int(math.Min(float64(blocksToGet), float64(s.options.BlockBatchSize)))
		blocksToGet -= uint64(batchSize)
		jobResult := make(chan *types.Block, batchSize)
		jobFails := int32(0)

		counter++
		logger.Infof("Getting %v blocks starting from block %v. Batch %v", batchSize, fromBlock, counter)

		wg.Add(batchSize)

		for i := 0; i < batchSize; i++ {
			go func(blockNumber *big.Int) {
				defer wg.Done()

				block, err := ethClient.BlockByNumber(blockNumber)

				if errors.Is(err, context.Canceled) {
					logger.Errorf("Task to get block %v is cancelled", blockNumber)
				} else if err != nil {
					logger.WithError(err).Errorf("Task to get block %v is failed", blockNumber)
					cancelFunc()
					jobFails = 0
					atomic.AddInt32(&jobFails, 1)
				} else {
					jobResult <- block
				}
			}(big.NewInt(int64(fromBlock)))

			fromBlock++
		}

		wg.Wait()
		close(jobResult)

		logger.Infof("Finish getting %v blocks starting from block %v. Batch %v", batchSize, fromBlock, counter)

		if jobFails > 0 {
			return nil, errors.New("fail to get blocks")
		}

		for block := range jobResult {
			blocks[block.NumberU64()] = block
		}
	}

	return blocks, nil
}

// startScanTask create task when necessary and starts a goroutine to execute the task. It's possible that the start stage could fail.
// In this case, the start of the scanner should be treated as failed and abort everything.
func (s *Scanner) startScheduledTask(ctx context.Context, task ScheduledTask) error {
	logger := logrus.WithField("task", task.Name())

	if !task.Enabled() {
		s.runningTaskWg.Done()
		logger.Info("Task is disabled")
		return nil
	}

	logger.Info("Starting task")

	// Use scanner ctx as the based context so that we are also to propagate cancellation with context
	taskCtx, cancelFunc := context.WithCancel(s.ctx)

	go func() {
		defer s.runningTaskWg.Done()
		defer cancelFunc()

		logger.Info("Initializing task")

		err := util.ExecuteWithRecover(logger, "ScheduledTask.Init", func() error {
			return task.Init(taskCtx)
		})
		if err != nil {
			logger.WithError(err).Error("Fail to initialize the task")
			return
		}

		logger.Info("Initialization finished")

		if err != nil {
			logger.WithField("error", err).Error("Task fails in initialization")
		} else {
			ticker := time.NewTicker(task.Interval())

			for err == nil {
				select {
				case <-taskCtx.Done():
					err = taskCtx.Err()
				case <-ticker.C:
					logger.Info("Executing task")

					err := util.ExecuteWithRecover(logger, "ScheduledTask.Execute", func() error {
						return task.Execute(taskCtx)
					})
					if err != nil {
						logger.WithError(err).Error("Fail to execute the task")
						return
					}
				}
			}
		}

		if err == nil || errors.Is(err, context.Canceled) {
			logger.Info("Task finished")
		} else {
			logger.WithField("error", err).Error("Task aborted")
		}
	}()

	return nil
}
