package schedule

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/domain"
	"github.com/fy403/local-msg-table/event"
	"github.com/fy403/local-msg-table/publish"
	"github.com/fy403/local-msg-table/subscrible"
	"github.com/fy403/local-msg-table/subscrible/rocketmq"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"sync"
	"time"
)

var (
	logger = logrus.WithField("module", "startTxcMessageScheduler")
)

// SendTxcMessageScheduler 定义发送事务消息调度线程
type SendTxcMessageScheduler struct {
	initialDelay              int64
	period                    int64
	corePoolSize              int
	baseEventService          event.BaseEventService
	shieldTxcProducerClient   publish.EventPublish
	timeUnit                  time.Duration
	executorService           *sync.WaitGroup
	stopChan                  chan struct{}
	shieldTxcConsumerListener subscrible.EventSubscribe
}

// NewSendTxcMessageScheduler 创建一个新的SendTxcMessageScheduler实例
func NewSendTxcMessageScheduler() *SendTxcMessageScheduler {
	return &SendTxcMessageScheduler{
		initialDelay:    0,
		period:          5,
		corePoolSize:    1,
		timeUnit:        time.Second,
		executorService: &sync.WaitGroup{},
		stopChan:        make(chan struct{}),
	}
}

// NewSendTxcMessageSchedulerWithConfig 创建一个新的SendTxcMessageScheduler实例并设置配置
func NewSendTxcMessageSchedulerWithConfig(initialDelay int64, period int64, corePoolSize int) *SendTxcMessageScheduler {
	return &SendTxcMessageScheduler{
		initialDelay:    initialDelay,
		period:          period,
		corePoolSize:    corePoolSize,
		timeUnit:        time.Second,
		executorService: &sync.WaitGroup{},
		stopChan:        make(chan struct{}),
	}
}

func (s *SendTxcMessageScheduler) RegisterCommitListenerWithRocketMQ(topicSource string, nameSrvAddr string, fn func(*domain.ShieldTxcMessage) consumer.ConsumeResult) {
	commitListener := rocketmq.NewShieldTxcCommitListener(fn, s.baseEventService)
	s.shieldTxcConsumerListener = rocketmq.NewShieldTxcRocketMQConsumerClient(topicSource, nameSrvAddr, commitListener, nil)
}

func (s *SendTxcMessageScheduler) RegisterRollbackListenerWithRocketMQ(topicSource string, nameSrvAddr string, fn func(*domain.ShieldTxcMessage) consumer.ConsumeResult) {
	rollbackListener := rocketmq.NewShieldTxcRollbackListener(fn, s.baseEventService)
	s.shieldTxcConsumerListener = rocketmq.NewShieldTxcRocketMQConsumerClient(topicSource, nameSrvAddr, nil, rollbackListener)
}

// Run 查询并发送消息
func (s *SendTxcMessageScheduler) Run() {
	ticker := time.NewTicker(s.timeUnit * time.Duration(s.period))
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.executorService.Add(1)
			go func() {
				defer func() {
					s.executorService.Done()
				}()
				// 获取待调度的消息，初始态==初始化
				shieldEvents, err := s.baseEventService.QueryEventListByStatus(constant.PRODUCE_INIT)
				if err != nil {
					logger.Errorf("Failed to query event list: %v", err)
					return
				}
				logger.Debugf("Ticker Query event list: %d\n", len(shieldEvents))
				if len(shieldEvents) == 0 {
					return
				}
				for _, shieldEvent := range shieldEvents {
					s.sendMessageSync(shieldEvent)
				}
			}()
		case <-s.stopChan:
			return
		}
	}
}

func (s *SendTxcMessageScheduler) sendMessageSync(shieldEvent *domain.ShieldEvent) {
	// 发送前改状态
	s.processBeforeSendMessage(shieldEvent)
	// 发送消息核心逻辑
	s.sendMessage(shieldEvent)
	// 判断发送结果,成功则更新为已发送
	s.processAfterSendMessage(shieldEvent)
}

func (s *SendTxcMessageScheduler) PutMessage(appId string, txType constant.TXType, eventId string, content string) error {
	if eventId == "" {
		eventId = uuid.New().String()
	}
	if appId == "" {
		return fmt.Errorf("please enter appId!")
	}
	event := &domain.ShieldEvent{
		EventID:     eventId,
		TxType:      string(txType),
		EventStatus: string(constant.PRODUCE_INIT),
		Content:     content,
		AppID:       appId,
	}

	insertResult, err := s.baseEventService.InsertEvent(event)
	if err != nil {
		return fmt.Errorf("insert ShieldEvent into DB occurred Exception! %w", err)
	}
	if !insertResult {
		return fmt.Errorf("insert ShieldEvent into DB failed")
	}
	go s.sendMessageSync(event)
	return nil
}

// processBeforeSendMessage 发送前改状态
func (s *SendTxcMessageScheduler) processBeforeSendMessage(shieldEvent *domain.ShieldEvent) {
	// 更新前状态:生产初始化
	shieldEvent.SetBeforeUpdateEventStatus(shieldEvent.GetEventStatus())
	// 更新后状态:生产处理中
	shieldEvent.SetEventStatus(constant.PRODUCE_PROCESSING)
	updateBefore, err := s.baseEventService.UpdateEventStatusById(shieldEvent)
	if !updateBefore || err != nil {
		// 更新失败
		return
	}
}

// sendMessage 发送事务消息
func (s *SendTxcMessageScheduler) sendMessage(shieldEvent *domain.ShieldEvent) {
	// 组装Message
	eventId := shieldEvent.EventID
	// Assemble the message
	shieldTxcMessage := &domain.ShieldTxcMessage{
		EventID:     shieldEvent.GetEventID(),
		AppID:       shieldEvent.GetAppID(),
		Content:     shieldEvent.GetContent(),
		EventStatus: shieldEvent.GetEventStatus(),
		TxType:      shieldEvent.GetTxType(),
	}
	messageBody, err := shieldTxcMessage.Encode()
	if err != nil {
		logger.Errorf("Failed to encode message: %v", err)
		return
	}

	var bizResult *domain.BizResult
	// 发送commit消息
	if shieldTxcMessage.TxType == constant.GetCommit() {
		bizResult = s.shieldTxcProducerClient.SendCommitMsg([]byte(messageBody), context.Background(), eventId)
	}
	// 发送rollback消息
	if shieldTxcMessage.TxType == constant.GetRollback() {
		bizResult = s.shieldTxcProducerClient.SendRollbackMsg([]byte(messageBody), context.Background(), eventId)
	}

	if err != nil {
		logger.Errorf("Failed to send message: %v", err)
		return
	}

	if bizResult.GetBizCode() != constant.SEND_MESSAGE_SUCC {
		logger.Debugf("[startTxcMessageScheduler] Send ShieldTxc Message result:[FAIL], Message Body:[%s]", messageBody)
		return
	}
	logger.Debugf("[startTxcMessageScheduler] Send ShieldTxc Message result:[SUCCESS], Message Body:[%s]", messageBody)
}

// processAfterSendMessage 发送后改状态
func (s *SendTxcMessageScheduler) processAfterSendMessage(shieldEvent *domain.ShieldEvent) {
	// 更新前状态:生产处理中
	shieldEvent.SetBeforeUpdateEventStatus(shieldEvent.GetEventStatus())
	// 更新后状态:生产处理完成
	shieldEvent.SetEventStatus(constant.PRODUCE_PROCESSED)
	updateBefore, err := s.baseEventService.UpdateEventStatusById(shieldEvent)
	if !updateBefore || err != nil {
		// 更新失败
		return
	}
}

// Schedule 启动调度
func (s *SendTxcMessageScheduler) Schedule() {
	go func() {
		time.Sleep(s.timeUnit * time.Duration(s.initialDelay))
		s.Run()
	}()
}

// Stop 停止调度
func (s *SendTxcMessageScheduler) Shutdown() {
	close(s.stopChan)
	s.executorService.Wait()
}

// GetCorePoolSize 获取核心线程数
func (s *SendTxcMessageScheduler) GetCorePoolSize() int {
	return s.corePoolSize
}

// SetCorePoolSize 设置核心线程数
func (s *SendTxcMessageScheduler) SetCorePoolSize(corePoolSize int) *SendTxcMessageScheduler {
	s.corePoolSize = corePoolSize
	return s
}

// GetInitialDelay 获取初始延迟
func (s *SendTxcMessageScheduler) GetInitialDelay() int64 {
	return s.initialDelay
}

// SetInitialDelay 设置初始延迟
func (s *SendTxcMessageScheduler) SetInitialDelay(initialDelay int64) *SendTxcMessageScheduler {
	s.initialDelay = initialDelay
	return s
}

// GetPeriod 获取周期
func (s *SendTxcMessageScheduler) GetPeriod() int64 {
	return s.period
}

// SetPeriod 设置周期
func (s *SendTxcMessageScheduler) SetPeriod(period int64) *SendTxcMessageScheduler {
	s.period = period
	return s
}

// GetTimeUnit 获取时间单位
func (s *SendTxcMessageScheduler) GetTimeUnit() time.Duration {
	return s.timeUnit
}

// SetBaseEventService 设置BaseEventService
func (s *SendTxcMessageScheduler) SetBaseEventService(baseEventService event.BaseEventService) *SendTxcMessageScheduler {
	s.baseEventService = baseEventService
	return s
}

// SetShieldTxcRocketMQProducerClient 设置ShieldTxcRocketMQProducerClient
func (s *SendTxcMessageScheduler) SetShieldTxcProducerClient(shieldTxcProducerClient publish.EventPublish) *SendTxcMessageScheduler {
	s.shieldTxcProducerClient = shieldTxcProducerClient
	return s
}
