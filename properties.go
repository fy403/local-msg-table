package lmt

import (
	"errors"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/event"
	"github.com/fy403/local-msg-table/event/mysql"
	"github.com/fy403/local-msg-table/exception"
	"github.com/fy403/local-msg-table/publish"
	"github.com/fy403/local-msg-table/publish/rocketmq"
	"github.com/fy403/local-msg-table/schedule"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type Properties struct {
	RetryTimesWhenSendFailed    int
	TranMessageSendInitialDelay int
	TranMessageSendPeriod       int64
	TranMessageSendCorePoolSize int
	EventStoreType              constant.EventStoreType
	MessageQueueType            constant.MessageQueueType
	MySQLConfig                 MySQLConfig
	RocketMQConfig              RocketMQConfig
}

type MySQLConfig struct {
	DefaultDB    string
	DefaultTable string
}

type RocketMQConfig struct {
	NameSrvAddr string
	TopicSource string
}

func NewProperties() *Properties {
	return &Properties{
		RetryTimesWhenSendFailed:    0,
		TranMessageSendInitialDelay: 0,
		TranMessageSendPeriod:       5,
		TranMessageSendCorePoolSize: 1,
		EventStoreType:              constant.MYSQL,
		MessageQueueType:            constant.ROCKETMQ,
		MySQLConfig: MySQLConfig{
			DefaultDB:    constant.DEFAULT_SHIELD_TXC_DATABASE,
			DefaultTable: constant.DEFAULT_SHIELD_TXC_TABLE,
		},
		RocketMQConfig: RocketMQConfig{
			TopicSource: "",
		},
	}
}

// SetRetryTimesWhenSendFailed 设置RetryTimesWhenSendFailed
func (p *Properties) SetRetryTimesWhenSendFailed(retryTimesWhenSendFailed int) *Properties {
	p.RetryTimesWhenSendFailed = retryTimesWhenSendFailed
	return p
}

// SetTranMessageSendInitialDelay 设置TranMessageSendInitialDelay
func (p *Properties) SetTranMessageSendInitialDelay(tranMessageSendInitialDelay int) *Properties {
	p.TranMessageSendInitialDelay = tranMessageSendInitialDelay
	return p
}

// SetTranMessageSendPeriod 设置TranMessageSendPeriod
func (p *Properties) SetTranMessageSendPeriod(tranMessageSendPeriod int64) *Properties {
	p.TranMessageSendPeriod = tranMessageSendPeriod
	return p
}

// SetTranMessageSendCorePoolSize 设置TranMessageSendCorePoolSize
func (p *Properties) SetTranMessageSendCorePoolSize(tranMessageSendCorePoolSize int) *Properties {
	p.TranMessageSendCorePoolSize = tranMessageSendCorePoolSize
	return p
}

// SetEventStoreType 设置EventStoreType
func (p *Properties) SetEventStoreType(eventStoreType constant.EventStoreType) *Properties {
	p.EventStoreType = eventStoreType
	return p
}

// SetMessageQueueType 设置MessageQueueType
func (p *Properties) SetMessageQueueType(messageQueueType constant.MessageQueueType) *Properties {
	p.MessageQueueType = messageQueueType
	return p
}

// SetMySQLConfig 设置MySQLConfig
func (p *Properties) SetMySQLConfig(mysqlConfig MySQLConfig) *Properties {
	p.MySQLConfig = mysqlConfig
	return p
}

// SetRocketMQConfig 设置RocketMQConfig
func (p *Properties) SetRocketMQConfig(rocketMQConfig RocketMQConfig) *Properties {
	p.RocketMQConfig = rocketMQConfig
	return p
}

var (
	logger = logrus.WithField("module", "Properties")
)

// createEventStoreService 初始化BaseEventRepository实例
func (c *Properties) createEventStoreService(db *gorm.DB) event.BaseEventService {
	var baseEventRepository event.BaseEventService
	switch c.EventStoreType {
	case constant.MYSQL:
		if c.MySQLConfig.DefaultDB == "" {
			logger.Error("please set 'shield.event.mysql.defaultDB' which can not be NULL!")
			return nil
		}
		baseEventRepository = mysql.NewMySQLBaseEventRepositoryWithPage(db, c.MySQLConfig.DefaultTable, 100)
	}
	if baseEventRepository == nil {
		logger.Error("Failed to initialize [createEventStoreService] instance.")
		return nil
	}
	logger.Debug("Initializing [createEventStoreService] instance success.")
	return baseEventRepository
}

// createEventProducerClient 初始化ShieldTxcRocketMQProducerClient实例
func (c *Properties) createEventProducerClient() (publish.EventPublish, error) {
	retryTimesWhenSendFailed := c.RetryTimesWhenSendFailed
	var eventProducerClient publish.EventPublish
	switch c.MessageQueueType {
	case constant.ROCKETMQ:
		nameSrvAddr := c.RocketMQConfig.NameSrvAddr
		if nameSrvAddr == "" {
			return nil, errors.New("please set 'shield.event.rocketmq.nameSrvAddr' which can not be NULL!")
		}
		topicSource := c.RocketMQConfig.TopicSource
		if topicSource == "" {
			return nil, exception.NewBizException("make sure config value of 'shield.event.rocketmq.topicSource' not empty!")
		}
		eventProducerClient = rocketmq.NewShieldTxcRocketMQProducerClientWithRetry(topicSource, nameSrvAddr, retryTimesWhenSendFailed)
	}
	if eventProducerClient == nil {
		return nil, errors.New("failed to initialize [eventProducerClient] instance.")
	}
	logger.Debug("Initializing [ShieldTxcRocketMQProducerClient] instance success.")
	return eventProducerClient, nil
}

// startTxcMessageScheduler 初始化SendTxcMessageScheduler实例
func (c *Properties) startTxcMessageScheduler(db *gorm.DB) (*schedule.SendTxcMessageScheduler, error) {
	baseEventRepository := c.createEventStoreService(db)
	eventProducerClient, err := c.createEventProducerClient()
	if err != nil {
		return nil, err
	}
	sendTxcMessageScheduler := schedule.NewSendTxcMessageScheduler()
	sendTxcMessageScheduler.SetInitialDelay(int64(c.TranMessageSendInitialDelay))
	sendTxcMessageScheduler.SetPeriod(c.TranMessageSendPeriod)
	sendTxcMessageScheduler.SetCorePoolSize(c.TranMessageSendCorePoolSize)
	sendTxcMessageScheduler.SetBaseEventService(baseEventRepository)
	sendTxcMessageScheduler.SetShieldTxcProducerClient(eventProducerClient)
	//logger.Debug("Initializing [StartTxcMessageSchedulerSendTxcMessageScheduler] instance success.")
	//sendTxcMessageScheduler.Schedule()
	return sendTxcMessageScheduler, nil
}

// Init 初始化所有配置
func (c *Properties) Init(db *gorm.DB) (*schedule.SendTxcMessageScheduler, error) {
	return c.startTxcMessageScheduler(db)
}
