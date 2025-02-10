package rocketmq

import (
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/util"
	"log"
)

// TxcRocketMQProducerImpl 实现 EventPublish 接口
type TxcRocketMQProducerImpl struct {
	commitProducer           rocketmq.Producer
	rollbackProducer         rocketmq.Producer
	retryTimesWhenSendFailed int
}

// NewTxcRocketMQProducerImpl 创建一个新的 TxcRocketMQProducerImpl 实例
func NewTxcRocketMQProducerImpl(topic, nameSrvAddr string, retryTimesWhenSendFailed int) *TxcRocketMQProducerImpl {
	impl := &TxcRocketMQProducerImpl{
		retryTimesWhenSendFailed: retryTimesWhenSendFailed,
	}
	impl.initCommit(topic, nameSrvAddr, retryTimesWhenSendFailed)
	impl.initRollback(topic, nameSrvAddr, retryTimesWhenSendFailed)
	return impl
}

func (p *TxcRocketMQProducerImpl) GetCommitProducer() rocketmq.Producer {
	return p.commitProducer
}

func (p *TxcRocketMQProducerImpl) GetRollbackProducer() rocketmq.Producer {
	return p.rollbackProducer
}

// initCommit 初始化提交生产者
func (p *TxcRocketMQProducerImpl) initCommit(topic, nameSrvAddr string, retryTimesWhenSendFailed int) {
	log.Println("Initializing [TxcRocketMQProducerImpl.commitProducer] instance init success.")
	if topic == "" {
		log.Fatalf("please insert event publish topic")
	}
	if nameSrvAddr == "" {
		log.Fatalf("please insert RocketMQ NameServer address")
	}

	producerGroup := util.GroupID(constant.TRANSACTION_COMMIT_STAGE, topic)
	instance, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameSrvAddr})),
		producer.WithRetry(retryTimesWhenSendFailed),
		producer.WithGroupName(producerGroup),
	)
	if err != nil {
		log.Fatalf("init commit producer error: %s", err.Error())
	}
	p.commitProducer = instance // 将新实例赋值给指针
}

// initRollback 初始化回滚生产者
func (p *TxcRocketMQProducerImpl) initRollback(topic, nameSrvAddr string, retryTimesWhenSendFailed int) {
	log.Println("Initializing [TxcRocketMQProducerImpl.rollbackProducer] instance.")
	if topic == "" {
		log.Fatalf("please insert event publish topic")
	}
	if nameSrvAddr == "" {
		log.Fatalf("please insert RocketMQ NameServer address")
	}

	producerGroup := util.GroupID(constant.TRANSACTION_ROLLBACK_STAGE, topic)
	instance, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameSrvAddr})),
		producer.WithRetry(retryTimesWhenSendFailed),
		producer.WithGroupName(producerGroup),
	)
	if err != nil {
		log.Fatalf("init rollback producer error: %s", err.Error())
	}
	p.rollbackProducer = instance // 将新实例赋值给指针
}

// MessageQueueType 返回消息队列实现类型
func (p *TxcRocketMQProducerImpl) MessageQueueType() constant.MessageQueueType {
	return constant.ROCKETMQ
}

// Init 初始化
func (p *TxcRocketMQProducerImpl) Init() {
	// 初始化逻辑可以在这里添加
}

// Start 启动
func (p *TxcRocketMQProducerImpl) Start() {
	if err := p.commitProducer.Start(); err != nil {
		log.Fatalf("start commit producer error: %s", err.Error())
	}
	if err := p.rollbackProducer.Start(); err != nil {
		log.Fatalf("start rollback producer error: %s", err.Error())
	}
}

// Shutdown 关闭
func (p *TxcRocketMQProducerImpl) Shutdown() {
	if p.commitProducer != nil {
		p.commitProducer.Shutdown()
	}
	if p.rollbackProducer != nil {
		p.rollbackProducer.Shutdown()
	}
}
