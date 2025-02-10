package rocketmq

import (
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/util"
	"log"
)

// ShieldTxcRocketMQConsumerClient 实现 EventSubscribe 接口
type ShieldTxcRocketMQConsumerClient struct {
	commitConsumer   rocketmq.PushConsumer
	rollbackConsumer rocketmq.PushConsumer
	nameSrvAddr      string
	topic            string
}

// NewShieldTxcRocketMQConsumerClient 创建一个新的 ShieldTxcRocketMQConsumerClient 实例
func NewShieldTxcRocketMQConsumerClient(topic, nameSrvAddr string, txCommtListener *ShieldTxcCommitListener, txRollbackListener *ShieldTxcRollbackListener) *ShieldTxcRocketMQConsumerClient {
	client := &ShieldTxcRocketMQConsumerClient{
		nameSrvAddr: nameSrvAddr,
		topic:       topic,
	}

	if txCommtListener == nil && txRollbackListener == nil {
		log.Fatalf("Please define at least one MessageListenerConcurrently instance, such as [ShieldTxcCommitListener] or [ShieldTxcRollbackListener] or both.")
	}

	if txCommtListener != nil {
		client.initCommitConsumer(topic, nameSrvAddr, txCommtListener)
		log.Println("Initializing [ShieldTxcRocketMQConsumerClient.CommitConsumer] instance init success.")
	}

	if txRollbackListener != nil {
		client.initRollbackConsumer(topic, nameSrvAddr, txRollbackListener)
		log.Println("Initializing [ShieldTxcRocketMQConsumerClient.RollbackListener] instance init success.")
	}

	return client
}

// initCommitConsumer 初始化事务提交消费者
func (c *ShieldTxcRocketMQConsumerClient) initCommitConsumer(topic, nameSrvAddr string, txCommtListener *ShieldTxcCommitListener) {
	consumerGroup := util.GroupID(constant.TRANSACTION_COMMIT_STAGE, topic)
	instance, _ := rocketmq.NewPushConsumer(
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameSrvAddr})),
		consumer.WithGroupName(consumerGroup),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithConsumerModel(consumer.Clustering),
	)
	instance.Subscribe(util.Topic(constant.TRANSACTION_COMMIT_STAGE, topic), consumer.MessageSelector{}, txCommtListener.ConsumeMessage())
	if err := instance.Start(); err != nil {
		log.Fatalf("Loading [ShieldTxcRocketMQConsumerClient.commitConsumer] occurred exception: %s", err.Error())
	}
	c.commitConsumer = instance
}

// initRollbackConsumer 初始化事务回滚消费者
func (c *ShieldTxcRocketMQConsumerClient) initRollbackConsumer(topic, nameSrvAddr string, txRollbackListener *ShieldTxcRollbackListener) {
	consumerGroup := util.GroupID(constant.TRANSACTION_ROLLBACK_STAGE, topic)
	instance, _ := rocketmq.NewPushConsumer(
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{nameSrvAddr})),
		consumer.WithGroupName(consumerGroup),
		consumer.WithConsumeFromWhere(consumer.ConsumeFromFirstOffset),
		consumer.WithConsumerModel(consumer.Clustering),
	)

	instance.Subscribe(util.Topic(constant.TRANSACTION_ROLLBACK_STAGE, topic), consumer.MessageSelector{}, txRollbackListener.ConsumeMessage())

	if err := instance.Start(); err != nil {
		log.Fatalf("Loading [ShieldTxcRocketMQConsumerClient.rollbackConsumer] occurred exception: %s", err.Error())
	}
	c.rollbackConsumer = instance
}

// Init 初始化
func (c *ShieldTxcRocketMQConsumerClient) Init() {
	// 初始化逻辑可以在这里添加
}

// Start 启动
func (c *ShieldTxcRocketMQConsumerClient) Start() {
	// 启动逻辑可以在这里添加
}

// Shutdown 关闭
func (c *ShieldTxcRocketMQConsumerClient) Shutdown() {
	c.commitConsumer.Shutdown()
	c.rollbackConsumer.Shutdown()
}

// MessageQueueType 返回消息队列实现类型
func (c *ShieldTxcRocketMQConsumerClient) MessageQueueType() constant.MessageQueueType {
	return constant.ROCKETMQ
}
