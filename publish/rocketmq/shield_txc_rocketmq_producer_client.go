package rocketmq

import (
	"context"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/domain"
	"github.com/fy403/local-msg-table/util"
	"strings"
)

// ShieldTxcRocketMQProducerClient 实现 EventPublish 接口
type ShieldTxcRocketMQProducerClient struct {
	txcRocketMQProducerImpl  *TxcRocketMQProducerImpl
	topic                    string
	nameSrvAddr              string
	retryTimesWhenSendFailed int
}

// NewShieldTxcRocketMQProducerClient 创建一个新的 ShieldTxcRocketMQProducerClient 实例
func NewShieldTxcRocketMQProducerClient(topic, nameSrvAddr string) *ShieldTxcRocketMQProducerClient {
	client := &ShieldTxcRocketMQProducerClient{
		topic:                    topic,
		nameSrvAddr:              nameSrvAddr,
		retryTimesWhenSendFailed: 0,
	}
	client.txcRocketMQProducerImpl = NewTxcRocketMQProducerImpl(topic, nameSrvAddr, 0)
	client.txcRocketMQProducerImpl.Start()
	return client
}

func NewShieldTxcRocketMQProducerClientWithRetry(topic, nameSrvAddr string, retryTimesWhenSendFailed int) *ShieldTxcRocketMQProducerClient {
	client := &ShieldTxcRocketMQProducerClient{
		topic:                    topic,
		nameSrvAddr:              nameSrvAddr,
		retryTimesWhenSendFailed: retryTimesWhenSendFailed,
	}
	client.txcRocketMQProducerImpl = NewTxcRocketMQProducerImpl(topic, nameSrvAddr, retryTimesWhenSendFailed)
	client.txcRocketMQProducerImpl.Start()
	return client
}

// SendCommitMsg 发送提交消息
func (c *ShieldTxcRocketMQProducerClient) sendCommitMsg(msg *primitive.Message, ctx context.Context, eventId string) *domain.BizResult {
	sendResult, err := c.txcRocketMQProducerImpl.GetCommitProducer().SendSync(ctx, msg)
	if err != nil {
		return domain.NewBizResult(constant.SEND_MESSAGE_FAIL)
	}
	return c.processAfterSendMsg(sendResult, eventId)
}

func (c *ShieldTxcRocketMQProducerClient) SendCommitMsg(msgBytes []byte, ctx context.Context, eventId string) *domain.BizResult {
	topic := util.Topic(constant.TRANSACTION_COMMIT_STAGE, c.topic)
	msg := &primitive.Message{
		Topic: topic,
		Body:  msgBytes,
	}
	return c.sendCommitMsg(msg, ctx, eventId)
}

// SendRollbackMsg 发送回滚消息
func (c *ShieldTxcRocketMQProducerClient) sendRollbackMsg(msg *primitive.Message, ctx context.Context, eventId string) *domain.BizResult {
	sendResult, err := c.txcRocketMQProducerImpl.GetRollbackProducer().SendSync(ctx, msg)
	if err != nil {
		return domain.NewBizResult(constant.SEND_MESSAGE_FAIL)
	}
	return c.processAfterSendMsg(sendResult, eventId)
}

func (c *ShieldTxcRocketMQProducerClient) SendRollbackMsg(msgBytes []byte, ctx context.Context, eventId string) *domain.BizResult {
	topic := util.Topic(constant.TRANSACTION_ROLLBACK_STAGE, c.topic)
	msg := &primitive.Message{
		Topic: topic,
		Body:  msgBytes,
	}
	return c.sendRollbackMsg(msg, ctx, eventId)
}

// processAfterSendMsg 发送消息后处理逻辑
func (c *ShieldTxcRocketMQProducerClient) processAfterSendMsg(sendResult *primitive.SendResult, eventId string) *domain.BizResult {
	if sendResult == nil {
		return domain.NewBizResult(constant.SEND_MESSAGE_FAIL)
	}
	msgId := sendResult.MsgID
	if strings.TrimSpace(msgId) == "" {
		return domain.NewBizResult(constant.SEND_MESSAGE_FAIL)
	}
	return domain.NewBizResult(constant.SEND_MESSAGE_SUCC)
}

// Init 初始化
func (c *ShieldTxcRocketMQProducerClient) Init() {
	// 初始化逻辑可以在这里添加
}

// Start 启动
func (c *ShieldTxcRocketMQProducerClient) Start() {
	c.txcRocketMQProducerImpl.Start()
}

// Shutdown 关闭
func (c *ShieldTxcRocketMQProducerClient) Shutdown() {
	c.txcRocketMQProducerImpl.Shutdown()
}

// MessageQueueType 返回消息队列实现类型
func (c *ShieldTxcRocketMQProducerClient) MessageQueueType() constant.MessageQueueType {
	return constant.ROCKETMQ
}

// GetCommitProducer 获取提交生产者
func (c *ShieldTxcRocketMQProducerClient) GetCommitProducer() rocketmq.Producer {
	return c.txcRocketMQProducerImpl.GetCommitProducer()
}

// GetRollbackProducer 获取回滚生产者
func (c *ShieldTxcRocketMQProducerClient) GetRollbackProducer() rocketmq.Producer {
	return c.txcRocketMQProducerImpl.GetRollbackProducer()
}

// GetTopic 获取主题
func (c *ShieldTxcRocketMQProducerClient) GetTopic() string {
	return c.topic
}

// GetNameSrvAddr 获取NameServer地址
func (c *ShieldTxcRocketMQProducerClient) GetNameSrvAddr() string {
	return c.nameSrvAddr
}

// GetRetryTimesWhenSendFailed 获取重试次数
func (c *ShieldTxcRocketMQProducerClient) GetRetryTimesWhenSendFailed() int {
	return c.retryTimesWhenSendFailed
}
