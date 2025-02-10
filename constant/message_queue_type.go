package constant

// MessageQueueType 定义消息队列类型
type MessageQueueType string

const (
	// ROCKETMQ RocketMQ
	ROCKETMQ MessageQueueType = "ROCKETMQ"
	// KAFKA Kafka
	KAFKA MessageQueueType = "KAFKA"
	// RABBITMQ RabbitMQ
	RABBITMQ MessageQueueType = "RABBITMQ"
	// ACTIVEMQ ActiveMQ
	ACTIVEMQ MessageQueueType = "ACTIVEMQ"
)
