package subscrible

import (
	"github.com/fy403/local-msg-table/constant"
)

// EventSubscribe 是事件订阅接口
type EventSubscribe interface {
	// MessageQueueType 返回消息队列实现类型
	MessageQueueType() constant.MessageQueueType

	// Init 初始化
	Init()

	// Start 启动
	Start()

	// Shutdown 关闭
	Shutdown()
}
