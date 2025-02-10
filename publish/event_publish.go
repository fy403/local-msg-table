package publish

import (
	"context"
	"github.com/fy403/local-msg-table/constant"
	"github.com/fy403/local-msg-table/domain"
)

// EventPublish 是事件发布接口
type EventPublish interface {
	MessageQueueType() constant.MessageQueueType
	Init()
	Start()
	Shutdown()
	SendCommitMsg(msgBytes []byte, ctx context.Context, eventId string) *domain.BizResult
	SendRollbackMsg(msgBytes []byte, ctx context.Context, eventId string) *domain.BizResult
}
