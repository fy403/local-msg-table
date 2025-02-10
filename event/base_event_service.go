package event

import (
	"github.com/fy403/local-msg-table/domain"
)

// BaseEventService 基础事件仓库接口
type BaseEventService interface {
	InsertEvent(event *domain.ShieldEvent) (bool, error)
	InsertEventWithId(event *domain.ShieldEvent) (bool, error)
	UpdateEventStatusById(event *domain.ShieldEvent) (bool, error)
	DeleteEventLogicallyById(event *domain.ShieldEvent) (bool, error)
	QueryEventListByStatus(eventStatus string) ([]*domain.ShieldEvent, error)
	QueryEventById(event *domain.ShieldEvent) (*domain.ShieldEvent, error)
}
