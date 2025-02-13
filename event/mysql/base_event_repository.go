package mysql

import (
	"fmt"
	"github.com/fy403/local-msg-table/domain"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"strings"
)

var (
	logger = logrus.WithField("module", "createBaseEventRepository")
)

// MySQLBaseEventRepository MySQL 实现
type MySQLBaseEventRepository struct {
	db            *gorm.DB
	tableName     string
	cursorOnePage uint
	lastCursor    int
}

// NewMySQLBaseEventRepository 创建一个新的 MySQLBaseEventRepository 实例
func NewMySQLBaseEventRepository(db *gorm.DB, tableName string) *MySQLBaseEventRepository {
	return &MySQLBaseEventRepository{
		db:        db,
		tableName: tableName,
	}
}

// NewMySQLBaseEventRepository 创建一个新的 MySQLBaseEventRepository 实例
func NewMySQLBaseEventRepositoryWithPage(db *gorm.DB, tableName string, pageSize uint) *MySQLBaseEventRepository {
	return &MySQLBaseEventRepository{
		db:            db,
		tableName:     tableName,
		cursorOnePage: pageSize,
	}
}

// InsertEvent 插入事件
func (r *MySQLBaseEventRepository) InsertEvent(event *domain.ShieldEvent) (bool, error) {
	var tx *gorm.DB
	if event.GetDB() == nil {
		tx = r.db
	} else {
		tx = event.GetDB()
	}
	event.SetRecordStatus(0)
	result := tx.Create(event)
	if result.Error != nil {
		logger.Errorf("Failed to insert event: %v", result.Error)
		return false, result.Error
	}
	return true, nil
}

// InsertEventWithId 插入事件带主键id
func (r *MySQLBaseEventRepository) InsertEventWithId(event *domain.ShieldEvent) (bool, error) {
	if event.EventID == "" {
		return false, fmt.Errorf("event ID cannot be empty")
	}
	event.SetRecordStatus(0)
	result := r.db.Create(event)
	if result.Error != nil {
		return false, result.Error
	}
	return true, nil
}

// UpdateEventStatusById 更新事件状态
func (r *MySQLBaseEventRepository) UpdateEventStatusById(event *domain.ShieldEvent) (bool, error) {
	result := r.db.Model(&domain.ShieldEvent{}).
		Where("tx_type = ? AND event_id = ? AND app_id = ? AND record_status = 0", event.GetTxType(), event.GetEventID(), event.GetAppID()).
		Update("event_status", event.GetEventStatus()).
		Update("before_update_event_status", event.GetBeforeUpdateEventStatus())
	if result.Error != nil {
		logger.Errorf("Failed to update event status: %v", result.Error)
		return false, result.Error
	}
	return true, nil
}

// DeleteEventLogicallyById 逻辑删除事件
func (r *MySQLBaseEventRepository) DeleteEventLogicallyById(event *domain.ShieldEvent) (bool, error) {
	result := r.db.Model(&domain.ShieldEvent{}).
		Where("event_id = ? AND app_id = ? ", event.GetEventID(), event.GetAppID()).
		Update("record_status", 1)
	if result.Error != nil {
		logger.Errorf("Failed to logically delete event: %v", result.Error)
		return false, result.Error
	}
	return true, nil
}

// QueryEventListByStatus 根据事件状态获取事件列表
func (r *MySQLBaseEventRepository) QueryEventListByStatus(eventStatus string) ([]*domain.ShieldEvent, error) {
	if r.cursorOnePage > 0 {
		// 如果设置了游标分页,则每次使用唯一性id（单调性）进行游标分页查找，存储中间参数到r中
		var resultList []*domain.ShieldEvent
		result := r.db.Where("event_status = ? AND record_status = 0 AND id > ?", eventStatus, r.lastCursor).
			Order("id").
			Limit(int(r.cursorOnePage)).
			Set("gorm:query_option", "FOR UPDATE").
			Find(&resultList)
		if result.Error != nil {
			logger.Errorf("Failed to query events by status with cursor: %v", result.Error)
			return nil, result.Error
		}
		// 更新游标为结果集中最后一个事件的ID
		if len(resultList) > 0 {
			r.lastCursor = resultList[len(resultList)-1].ID
		}
		return resultList, nil
	} else {
		var resultList []*domain.ShieldEvent
		result := r.db.Where("event_status = ? AND record_status = 0", eventStatus).
			Set("gorm:query_option", "FOR UPDATE").
			Find(&resultList)
		if result.Error != nil {
			logger.Errorf("Failed to query events by status: %v", result.Error)
			return nil, result.Error
		}
		return resultList, nil
	}
}

// QueryEventById 查询事件详情
func (r *MySQLBaseEventRepository) QueryEventById(event *domain.ShieldEvent) (*domain.ShieldEvent, error) {
	var shieldEvent domain.ShieldEvent
	result := r.db.First(&shieldEvent, "tx_type = ? AND event_id = ? AND app_id = ? AND record_status = 0 ", event.GetTxType(), event.GetEventID(), event.GetAppID())
	if result.Error != nil {
		if strings.Contains(result.Error.Error(), "record not found") {
			return nil, nil
		}
		logger.Errorf("Failed to query event by ID: %v", result.Error)
		return nil, result.Error
	}
	shieldEvent.SetSuccess(true)
	return &shieldEvent, nil
}
