package domain

import (
	"time"
)

// ShieldEvent 定义event映射实体
type ShieldEvent struct {
	ID           string `json:"id" gorm:"column:id;primaryKey;autoIncrement"`
	EventID      string `json:"eventId" gorm:"column:event_id"`
	Success      bool   `json:"success" gorm:"column:success"`
	TxType       string `json:"txType" gorm:"column:tx_type"`
	EventStatus  string `json:"eventStatus" gorm:"column:event_status"`
	Content      string `json:"content" gorm:"column:content"`
	AppID        string `json:"appId" gorm:"column:app_id"`
	RecordStatus int    `json:"recordStatus" gorm:"column:record_status"`
	//BizKey                  string    `json:"bizKey" gorm:"column:biz_key"`
	BeforeUpdateEventStatus string    `json:"beforeUpdateEventStatus" gorm:"column:before_update_event_status"`
	GmtCreate               time.Time `json:"gmtCreate" gorm:"column:gmt_create;autoCreateTime"`
	GmtUpdate               time.Time `json:"gmtUpdate" gorm:"column:gmt_update;autoUpdateTime"`
}

// TableName 指定表名为 shield_event
func (s *ShieldEvent) TableName() string {
	return "shield_event"
}

//// GetBizKey 获取业务键
//func (s *ShieldEvent) GetBizKey() string {
//	return s.BizKey
//}
//
//// SetBizKey 设置业务键
//func (s *ShieldEvent) SetBizKey(bizKey string) *ShieldEvent {
//	s.BizKey = bizKey
//	return s
//}

// GetTxType 获取事务类型
func (s *ShieldEvent) GetTxType() string {
	return s.TxType
}

// SetTxType 设置事务类型
func (s *ShieldEvent) SetTxType(txType string) *ShieldEvent {
	s.TxType = txType
	return s
}

// GetBeforeUpdateEventStatus 获取更新前状态
func (s *ShieldEvent) GetBeforeUpdateEventStatus() string {
	return s.BeforeUpdateEventStatus
}

// SetBeforeUpdateEventStatus 设置更新前状态
func (s *ShieldEvent) SetBeforeUpdateEventStatus(beforeUpdateEventStatus string) *ShieldEvent {
	s.BeforeUpdateEventStatus = beforeUpdateEventStatus
	return s
}

// GetSuccess 获取成功状态
func (s *ShieldEvent) GetSuccess() bool {
	return s.Success
}

// SetSuccess 设置成功状态
func (s *ShieldEvent) SetSuccess(success bool) *ShieldEvent {
	s.Success = success
	return s
}

// GetID 获取自增主键
func (s *ShieldEvent) GetEventID() string {
	return s.EventID
}

// SetID 设置自增主键
func (s *ShieldEvent) SetEventID(eventId string) *ShieldEvent {
	s.EventID = eventId
	return s
}

// GetEventStatus 获取事件状态
func (s *ShieldEvent) GetEventStatus() string {
	return s.EventStatus
}

// SetEventStatus 设置事件状态
func (s *ShieldEvent) SetEventStatus(eventStatus string) *ShieldEvent {
	s.EventStatus = eventStatus
	return s
}

// GetContent 获取业务实体
func (s *ShieldEvent) GetContent() string {
	return s.Content
}

// SetContent 设置业务实体
func (s *ShieldEvent) SetContent(content string) *ShieldEvent {
	s.Content = content
	return s
}

// GetAppID 获取应用ID
func (s *ShieldEvent) GetAppID() string {
	return s.AppID
}

// SetAppID 设置应用ID
func (s *ShieldEvent) SetAppID(appID string) *ShieldEvent {
	s.AppID = appID
	return s
}

// GetRecordStatus 获取记录状态
func (s *ShieldEvent) GetRecordStatus() int {
	return s.RecordStatus
}

// SetRecordStatus 设置记录状态
func (s *ShieldEvent) SetRecordStatus(recordStatus int) *ShieldEvent {
	s.RecordStatus = recordStatus
	return s
}

// GetGmtCreate 获取创建时间
func (s *ShieldEvent) GetGmtCreate() time.Time {
	return s.GmtCreate
}

// SetGmtCreate 设置创建时间
func (s *ShieldEvent) SetGmtCreate(gmtCreate time.Time) *ShieldEvent {
	s.GmtCreate = gmtCreate
	return s
}

// GetGmtUpdate 获取更新时间
func (s *ShieldEvent) GetGmtUpdate() time.Time {
	return s.GmtUpdate
}

// SetGmtUpdate 设置更新时间
func (s *ShieldEvent) SetGmtUpdate(gmtUpdate time.Time) *ShieldEvent {
	s.GmtUpdate = gmtUpdate
	return s
}

//// String 实现字符串表示
//func (s *ShieldEvent) String() string {
//	return fmt.Sprintf("ShieldEvent{Success=%v, ID=%d, EventType=%s, TxType=%s, EventStatus=%s, Content=%s, AppID=%s, RecordStatus=%d, BizKey=%s, BeforeUpdateEventStatus=%s, GmtCreate=%v, GmtUpdate=%v}",
//		s.Success, s.EventID, s.EventType, s.TxType, s.EventStatus, s.Content, s.AppID, s.RecordStatus, s.BizKey, s.BeforeUpdateEventStatus, s.GmtCreate, s.GmtUpdate)
//}

// Convert 消息协议转换为事件
func (s *ShieldEvent) Convert(shieldTxcMessage *ShieldTxcMessage) {
	s.SetEventID(shieldTxcMessage.EventID)
	s.SetTxType(shieldTxcMessage.TxType)
	s.SetContent(shieldTxcMessage.Content)
	s.SetAppID(shieldTxcMessage.AppID)
	s.SetEventStatus(shieldTxcMessage.EventStatus)
}
