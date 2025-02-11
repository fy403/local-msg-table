package domain

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
)

// ShieldTxcMessage 事务回滚消息
type ShieldTxcMessage struct {
	AbstractShieldTxcMessage
	EventID     string `json:"eventId"`
	TxType      string `json:"txType"`
	EventStatus string `json:"eventStatus"`
	Content     string `json:"content"`
	AppID       string `json:"appId"`
}

// NewShieldTxcMessage 创建一个新的 ShieldTxcMessage 实例
func NewShieldTxcMessage() *ShieldTxcMessage {
	return &ShieldTxcMessage{
		EventID: uuid.New().String(),
	}
}

// Encode 将消息编码为 JSON 字符串
func (m *ShieldTxcMessage) Encode() (string, error) {
	messageBody := map[string]interface{}{
		"eventId":     m.EventID,
		"txType":      m.TxType,
		"eventStatus": m.EventStatus,
		"content":     m.Content,
		"appId":       m.AppID,
	}

	jsonData, err := json.Marshal(messageBody)
	if err != nil {
		return "", fmt.Errorf("ShieldTxcMessage 消息序列化 JSON 异常: %w", err)
	}
	return string(jsonData), nil
}

// Decode 将 JSON 字符串解码为消息
func (m *ShieldTxcMessage) Decode(msg string) error {
	if msg == "" {
		return fmt.Errorf("消息不能为空")
	}

	var messageBody map[string]interface{}
	err := json.Unmarshal([]byte(msg), &messageBody)
	if err != nil {
		return fmt.Errorf("ShieldTxcMessage 反序列化消息异常: %w", err)
	}

	m.EventID = messageBody["eventId"].(string)
	m.TxType = messageBody["txType"].(string)
	m.EventStatus = messageBody["eventStatus"].(string)
	m.Content = messageBody["content"].(string)
	m.AppID = messageBody["appId"].(string)

	return nil
}
