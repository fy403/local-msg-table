package domain

import (
	"encoding/json"
)

// AbstractShieldTxcMessage 定义事务消息抽象类
type AbstractShieldTxcMessage interface {
	Encode() (string, error)
	Decode(msg string) error
}

// AbstractShieldTxcMessageBase 提供基础实现
type AbstractShieldTxcMessageBase struct{}

// Encode 消息序列化
func (a *AbstractShieldTxcMessageBase) Encode(message interface{}) (string, error) {
	data, err := json.Marshal(message)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// Decode 消息反序列化
func (a *AbstractShieldTxcMessageBase) Decode(msg string, message interface{}) error {
	return json.Unmarshal([]byte(msg), message)
}
