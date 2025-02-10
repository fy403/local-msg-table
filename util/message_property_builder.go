package util

import (
	"fmt"
	"strings"
)

// GroupID 获取groupId
func GroupID(tranStage, topic string) string {
	return fmt.Sprintf("GID_%s%s", tranStage, strings.ToUpper(topic))
}

// Topic 获取topic
func Topic(tranStage, topic string) string {
	return fmt.Sprintf("%s%s", tranStage, strings.ToUpper(topic))
}
