package constant

// EventType 定义事件类型
type EventType string

const (
	// INSERT 插入
	INSERT EventType = "INSERT"
	// DELETE 逻辑删除
	DELETE EventType = "DELETE"
	// UPDATE 更新
	UPDATE EventType = "UPDATE"
)
