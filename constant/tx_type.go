package constant

type TXType string

const (
	// COMMIT 提交
	COMMIT TXType = "COMMIT"
	// ROLLBACK 回滚
	ROLLBACK TXType = "ROLLBACK"
)

// GetCommit 返回提交的字符串
func GetCommit() string {
	return string(COMMIT)
}

// GetRollback 返回回滚的字符串
func GetRollback() string {
	return string(ROLLBACK)
}
