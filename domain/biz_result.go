package domain

// BizResult 定义内部业务返回体
type BizResult struct {
	BizCode int
	BizData string
}

// NewBizResult 创建一个新的BizResult实例
func NewBizResult(bizCode int) *BizResult {
	return &BizResult{
		BizCode: bizCode,
	}
}

// NewBizResultWithData 创建一个新的BizResult实例并设置数据
func NewBizResultWithData(bizCode int, bizData string) *BizResult {
	return &BizResult{
		BizCode: bizCode,
		BizData: bizData,
	}
}

// GetBizCode 获取业务码
func (b *BizResult) GetBizCode() int {
	return b.BizCode
}

// SetBizCode 设置业务码
func (b *BizResult) SetBizCode(bizCode int) *BizResult {
	b.BizCode = bizCode
	return b
}

// GetBizData 获取业务数据
func (b *BizResult) GetBizData() string {
	return b.BizData
}

// SetBizData 设置业务数据
func (b *BizResult) SetBizData(bizData string) *BizResult {
	b.BizData = bizData
	return b
}
