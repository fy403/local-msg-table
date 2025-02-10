package exception

import (
	"fmt"
)

// BizException 定义业务异常
type BizException struct {
	Message string
	Err     error
}

// NewBizException 创建一个新的BizException实例
func NewBizException(msg string) *BizException {
	return &BizException{
		Message: msg,
	}
}

// NewBizExceptionWithErr 创建一个新的BizException实例并包含错误
func NewBizExceptionWithErr(err error) *BizException {
	return &BizException{
		Err: err,
	}
}

// NewBizExceptionWithMsgAndErr 创建一个新的BizException实例并包含消息和错误
func NewBizExceptionWithMsgAndErr(msg string, err error) *BizException {
	return &BizException{
		Message: msg,
		Err:     err,
	}
}

// Error 实现错误接口
func (e *BizException) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}
