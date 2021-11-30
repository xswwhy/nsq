package protocol

// 自定义了错误接口, 下面有两个错误类型 ClientErr FatalClientErr
// Parent()获取最初的错误 // FIXME: 感觉函数名叫Origin()比较合适
type ChildErr interface {
	Parent() error
}

// 对ParentErr的包装的,ParentErr是其他函数返回的最原始的错误
// Code Desc 多加了一些错误信息而已
type ClientErr struct {
	ParentErr error  // 获取到的err
	Code      string // 这里Code 都是 E_INVALID E_BAD_BODY 类似的字符串
	Desc      string
}

// 错误必须要实现的接口
func (e *ClientErr) Error() string {
	return e.Code + " " + e.Desc
}
func (e *ClientErr) Parent() error {
	return e.ParentErr
}

func NewClientErr(parent error, code string, desc string) *ClientErr {
	return &ClientErr{
		ParentErr: parent,
		Code:      code,
		Desc:      desc,
	}
}

type FatalClientErr struct {
	ParentErr error
	Code      string
	Desc      string
}

// 错误必须要实现的接口
func (e *FatalClientErr) Error() string {
	return e.Code + " " + e.Desc
}
func (e *FatalClientErr) Parent() error {
	return e.ParentErr
}

func NewFatalClientErr(parent error, code string, desc string) *FatalClientErr {
	return &FatalClientErr{
		ParentErr: parent,
		Code:      code,
		Desc:      desc,
	}
}
