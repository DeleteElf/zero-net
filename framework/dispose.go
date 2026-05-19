package framework

import "sync"

// OnCloseHandler 关闭的api接口
type OnCloseHandler interface {
	// OnClosing 关闭前需要处理的事务，如果要拒绝继续关闭，返回false
	OnClosing() bool
	// OnClosed 关闭后的事务，仅当完毕完成才会执行此业务
	OnClosed()
}

// Closeable 可关闭对象
type Closeable interface {
	Close()
	SetOnCloseHandler(handler OnCloseHandler)
}

// CloseableObject 实现基础流程逻辑的关闭对象
type CloseableObject struct {
	IsClosed  bool
	closeLock sync.Mutex

	onCloseHandler OnCloseHandler
	Closeable
}

// Close 关闭流对象
func (o *CloseableObject) Close() {
	o.closeLock.Lock()
	defer o.closeLock.Unlock()
	if !o.IsClosed { //防止多次执行
		//开始释放资源
		if o.onCloseHandler != nil {
			if o.onCloseHandler.OnClosing() {
				o.IsClosed = true
				o.onCloseHandler.OnClosed()
			}
		}
	}
}
