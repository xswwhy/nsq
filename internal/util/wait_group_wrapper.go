package util

import "sync"

// 对sync.WaitGroup 的包装,不需要频繁使用 Add() Done()
// 有点python装饰器的感觉
type WaitGroupWrapper struct {
	sync.WaitGroup
}

func (w *WaitGroupWrapper) Wrap(cb func()) {
	w.Add(1)
	go func() {
		cb()
		w.Done()
	}()
}
