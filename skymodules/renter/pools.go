package renter

import (
	"bytes"
	"sync"
)

var (
	staticPoolExecuteProgramBuffers = newExecuteProgramBufferPool()
	staticPoolProvidePaymentBuffers = newProvidePaymentBufferPool()
	staticPoolUnresolvedWorkers     = newUnresolvedWorkersPool()
	staticPoolJobHasSectorResponse  = newJobHasSectorResponsePool()
)

type (
	executeProgramBufferPool struct {
		staticPool sync.Pool
	}
	providePaymentBufferPool struct {
		staticPool sync.Pool
	}
	unresolvedWorkersPool struct {
		staticPool sync.Pool
	}
	jobHasSectorResponsePool struct {
		staticPool sync.Pool
	}
)

func newUnresolvedWorkersPool() *unresolvedWorkersPool {
	return &unresolvedWorkersPool{
		staticPool: sync.Pool{
			New: func() interface{} {
				return &pcwsUnresolvedWorker{}
			},
		},
	}
}

func (p *unresolvedWorkersPool) Get() *pcwsUnresolvedWorker {
	return p.staticPool.Get().(*pcwsUnresolvedWorker)
}

func (p *unresolvedWorkersPool) Put(w *pcwsUnresolvedWorker) {
	p.staticPool.Put(w)
}

func newExecuteProgramBufferPool() *executeProgramBufferPool {
	return &executeProgramBufferPool{
		staticPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 1<<12))
			},
		},
	}
}

func (p *executeProgramBufferPool) Get() *bytes.Buffer {
	b := p.staticPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func (p *executeProgramBufferPool) Put(b *bytes.Buffer) {
	p.staticPool.Put(b)
}

func newJobHasSectorResponsePool() *jobHasSectorResponsePool {
	return &jobHasSectorResponsePool{
		staticPool: sync.Pool{
			New: func() interface{} {
				return &jobHasSectorResponse{}
			},
		},
	}
}

func (p *jobHasSectorResponsePool) Get() *jobHasSectorResponse {
	return p.staticPool.Get().(*jobHasSectorResponse)
}

func (p *jobHasSectorResponsePool) Put(iw *jobHasSectorResponse) {
	p.staticPool.Put(iw)
}

func newProvidePaymentBufferPool() *providePaymentBufferPool {
	return &providePaymentBufferPool{
		staticPool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 300))
			},
		},
	}
}

func (p *providePaymentBufferPool) Get() *bytes.Buffer {
	b := p.staticPool.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func (p *providePaymentBufferPool) Put(b *bytes.Buffer) {
	p.staticPool.Put(b)
}
