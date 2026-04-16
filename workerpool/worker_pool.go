package workerpool

import (
	"context"
	"sync"
)

type Task func() error

type Pool struct {
	wg  sync.WaitGroup
	sem chan struct{}
	ctx context.Context
}

func New(ctx context.Context, size int) *Pool {
	return &Pool{
		sem: make(chan struct{}, size),
		ctx: ctx,
	}
}

func (p *Pool) Submit(task Task) <-chan error {
	select {
	case p.sem <- struct{}{}:
	case <-p.ctx.Done():
		return nil
	}

	chErr := make(chan error, 1)
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.sem
			p.wg.Done()
		}()
		chErr <- task()
	}()
	return chErr
}

func (p *Pool) Shutdown() {
	p.wg.Wait()
}
