package limiter

import "context"

type Limiter struct {
	sem chan struct{}
}

func New(size int) *Limiter {
	if size <= 0 {
		size = 1
	}
	return &Limiter{sem: make(chan struct{}, size)}
}

func (l *Limiter) Do(ctx context.Context, fn func(context.Context) error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case l.sem <- struct{}{}:
	}
	defer func() { <-l.sem }()

	return fn(ctx)
}

func (l *Limiter) Capacity() int {
	return cap(l.sem)
}
