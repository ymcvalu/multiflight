package multiflight

import (
	"context"
	"errors"
	"fmt"

	"sync"
)

var (
	// errResultNotFound result not found
	errResultNotFound = errors.New("result not found")
)

// Loader load values for multiple keys
type Loader[K comparable, V any] func(ctx context.Context, keys []K) (map[K]V, error)

// ent is an in-flight or completed request for one key
type ent[K comparable, V any] struct {
	wg  sync.WaitGroup
	key K

	// These fields are written once before the WaitGroup is done
	// and are only read after the WaitGroup is done.
	val V
	err error
}

// Group multi group
type Group[K comparable, V any] struct {
	mu sync.Mutex       // protects m
	m  map[K]*ent[K, V] // lazily initialized
}

// Do executes and returns the results of the given function, making
// sure that only one execution is in-flight for every given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
func (g *Group[K, V]) Do(ctx context.Context, keys []K, load Loader[K, V]) (map[K]V, error) {
	ents := make([]*ent[K, V], 0, len(keys))
	missEnts := make([]*ent[K, V], 0, len(keys))

	g.withLock(func() {
		if g.m == nil {
			g.m = make(map[K]*ent[K, V], 1024) // 预分配一下
		}

		for _, key := range keys {
			if e, has := g.m[key]; has {
				ents = append(ents, e)
				continue
			}
			e := new(ent[K, V])
			e.key = key
			e.wg.Add(1)
			g.m[key] = e // for share
			ents = append(ents, e)
			missEnts = append(missEnts, e)
		}
	})

	// load keys
	if len(missEnts) > 0 {
		g.doLoad(ctx, missEnts, load)
	}

	result := make(map[K]V, len(keys))
	for _, e := range ents {
		e.wg.Wait()
		if e.err != nil {
			// result not found, skip
			if errors.Is(e.err, errResultNotFound) {
				continue
			}

			return nil, e.err // return the first err
		}
		result[e.key] = e.val
	}

	return result, nil
}

// doLoad load for miss keys.
func (g *Group[K, V]) doLoad(ctx context.Context, ents []*ent[K, V], load Loader[K, V]) {
	keys := make([]K, 0, len(ents))
	for _, e := range ents {
		keys = append(keys, e.key)
	}

	vals, err := load(ctx, keys)
	if err != nil {
		g.withLock(func() {
			for _, e := range ents {
				g.setCallErr(e, err)
			}
		})
		return
	}

	g.withLock(func() {
		for _, e := range ents {
			if v, has := vals[e.key]; has {
				g.setCallResult(e, v)
			} else {
				g.setCallErr(e, errResultNotFound)
			}
		}
	})
}

func (g *Group[K, V]) withLock(f func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	f()
}

func (g *Group[K, V]) setCallResult(e *ent[K, V], v V) {
	e.val = v
	e.wg.Done()
	delete(g.m, e.key)
}

func (g *Group[K, V]) setCallErr(e *ent[K, V], err error) {
	e.err = err
	e.wg.Done()
	delete(g.m, e.key)
}

func (g *Group[int, string]) Test() {
	fmt.Println("...")
}
