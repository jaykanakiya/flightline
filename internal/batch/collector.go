// Package batch decides when a batch of items is ready to flush, by size
// or by time. It doesn't do any I/O itself - the caller owns fetching items
// and flushing them.
package batch

import "time"

// Collector buffers items until the batch reaches maxSize, or maxWait has
// elapsed since the first item was added - whichever happens first.
type Collector[T any] struct {
	maxSize int
	maxWait time.Duration

	items   []T
	started time.Time
}

func NewCollector[T any](maxSize int, maxWait time.Duration) *Collector[T] {
	return &Collector[T]{maxSize: maxSize, maxWait: maxWait}
}

// Add appends an item and reports whether the batch should be flushed now.
func (c *Collector[T]) Add(item T) bool {
	if len(c.items) == 0 {
		c.started = time.Now()
	}
	c.items = append(c.items, item)
	return c.Ready()
}

// Ready reports whether the batch should be flushed. It can go true without
// a new Add, once maxWait has passed - the caller is expected to poll it
// while waiting for the next item.
func (c *Collector[T]) Ready() bool {
	if len(c.items) == 0 {
		return false
	}
	return len(c.items) >= c.maxSize || time.Since(c.started) >= c.maxWait
}

// TimeLeft returns how long until the current batch times out, for use as
// a fetch deadline. It's zero once the batch is empty or already due.
func (c *Collector[T]) TimeLeft() time.Duration {
	if len(c.items) == 0 {
		return c.maxWait
	}
	left := c.maxWait - time.Since(c.started)
	if left < 0 {
		return 0
	}
	return left
}

// Take returns the buffered items and resets the collector.
func (c *Collector[T]) Take() []T {
	items := c.items
	c.items = nil
	return items
}

func (c *Collector[T]) Len() int {
	return len(c.items)
}
