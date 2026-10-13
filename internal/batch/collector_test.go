package batch

import (
	"testing"
	"time"
)

func TestCollector_FlushesOnSize(t *testing.T) {
	c := NewCollector[int](3, time.Hour)

	if c.Add(1) {
		t.Fatal("should not be ready after 1 item")
	}
	if c.Add(2) {
		t.Fatal("should not be ready after 2 items")
	}
	if !c.Add(3) {
		t.Fatal("should be ready after 3 items")
	}

	items := c.Take()
	if len(items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(items))
	}
	if c.Len() != 0 {
		t.Fatal("collector should be empty after Take")
	}
}

func TestCollector_FlushesOnTimeout(t *testing.T) {
	c := NewCollector[int](100, 10*time.Millisecond)

	c.Add(1)
	if c.Ready() {
		t.Fatal("should not be ready immediately")
	}

	time.Sleep(20 * time.Millisecond)

	if !c.Ready() {
		t.Fatal("should be ready after timeout")
	}
}

func TestCollector_EmptyNeverReady(t *testing.T) {
	c := NewCollector[int](1, time.Nanosecond)
	time.Sleep(time.Millisecond)
	if c.Ready() {
		t.Fatal("empty collector should never be ready")
	}
}

func TestCollector_TimeLeft(t *testing.T) {
	c := NewCollector[int](100, 50*time.Millisecond)

	if c.TimeLeft() != 50*time.Millisecond {
		t.Fatalf("expected full wait when empty, got %v", c.TimeLeft())
	}

	c.Add(1)
	time.Sleep(60 * time.Millisecond)

	if c.TimeLeft() != 0 {
		t.Fatalf("expected 0 once overdue, got %v", c.TimeLeft())
	}
}
