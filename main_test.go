package main

import (
	"context"
	"testing"
	"time"
)

func TestFull(t *testing.T) {
	res, err := runPool(context.Background(), []int{1, 2, 3, 4, 5, 6, 7, 8}, 4)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]int{1: 1, 2: 4, 3: 9, 4: 16, 5: 25, 6: 36, 7: 49, 8: 64}
	for _, r := range res {
		if want[r.Input] != r.Square {
			t.Errorf("%d^2 = %d, ждали %d", r.Input, r.Square, want[r.Input])
		}
	}
}

func TestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	res, err := runPool(ctx, []int{1, 2, 3, 4, 5, 6, 7, 8}, 4)
	if err == nil || len(res) >= 8 {
		t.Errorf("ожидали частичный результат, получили %d и err=%v", len(res), err)
	}
}

func TestBadWorkers(t *testing.T) {
	if _, err := runPool(context.Background(), []int{1}, 0); err == nil {
		t.Error("ожидали ошибку для 0 работников")
	}
}
