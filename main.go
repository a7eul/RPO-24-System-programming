package main

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Result struct {
	Input  int
	Square int
}

func processOne(ctx context.Context, x int) (Result, error) {
	timer := time.NewTimer(200 * time.Millisecond)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return Result{}, ctx.Err()
	case <-timer.C:
	}
	return Result{Input: x, Square: x * x}, nil
}

func runSequential(ctx context.Context, inputs []int) ([]Result, error) {
	var out []Result
	for _, x := range inputs {
		res, err := processOne(ctx, x)
		if err != nil {
			return out, err
		}
		out = append(out, res)
	}
	return out, nil
}

func runPool(ctx context.Context, inputs []int, workers int) ([]Result, error) {
	if workers < 1 {
		return nil, fmt.Errorf("работников должно быть >= 1, получил %d", workers)
	}

	jobs := make(chan int, len(inputs))
	results := make(chan Result, len(inputs))

	go func() {
		defer close(jobs)
		for _, x := range inputs {
			select {
			case <-ctx.Done():
				return
			case jobs <- x:
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for x := range jobs {
				res, err := processOne(ctx, x)
				if err != nil {
					return
				}
				select {
				case <-ctx.Done():
					return
				case results <- res:
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	var out []Result
loop:
	for {
		select {
		case res, ok := <-results:
			if !ok {
				break loop
			}
			out = append(out, res)
		case <-ctx.Done():
			sort.Slice(out, func(i, j int) bool { return out[i].Input < out[j].Input })
			return out, ctx.Err()
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Input < out[j].Input })
	return out, nil
}

func show(name string, res []Result, dur time.Duration, err error) {
	fmt.Printf("\n--- %s (%v) ---\n", name, dur.Round(time.Millisecond))
	if err != nil {
		fmt.Println("ошибка:", err)
	}
	sum := 0
	for _, r := range res {
		fmt.Printf("  %d^2 = %d\n", r.Input, r.Square)
		sum += r.Square
	}
	fmt.Printf("итого: %d шт, сумма = %d\n", len(res), sum)
}

func main() {
	inputs := []int{1, 2, 3, 4, 5, 6, 7, 8}
	ctx := context.Background()

	t := time.Now()
	r, e := runSequential(ctx, inputs)
	show("последовательно", r, time.Since(t), e)

	t = time.Now()
	r, e = runPool(ctx, inputs, 1)
	show("пул 1", r, time.Since(t), e)

	t = time.Now()
	r, e = runPool(ctx, inputs, 4)
	show("пул 4", r, time.Since(t), e)

	ctx2, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	t = time.Now()
	r, e = runPool(ctx2, inputs, 4)
	show("пул 4, таймаут 300мс", r, time.Since(t), e)
}
